/*
 * Bedrock messages module.
 *
 * This module exposes an API for sending, recieving
 * and querying a database of messages.
 *
 * Copyright (c) 2015 Digital Bazaar, Inc. All rights reserved.
 */
 /* jshint node: true */

'use strict';

var async = require('async');
var bedrock = require('bedrock');
var BedrockError = bedrock.util.BedrockError;
var brPassport = require('bedrock-passport');
var config = bedrock.config;
var database = require('bedrock-mongodb');
var ensureAuthenticated = brPassport.ensureAuthenticated;
var rest = require('bedrock-rest');
var uuid = require('node-uuid').v4;
var validate = require('bedrock-validation').validate;
var store = null;
var storeInvalid = null;
require('bedrock-express');

require('./config');

// configure for tests
bedrock.events.on('bedrock.test.configure', function() {
  require('./test.config');
});

var logger = bedrock.loggers.get('app');

var api = {};
module.exports = api;

// create the collection to store messages
bedrock.events.on('bedrock-mongodb.ready', function(callback) {
  logger.debug('Creating messages collection.');
  async.auto({
    openCollections: function(callback) {
      database.openCollections(['messages', 'invalidMessages'], function(err) {
        if(!err) {
          store = database.collections.messages;
          storeInvalid = database.collections.invalidMessages;
        }
        callback(err);
      });
    },
    createIndexes: ['openCollections', function(callback) {
      database.createIndexes([{
        collection: 'messages',
        fields: {id: 1},
        options: {unique: true, background: false}
      }, {
        collection: 'messages',
        fields: {recipient: 1},
        options: {unique: false, background: false}
      }], callback);
    }],
    createMessages: ['createIndexes', function(callback) {
      if(config.messages.newMessages.length === 0) {
        return callback();
      }
      api.store(config.messages.newMessages, callback);
    }]
  }, function(err) {
    callback(err);
  });
});

/*
 * Stores a message with its recipient in the database.
 */
api.store = function(messages, callback) {
  var batch = uuid();
  var currentTime = Date.now();
  // FIXME: validateMessages throws if messages is []
  var validation = validateMessages(messages);
  var messageCollection = {
    valid: {
      messages: validation.validMessages,
      store: store,
      count: validation.validMessages.length
    },
    invalid: {
      messages: validation.invalidMessages,
      store: storeInvalid,
      count: validation.invalidMessages.length
    }
  };
  async.forEachOf(messageCollection, function(collection, key, callback) {
    if(collection.count === 0) {
      return callback();
    }
    var idGenerator = null;
    async.auto({
      createIdGenerator: function(callback) {
        database.getDistributedIdGenerator(
          'bedrock-messages', function(err, idg) {
          if(!err) {
            idGenerator = idg;
          }
          callback(err);
        });
      },
      process: ['createIdGenerator', function(callback, results) {
        var values = [];
        async.each(collection.messages, function(message, callback) {
          async.auto({
            generateId: function(callback) {
              idGenerator.generateId(callback);
            },
            process: ['generateId', function(callback, results) {
              message.meta = createMeta('created', currentTime, batch);
              message.id = results.generateId;
              values.push({
                id: database.hash(message.id),
                value: message
              });
              callback();
            }]
          }, callback);
        }, function(err, results) {
          if(err) {
            return callback(err);
          }
          callback(null, values);
        });
      }],
      store: ['process', function(callback, results) {
        collection.store.insert(
          results.process, database.writeOptions, callback);
      }]
    }, callback);
  }, function(err) {
    if(err) {
      callback(err);
    }
    var results = {
      batch: batch,
      valid: messageCollection.valid.count,
      invalid: messageCollection.invalid.count
    };
    callback(null, results);
  });
};

/*
 * Send messages to recipient
 */
api.sendMessages = function(recipient, messages) {

};

// add routes
bedrock.events.on('bedrock-express.configure.routes', function(app) {
  // FIXME: what is the permissions model for this?
  app.get(
    config.messages.endpoints.messages + '/:id', rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      getId(req.params.id, function(err, results) {
        // FIXME: address error conditions
        res.json(results.value);
      });
    });

  // retrieve messages for the identity authenticated by brPassport
  app.post(config.messages.endpoints.messagesSearch, ensureAuthenticated,
    function(req, res, next) {
      get(req.user.identity.id, function(err, results) {
        if(err) {
          // FIXME: fix-up error message
          return next(new BedrockError(
            'Message query failed.', 'MessageQuery', {
              httpStatusCode: 400,
              public: true
            }));
        }
        res.json(results);
      });
    });

  // return ALL messages for a recipient, this will be used by
  // bedrock-message-client
  // FIXME: this endpoint is intended for users/admins that are not the
  // recipient to query for messages
  app.post(config.messages.endpoints.messagesSearch + '/:recipient',
    ensureAuthenticated, function(req, res, next) {
      // FIXME: replace req.user.identity with req.params.recipient,
      // add permissions check
      get(req.user.identity.id, function(err, results) {
        if(err) {
          // FIXME: fix-up error message
          return next(new BedrockError(
            'Message query failed.', 'MessageQuery', {
              httpStatusCode: 400,
              public: true
            }));
        }
        res.json(results);
      });
    });

  // return new messages, this endpoint will not return the same results twice
  app.post(
    config.messages.endpoints.messagesSearch + '/:recipient/new',
    ensureAuthenticated, function(req, res, next) {
      if(req.params.recipient !== req.user.identity.id) {
        // FIXME: fix-up error message
        return next(new BedrockError(
          'Authentication mismatch. Messages query identity does not match ' +
          'the authenticated user.', 'AuthenticationMismatch', {
          httpStatusCode: 409,
          public: true
        }));
      }
      getNew(req.user.identity.id, function(err, results) {
        if(err) {
          // FIXME: fix-up error message
          return next(new BedrockError(
            'New message query failed.', 'NewMessageQuery', {
              httpStatusCode: 400,
              public: true
            }));
        }
        res.json(results);
      });
    });
});

// Exposed for testing
api._getNew = function(recipient, callback) {
  getNew(recipient, callback);
};
api._get = function(recipient, callback) {
  get(recipient, callback);
};
api._getId = function(id, callback) {
  getId(id, callback);
};

// sort messages into two arrays, valid and invalid
function validateMessages(m) {
  var messages = [];
  if(m.constructor === Array) {
    messages = m;
  } else {
    messages.push(m);
  }
  var results = {
    validMessages: [],
    invalidMessages: []
  };
  messages.forEach(function(message) {
    var validation = validate('message.bedrock-messages', message);
    if(validation.valid) {
      results.validMessages.push(message);
    } else {
      // var validationError = validation.error;
      // validationError.errors = validation.errors;
      var invalidMessage = {
        message: message,
        validationErrors: validation.errors
      };
      results.invalidMessages.push(invalidMessage);
    }
  });
  return results;
}

// Retrive ALL messages associated with the recipient
function get(recipient, callback) {
  var query = {
    'value.recipient': recipient
  };
  var projection = {
    _id: false,
    id: true,
    'value.id': true,
    'value.@context': true,
    'value.meta': true,
    'value.recipient': true,
    'value.sender': true,
    'value.type': true,
    'value.content': true
  };
  store.find(query, projection)
    .toArray(function(err, results) {
      if(err) {
        callback(err);
      }
      var messages = results.map(function(record) {
        return record.value;
      });
      callback(null, messages);
    });
}

// Retrive a single message by mongo _id and mark it as read?
// TODO: should a message ALWAYS be marked as read or should it be optional?
function getId(id, callback) {
  var query = {
    id: database.hash(id)
  };
  // FIXME: should a message being read be an 'event' as well?
  var update = {
    $set: {
      'value.meta.read': true
    }
  };
  store.findAndModify(query, [], update, callback);
}

/*
 * Retrieve all NEW messages associated with recipient.
 * - mark messages with a matching recipient
 * - retrieve messages that were marked
 * - jobId can be used to locate messages collected during a collection event
 */
function getNew(recipient, callback) {
  var jobId = uuid();
  async.auto({
    mark: function(callback) {
      store.update(
        {'value.recipient': recipient, 'value.meta.jobId': null},
        {
          $set: {'value.meta.jobId': jobId},
          $push: {'value.meta.events': {
            type: 'delivered',
            date: Date.now()
          }}
        }, {multi: true}, callback);
    },
    get: ['mark', function(callback, results) {
      if(results.mark.result.nModified === 0) {
        // no matching records, return an empty array
        return callback(null, []);
      }
      var projection = {
        _id: false,
        id: false,
        'value.id': false
      };
      store.find({'value.meta.jobId': jobId}, projection)
        .toArray(callback);
    }]
  }, function(err, results) {
    if(err) {
      callback(err);
    }
    var messages = results.get.map(function(record) {
      return record.value;
    });
    callback(null, messages);
  });
}

function createMeta(type, currentTime, batch) {
  var meta = {};
  var event = {
    type: type,
    date: currentTime,
    batch: batch
  };
  meta.events = [event];
  return meta;
}
