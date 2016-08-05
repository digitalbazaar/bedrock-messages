/*
 * Bedrock messages module.
 *
 * This module exposes an API for sending, recieving
 * and querying a database of messages.
 *
 * Copyright (c) 2015-2016 Digital Bazaar, Inc. All rights reserved.
 */
/* jshint node: true */

'use strict';

var async = require('async');
var bedrock = require('bedrock');
var BedrockError = bedrock.util.BedrockError;
var brPassport = require('bedrock-passport');
var brPermission = require('bedrock-permission');
var config = bedrock.config;
var database = require('bedrock-mongodb');
var ensureAuthenticated = brPassport.ensureAuthenticated;
var rest = require('bedrock-rest');
var uuid = require('uuid').v4;
var validate = require('bedrock-validation').validate;
var store = null;
var storeInvalid = null;
require('bedrock-express');

require('./config');

// module permissions
var PERMISSIONS = config.permission.permissions;

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
      }, {
        collection: 'messages',
        fields: {'value.meta.jobId': 1},
        options: {sparse: true, background: false}
      }
    ], callback);
    }]
  }, function(err) {
    callback(err);
  });
});

bedrock.events.on('bedrock.start', function(callback) {
  if(config.messages.newMessages.length === 0) {
    return callback();
  }
  api.store(config.messages.newMessages, callback);
});

// accepts an array of messages to retrieve
api.getMessages = function(actor, messages, options, callback) {
  if(!messages) {
    // No messages supplied, return error
    return callback(new BedrockError(
      'No messages supplied', 'GetMessagesFailure', {
        httpStatusCode: 400,
        public: true}));
  }
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: options.recipient},
        callback);
    },
    batchOperation: ['checkPermissions', function(callback) {
      var ids = messages.map(function(message) {
        return database.hash(message);
      });
      var q = {
        id: {$in: ids},
        'value.recipient': options.recipient
      };
      store.find(q).toArray(function(err, result) {
        if(err) {
          return callback(new BedrockError(
            'Database batch update failed', 'GetMessagesFailure', {
              httpStatusCode: 500,
              public: true,
              mongoError: err
            }));
        }
        callback(null, result);
      });
    }],
    checkResults: ['batchOperation', function(callback, results) {
      if(results.batchOperation.length === messages.length) {
        var m = results.batchOperation.map(function(record) {
          return record.value;
        });
        return callback(null, m);
      }
      // FIXME: should error reporting here be more sophisticated
      // error returns mongo details
      var partialResult = [];
      if(results.batchOperation.length > 0) {
        partialResult = results.batchOperation.map(function(record) {
          return record.value;
        });
      }
      callback(new BedrockError(
        'Database batch lookup failed', 'GetMessagesFailure', {
          httpStatusCode: 404,
          public: true,
          mongoResult: partialResult
        }));
    }]
  }, function(err, results) {
    callback(err, results.checkResults);
  });
};

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
    async.auto({
      process: function(callback, results) {
        var values = [];
        async.each(collection.messages, function(message, callback) {
          message.meta = createMeta('created', currentTime, batch);
          // FIXME: do we need a better id generator?
          message.id = currentTime + '-' + uuid();
          if(!('recipient' in message)) {
            // required for recipient index
            message.recipient = 'MISSING_VALUE';
          }
          values.push({
            id: database.hash(message.id),
            recipient: database.hash(message.recipient),
            value: message
          });
          callback();
        }, function(err, results) {
          callback(err, values);
        });
      },
      store: ['process', function(callback, results) {
        collection.store.insert(
          results.process, database.writeOptions, callback);
      }],
      emit: ['store', function(callback, results) {
        if(key === 'invalid') {
          return callback();
        }
        results.process.forEach(function(message) {
          bedrock.events.emit(
            'bedrock-messages.NewMessage', {
              recipient: message.value.recipient,
              id: message.value.id
            });
        });
        callback();
      }]
    }, callback);
  }, function(err) {
    if(err) {
      return callback(err);
    }
    var results = {
      batch: batch,
      valid: messageCollection.valid.count,
      invalid: messageCollection.invalid.count
    };
    callback(null, results);
  });
};

// add routes
bedrock.events.on('bedrock-express.configure.routes', function(app) {
  // FIXME: what is the permissions model for this?

  // Update endpoint
  app.patch(
    config.messages.endpoints.messages, rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      // Seperate all "delete" operations from other operations
      var operations = req.body;
      var deleteOperations = operations.filter(function(operation) {
        return operation.op === 'delete';
      });
      var updateOperations = operations.filter(function(operation) {
        return operation.op !== 'delete';
      });
      async.auto({
        delete: function(callback) {
          if(deleteOperations.length !== 0) {
            return callback();
          }
          var messages = deleteOperations.map(function(operation) {
            return operation.id;
          });
          batchDelete(
            req.user.identity,
            messages,
            {recipient: req.user.identity.id},
            callback);
        },
        update: function(callback) {
          if(updateOperations.length === 0) {
            return callback();
          }
          batchUpdate(
            req.user.identity,
            updateOperations,
            {recipient: req.user.identity.id},
            callback);
        }
      }, function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Delete endpoint
  app.delete(
    config.messages.endpoints.messages, rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      if(!req.query.id) {
        return next(new BedrockError(
          'InvalidRequest.', 'InvalidRequest',
          {httpStatusCode: 404, 'public': true}));
      }
      deleteMessage(
        req.user.identity, req.query.id, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Get endpoint
  app.get(config.messages.endpoints.messages, ensureAuthenticated,
    function(req, res, next) {
      // Retrieve single message
      if(req.query.id) {
        getId(
          req.user.identity, req.query.id, {recipient: req.user.identity.id},
          function(err, results) {
          if(err) {
            return next(err);
          }
          res.json(results);
        });
        return;
      }
      // Retrieve new messages for recipient
      if(req.query.state === 'new' && req.query.recipient &&
        !req.query.limit && !req.query.offset) {
        getNew(req.user.identity, req.query.recipient, function(err, results) {
          if(err) {
            return next(err);
          }
          res.json(results);
        });
        return;
      }
      // Retrieve messages for recipient
      if(req.query.recipient) {
        get(req.user.identity, req.query, req.query.recipient,
          function(err, results) {
          if(err) {
            return next(err);
          }
          res.json(results);
        });
        return;
      }
      // Retrieve messages for currently authenticated user
      if(!req.query.recipient && !req.query.id) {
        get(
          req.user.identity, req.query, req.user.identity.id,
          function(err, results) {
          if(err) {
            return next(err);
          }
          res.json(results);
        });
        return;
      }
      // Invalid query paramaters sent
      return next(new BedrockError(
          'InvalidRequest.', 'InvalidRequest',
          {httpStatusCode: 404, 'public': true}));
    });
});

// Exposed for testing
api._getNew = getNew;
api._get = get;
api._getId = getId;
api._updateMessage = updateMessage;
api._batchUpdate = batchUpdate;
api._deleteMessage = deleteMessage;
api._batchDelete = batchDelete;

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
function get(actor, query, recipient, callback) {
  var limit = query.limit || 0;
  var offset = query.offset || 0;
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: recipient}, callback);
    },
    totalCount: ['checkPermissions', function(callback, results) {
      var q = {
        recipient: database.hash(recipient)
      };
      store.count(q, callback);
    }],
    queryCount: ['checkPermissions', function(callback, results) {
      var q = {
        recipient: database.hash(recipient)
      };
      var options = {
        limit: limit,
        skip: offset
      };
      store.count(q, options, callback);
    }],
    find: ['checkPermissions', function(callback, results) {
      var q = {
        recipient: database.hash(recipient)
      };
      var projection = {
        _id: false,
        id: true,
        'value.id': true,
        'value.@context': true,
        'value.date': true,
        'value.meta': true,
        'value.recipient': true,
        'value.sender': true,
        'value.subject': true,
        'value.type': true,
        'value.content': true
      };
      var options = {
        limit: limit,
        skip: offset
      };
      store.find(q, projection, options)
        .toArray(function(err, result) {
          if(err) {
            return callback(err);
          }
          var messages = result.map(function(record) {
            return record.value;
          });
          callback(null, messages);
        });
    }]
  }, function(err, results) {
    callback(err, results.find);
  });
}

// Retrive a single message by id and mark it as read?
// TODO: should a message ALWAYS be marked as read or should it be optional?
function getId(actor, id, options, callback) {
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: options.recipient},
        callback);
    },
    find: ['checkPermissions', function(callback) {
      var q = {
        id: database.hash(id),
        'value.recipient': options.recipient
      };
      // FIXME: should a message being read be an 'event' as well?
      var u = {
        $set: {
          'value.meta.read': true
        }
      };
      store.findAndModify(q, [], u, callback);
    }]
  }, function(err, results) {
    if(err) {
      return callback(err);
    }
    if(!results.find.value) {
      // no matches
      return callback(null, null);
    }
    callback(null, results.find.value.value);
  });
}

function updateMessage(actor, request, options, callback) {
  if(request.operation !== 'archive') {
    return callback(
      new BedrockError('No suitable update operation', 'MessageUpdate', {
        httpStatusCode: 400,
        public: true
      }));
  }
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: options.recipient},
        callback);
    },
    updateOperation: ['checkPermissions', function(callback) {
      var q = {
        id: database.hash(request.message),
        'value.recipient': options.recipient
      };
      var u = {
        $set: {'value.meta.archived': true},
        $push: {'value.meta.events': {
          type: 'archived',
          date: Date.now()
        }}
      };
      store.update(q, u, function(err, result) {
        if(err) {
          return callback(
            new BedrockError('Database update failed', 'MessageUpdate', {
              httpStatusCode: 500,
              public: true
            }));
        }
        callback(null, result);
      });
    }]
  }, function(err, results) {
    callback(err, results.updateOperation);
  });
}

function batchUpdate(actor, request, options, callback) {
  var messages = [];
  for(var operation in request) {
    // TODO: Support multiple types of operations
    // (right now only 'archive' is supported).
    if(request[operation].op !== 'archive' || !request[operation].id) {
      return callback(new BedrockError(
      'Invalid patch operation', 'MessageUpdate', {
        httpStatusCode: 400,
        public: true}));
    }
    messages.push(request[operation].id);
  }
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: options.recipient},
        callback);
    },
    batchOperation: ['checkPermissions', function(callback) {
      var ids = messages.map(function(message) {
        return database.hash(message);
      });
      var q = {
        id: {$in: ids},
        'value.recipient': options.recipient
      };
      var u = {
        $set: {'value.meta.archived': true},
        $push: {'value.meta.events': {
          type: 'archived',
          date: Date.now()
        }}
      };
      store.updateMany(q, u, function(err, result) {
        if(err) {
          return callback(new BedrockError(
            'Database batch update failed', 'BatchUpdateFailure', {
              httpStatusCode: 500,
              public: true,
              mongoError: err
            }));
        }
        callback(null, result);
      });
    }],
    checkResults: ['batchOperation', function(callback, results) {
      if(results.batchOperation.result.nModified === messages.length) {
        return callback(null, results.batchOperation);
      }
      // FIXME: should error reporting here be more sophisticated
      // error returns mongo details
      callback(new BedrockError(
        'Database batch update failed', 'BatchUpdateFailure', {
          httpStatusCode: 500,
          public: true,
          mongoResult: results.batchOperation
        }));
    }]
  }, function(err, results) {
    callback(err, results.checkResults);
  });
}

function deleteMessage(actor, id, options, callback) {
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: options.recipient},
        callback);
    },
    deleteOperation: ['checkPermissions', function(callback) {
      var q = {
        id: database.hash(id),
        'value.recipient': options.recipient
      };
      store.deleteOne(q, function(err, results) {
        var deleteResults = {};
        deleteResults.result = results.result;
        var error = null;
        if(err) {
          deleteResults.error = err;
          error = new BedrockError(
            'Database message delete failed', 'MessageDelete', {
              httpStatusCode: 500,
              public: true,
              body: deleteResults
            });
        }
        callback(error, deleteResults);
      });
    }]
  }, function(err, results) {
    callback(err, results.deleteOperation);
  });
}

function batchDelete(actor, messages, options, callback) {
  if(!messages) {
    return callback(
      new BedrockError('No messages supplied', 'MessageDeleteBatch', {
      httpStatusCode: 400,
      public: true
    }));
  }
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_REMOVE, {resource: options.recipient},
        callback);
    },
    deleteOperation: ['checkPermissions', function(callback) {
      var ids = messages.map(function(message) {
        return database.hash(message);
      });
      var q = {
        id: {$in: ids},
        'value.recipient': options.recipient
      };
      store.deleteMany(q, function(err, results) {
        if(err) {
          return callback(new BedrockError(
            'Database message batch delete failed', 'BatchDeleteFailure', {
              httpStatusCode: 500,
              public: true,
              mongoError: err
            }));
        }
        callback(null, results);
      });
    }],
    checkResults: ['deleteOperation', function(callback, results) {
      if(results.deleteOperation.result.n === messages.length) {
        return callback(null, results.deleteOperation);
      }
      // FIXME: should error reporting here be more sophisticated
      // error returns mongo details
      callback(new BedrockError(
        'Database batch update failed', 'BatchDeleteFailure', {
          httpStatusCode: 500,
          public: true,
          mongoResult: results.deleteOperation
        }));
    }]
  }, function(err, results) {
    callback(err, results.checkResults);
  });
}

/*
 * Retrieve all NEW messages associated with recipient.
 * - mark messages with a matching recipient
 * - retrieve messages that were marked
 * - jobId can be used to locate messages collected during a collection event
 */
function getNew(actor, recipient, callback) {
  var jobId = uuid();
  var recipientHash = database.hash(recipient);
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: recipient}, callback);
    },
    mark: ['checkPermissions', function(callback) {
      store.update(
        {recipient: recipientHash, 'value.meta.jobId': {$exists: false}},
        {
          $set: {'value.meta.jobId': jobId},
          $push: {'value.meta.events': {
            type: 'delivered',
            date: Date.now()
          }}
        }, {multi: true}, callback);
    }],
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
      store.find({
        'value.meta.jobId': jobId
      }, projection)
        .toArray(callback);
    }]
  }, function(err, results) {
    if(err) {
      return callback(err);
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
