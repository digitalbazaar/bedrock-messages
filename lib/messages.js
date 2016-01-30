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

var _ = require('lodash');
var async = require('async');
var bedrock = require('bedrock');
var BedrockError = bedrock.util.BedrockError;
var brPassport = require('bedrock-passport');
var brPermission = require('bedrock-permission');
var config = bedrock.config;
var database = require('bedrock-mongodb');
var ensureAuthenticated = brPassport.ensureAuthenticated;
var rest = require('bedrock-rest');
var uuid = require('node-uuid').v4;
var validate = require('bedrock-validation').validate;
var store = null;
var storeInvalid = null;
var storeBatch = null;
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
      database.openCollections(
        ['messages', 'invalidMessages', 'messagesBatch'], function(err) {
        if(!err) {
          store = database.collections.messages;
          storeInvalid = database.collections.invalidMessages;
          storeBatch = database.collections.messagesBatch;
        }
        callback(err);
      });
    },
    createIndexes: ['openCollections', function(callback) {
      // FIXME: review indexes for use with new algorithm
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
/*
 * Stores a message with its recipient in the database.
 */
api.store = function(messages, callback) {
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
  var jobId = uuid();
  async.forEachOf(messageCollection, function(collection, key, callback) {
    if(collection.count === 0) {
      return callback();
    }
    async.auto({
      getBatch: function(callback) {
        if(key === 'invalid') {
          return callback();
        }
        // get unique recipients
        var recipients = collection.messages.map(function(message) {
          return message.recipient;
        });
        var uniqueRecipients = _.uniq(recipients);
        // get the recipients batch document or create one if it doesn't exist
        var batchIds = {};
        async.each(uniqueRecipients, function(recipient, callback) {
          readBatch(recipient, function(err, result) {
            // add batch ids to a map that will be used during message creation
            batchIds[recipient] = result.id;
            callback();
          });
        }, function(err, results) {
          callback(err, batchIds);
        });
      },
      process: ['getBatch', function(callback, results) {
        var values = [];
        async.each(collection.messages, function(message, callback) {
          var batch;
          if(key === 'valid') {
            batch = results.getBatch[message.recipient];
            message.meta = createMeta('created', currentTime, jobId, batch);
          }
          if(key === 'invalid') {
            var invalidBatchId = -999;
            message.meta =
              createMeta('created', currentTime, jobId, invalidBatchId);
          }
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
      }],
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
      }],
      batch: ['store', function(callback, results) {
        if(key === 'invalid') {
          return callback();
        }
        callback();
      }]
    }, callback);
  }, function(err) {
    if(err) {
      return callback(err);
    }
    var results = {
      job: jobId,
      valid: {
        count: messageCollection.valid.count
      },
      invalid: {
        count: messageCollection.invalid.count
      }
    };
    callback(null, results);
  });
};

// add routes
bedrock.events.on('bedrock-express.configure.routes', function(app) {
  // FIXME: what is the permissions model for this?
  app.get(
    config.messages.endpoints.messages + '/:id', rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      getId(
        req.user.identity, req.params.id, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Update endpoint, single
  app.post(
    config.messages.endpoints.messages + '/:id', rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      if(req.body.message !== req.params.id) {
        return next(new BedrockError(
          'Message ID mismatch.', 'MessageIdMismatch',
          {httpStatusCode: 409, 'public': true}));
      }
      updateMessage(
        req.user.identity, req.body, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Update endpoint, batch
  app.post(
    config.messages.endpoints.messagesBatch, rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      bulkUpdate(
        req.user.identity, req.body, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Delete endpoint, single
  app.delete(
    config.messages.endpoints.messages + '/:id', rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      deleteMessage(
        req.user.identity, req.params.id, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // Delete endpoint, batch
  app.delete(
    config.messages.endpoints.messagesBatch, rest.when.prefers.ld,
    ensureAuthenticated, function(req, res, next) {
      bulkDelete(
        req.user.identity, req.body, {recipient: req.user.identity.id},
        function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // retrieve messages for the identity authenticated by brPassport
  app.post(config.messages.endpoints.messagesSearch, ensureAuthenticated,
    function(req, res, next) {
      get(req.user.identity, req.user.identity.id, function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // return ALL messages for a recipient, this will be used by
  // bedrock-message-client
  // FIXME: this endpoint is intended for users/admins that are not the
  // recipient to query for messages
  // TODO: we might want to create a seperate endpoint that only returns
  // message header information, the main use case for function is to populate
  // a list of messages (subject/sender/date), so returning the whole message is
  // unneccessary
  app.post(config.messages.endpoints.messagesSearch + '/:recipient',
    ensureAuthenticated, function(req, res, next) {
      get(req.user.identity, req.params.recipient, function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });

  // return new messages, this endpoint will not return the same results twice
  app.post(
    config.messages.endpoints.messagesSearch + '/:recipient/new',
    ensureAuthenticated, function(req, res, next) {
      getNew(req.user.identity, req.params.recipient, function(err, results) {
        if(err) {
          return next(err);
        }
        res.json(results);
      });
    });
});

api.getMessages = function(actor, messages, options, callback) {
  getMessages(actor, messages, options, callback);
};

// Exposed for testing
api._getNew = function(actor, recipient, callback) {
  getNew(actor, recipient, callback);
};
api._get = function(actor, recipient, callback) {
  get(actor, recipient, callback);
};
api._getId = function(actor, id, options, callback) {
  getId(actor, id, options, callback);
};
api._updateMessage = function(actor, request, options, callback) {
  updateMessage(actor, request, options, callback);
};
api._bulkUpdate = function(actor, request, options, callback) {
  bulkUpdate(actor, request, options, callback);
};
api._deleteMessage = function(actor, id, options, callback) {
  deleteMessage(actor, id, options, callback);
};
api._bulkDelete = function(actor, request, options, callback) {
  bulkDelete(actor, request, options, callback);
};
api._batchMessage = function(batch, message, callback) {
  batchMessage(batch, message, callback);
};
api._getUnbatchedMessage = function(batch, callback) {
  getUnbatchedMessage(batch, callback);
};
api._closeBatch = function(recipient, callback) {
  closeBatch(recipient, callback);
};
api._readBatch = function(recipient, callback) {
  readBatch(recipient, callback);
};
api._cleanupJob = function(callback) {
  cleanupJob(callback);
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
function get(actor, recipient, callback) {
  async.auto({
    checkPermissions: function(callback) {
      brPermission.checkPermission(
        actor, PERMISSIONS.MESSAGE_ACCESS, {resource: recipient}, callback);
    },
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
      store.find(q, projection)
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

// accepts an array of messages to retrieve
function getMessages(actor, messages, options, callback) {
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
      store.findAndModify(q, [], u, database.writeOptions, callback);
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
      store.update(q, u, database.writeOptions, function(err, result) {
        if(err) {
          return callback(
            new BedrockError('Database update failed', 'MessageUpdate', {
              httpStatusCode: 500,
              public: true,
            }));
        }
        callback(null, result);
      });
    }]
  }, function(err, results) {
    callback(err, results.updateOperation);
  });
}

function bulkUpdate(actor, request, options, callback) {
  if(request.operation !== 'archive') {
    // No suitable operation found, return error
    return callback(new BedrockError(
      'No suitable update operation', 'MessageUpdateBatch', {
        httpStatusCode: 400,
        public: true}));
  }
  if(!request.messages) {
    // No messages supplied, return error
    return callback(new BedrockError(
      'No messages supplied', 'MessageUpdateBatch', {
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
      var ids = request.messages.map(function(message) {
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
      store.updateMany(q, u, database.writeOptions, function(err, result) {
        if(err) {
          return callback(new BedrockError(
            'Database batch update failed', 'BulkUpdateFailure', {
              httpStatusCode: 500,
              public: true,
              mongoError: err
            }));
        }
        callback(null, result);
      });
    }],
    checkResults: ['batchOperation', function(callback, results) {
      if(results.batchOperation.result.nModified === request.messages.length) {
        return callback(null, results.batchOperation);
      }
      // FIXME: should error reporting here be more sophisticated
      // error returns mongo details
      callback(new BedrockError(
        'Database batch update failed', 'BulkUpdateFailure', {
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
      store.deleteOne(q, database.writeOptions, function(err, results) {
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

function bulkDelete(actor, request, options, callback) {
  if(!request.messages) {
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
      var ids = request.messages.map(function(message) {
        return database.hash(message);
      });
      var q = {
        id: {$in: ids},
        'value.recipient': options.recipient
      };
      store.deleteMany(q, database.writeOptions, function(err, results) {
        if(err) {
          return callback(new BedrockError(
            'Database message batch delete failed', 'BulkDeleteFailure', {
              httpStatusCode: 500,
              public: true,
              mongoError: err
            }));
        }
        callback(null, results);
      });
    }],
    checkResults: ['deleteOperation', function(callback, results) {
      if(results.deleteOperation.result.n === request.messages.length) {
        return callback(null, results.deleteOperation);
      }
      // FIXME: should error reporting here be more sophisticated
      // error returns mongo details
      callback(new BedrockError(
        'Database batch update failed', 'BulkDeleteFailure', {
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
        }, database.writeOptions, callback);
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

function createMeta(type, currentTime, jobId, batch) {
  var meta = {};
  meta.batch = {
    id: batch,
    state: 'pending'
  };
  var event = {
    type: type,
    date: currentTime,
    job: jobId
  };
  meta.events = [event];
  return meta;
}

function readBatch(recipient, callback) {
  var q = {
    id: database.hash(recipient)
  };
  var u = {
    $setOnInsert: {
      id: database.hash(recipient),
      value: {
        id: 0,
        recipient: recipient,
        messages: {}
      }
    }
  };
  var o = _.extend({}, database.writeOptions, {new: true, upsert: true});
  storeBatch.findAndModify(q, [], u, o, function(err, result) {
    callback(err, result.value.value);
  });
}

function batchMessage(batch, message, done) {
  async.auto({
    updateBatch: [function(callback) {
      var q = {
        id: database.hash(message.recipient),
        'value.id': batch.id
      };
      var set = {
        'value.dirty': true
      };
      set['value.messages.' + message.id] = true;
      var u = {
        $set: set
      };
      storeBatch.updateOne(q, u, database.writeOptions, callback);
    }],
    updateMessage: ['updateBatch', function(callback, results) {
      if(results.updateBatch.matchedCount === 0) {
        console.log('YYYYYYYYYYYYYYYY');
        // batch was delivered
        return done(null, null);
      }
      console.log('TTTTTTTTTTTTTTTTT');
      // message was added to the join map, update message state
      var q = {
        'value.meta.batch.state': 'pending',
        'value.meta.batch.id': batch.id
      };
      var u = {
        $set: {
          'value.meta.batch.state': 'ready'
        }
      };
      store.updateOne(q, u, database.writeOptions, callback);
    }],
    removeFromMap: ['updateMessage', function(callback, results) {
      if(results.updateMessage.modifiedCount === 0) {
        console.log('ZZZZZZZZZZZZZZZZZZZz');
        // another process is handling the message
        return done(null, null);
      }
      console.log('SSSSSSSSSSSSSSSSSS');
      // message state successfully updated, remove from map
      var q = {
        id: database.hash(message.recipient),
        'value.id': message.meta.batch.id
      };
      var unset = {};
      unset['value.messages.' + message.id] = '';
      console.log('^^^^^^^^^^', unset);
      var u = {
        $unset: unset
      };
      storeBatch.updateOne(q, u, database.writeOptions, callback);
    }]
  }, function(err, results) {
    console.log('%%%%%%%%%%%%%', results.removeFromMap.result);
    done(err, batch);
  });
}

function resetBatch(batch, callback) {
  var q = {
    'id': database.hash(batch.recipient),
    'value.messages': {}
  };
  var u = {
    $set: {
      'value.dirty': false
    }
  };
  storeBatch.updateOne(q, u, database.writeOptions, callback);
}

function resetMessage(batch, message, done) {
  var q = {
    'value.id': message.id,
    'value.meta.batch.id': message.meta.batch.id
  };
  var u = {
    $set: {
      'value.meta.batch.id': batch.id,
      'value.meta.batch.state': 'pending'
    }
  };
  store.updateOne(q, u, database.writeOptions, function(err) {
    done(err, {batch: batch, message: message});
  });
}

function getUnbatchedMessage(batch, done) {
  var q;
  // already have a batch to work on
  if(batch) {
    var messages = Object.keys(batch.messages);
    console.log('*************', messages.length);
    if(messages.length > 0) {
      // query for message in join map
      var message = messages[0];
      q = {
        id: database.hash(message)
      };
    } else {
      // query for pending message w/batch ID < batch.id
      q = {
        'value.meta.batch.state': 'pending',
        'value.meta.batch.id': {$lt: batch.id}
      };
    }
    return store.findOne(q, function(err, result) {
      if(err) {
        return done(err);
      }
      if(result) {
        return resetMessage(batch, result.value, done);
      }
      resetBatch(batch, function(err) {
        if(err) {
          return done(err);
        }
        loop(null, done);
      });
    });
  }
  // find a batch to work on
  async.auto({
    findDirty: function(callback) {
      // lookup dirty batch
      q = {
        'value.dirty': true
      };
      storeBatch.findOne(q, function(err, result) {
        if(result) {
          return loop(result.value, done);
        }
        callback(err, result);
      });
    },
    findPending: ['findDirty', function(callback) {
      // no marked dirty batch found, look for pending message
      // in unmarked batch
      q = {
        'value.meta.batch.state': 'pending'
      };
      store.findOne(q, function(err, result) {
        if(!result) {
          // all batches clean
          return done(err, null);
        }
        callback(err, result.value);
      });
    }],
    readBatch: ['findPending', function(callback, results) {
      readBatch(results.findPending.recipient, callback);
    }],
    resetMessage: ['readBatch', function(callback, results) {
      resetMessage(results.readBatch, results.findPending, callback);
    }]
  }, function(err, results) {
    done(err, results.resetMessage);
  });

  function loop(batch, callback) {
    process.nextTick(function() {
      getUnbatchedMessage(batch, callback);
    });
  }
}

function closeBatch(recipient, done) {
  async.auto({
    readBatch: function(callback) {
      readBatch(recipient, callback);
    },
    findMessage: ['readBatch', function(callback, results) {
      var messageIds = Object.keys(results.readBatch.messages);
      var q = {
        'value.meta.batch.id': results.readBatch.id
      };
      if(messageIds.length > 0) {
        q['value.id'] = {$not: {$in: messageIds}};
      }
      store.findOne(q, callback);
    }],
    updateBatch: ['findMessage', function(callback, results) {
      if(!results.findMessage) {
        return done(null, null);
      }
      var q = {id: database.hash(recipient), 'value.id': results.readBatch.id};
      var u = {$inc: {'value.id': 1}};
      storeBatch.findAndModify(q, [], u, database.writeOptions, callback);
    }],
    getMessages: ['updateBatch', function(callback, results) {
      if(!results.updateBatch) {
        return done(null, null);
      }
      var batch = results.updateBatch.value.value;
      var q = {
        'value.meta.batch.id': batch.id,
        'value.meta.batch.state': 'ready'
      };
      var messageIds = Object.keys(batch.messages);
      if(messageIds.length > 0) {
        q['value.id'] = {$not: {$in: messageIds}};
      }
      var projection = {
        'value.meta': false
      };
      store.find(q, projection).toArray(callback);
    }]
  }, function(err, results) {
    var batch = results.updateBatch.value.value;
    done(err, {id: batch.id, messages: results.getMessages});
  });
}

function cleanupJob(callback) {
  var batch = null;
  var done = false;
  async.until(function() {
    return done;
  }, function(loopCallback) {
    async.auto({
      getUnbatched: function(callback) {
        getUnbatchedMessage(batch, callback);
      },
      batchMessage: ['getUnbatched', function(callback, results) {
        console.log('@@@@@@@', results.getUnbatched);
        if(!results.getUnbatched) {
          done = true;
          return loopCallback();
        }
        batchMessage(
          results.getUnbatched.batch, results.getUnbatched.message,
          function(err, result) {
          console.log('#######', err, result);
          batch = result;
          callback(err);
        });
      }]
    }, loopCallback);
  }, callback);
}
