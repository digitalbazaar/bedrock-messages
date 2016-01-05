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
var uuid = require('node-uuid').v4;
var validate = require('bedrock-validation').validate;
var store = null;
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
      database.openCollections(['messages'], function(err) {
        if(!err) {
          store = database.collections.messages;
        }
        callback(err);
      });
    },
    createIndexes: ['openCollections', function(callback) {
      database.createIndexes([{
        collection: 'messages',
        fields: {recipient: 1},
        options: {unique: false, background: false}
      }], callback);
    }]
  }, function(err) {
    callback(err);
  });
});

/*
 * Stores a message with its recipient in the database.
 */
api.store = function(message, callback) {
  var validation = validate('message.bedrock-messages', message);
  if(!validation.valid) {
    var validationError = validation.error;
    validationError.errors = validation.errors;
    return callback(validationError);
  }
  message.meta = {};
  message.meta.events = [];
  var event = {
    type: 'created',
    date: Date.now()
  };
  message.meta.events.push(event);

  store.insert(message, database.writeOptions, callback);
};

/*
 * Send messages to recipient
 */
api.sendMessages = function(recipient, messages) {

};

// add routes
bedrock.events.on('bedrock-express.configure.routes', function(app) {
  // FIXME: this end
  app.get(
    config.messages.endpoints.messages, ensureAuthenticated,
    function(req, res, next) {

    });

  // return ALL messages for a recipient, this will be used by
  // bedrock-message-client
  app.post(config.messages.endpoints.messagesSearch + '/:recipient',
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

// Retrive ALL messages associated with the recipient
function get(recipient, callback) {
  var query = {
    recipient: recipient
  };
  var projection = {
    _id: false,
    recipient: true,
    content: true,
  };
  store.find(query, projection)
    .toArray(callback);
}

/*
 * Retrieve all NEW messages associated with recipient.
 * - mark messages with a matching recipient
 * - retrieve messages that were marked
 */
function getNew(recipient, callback) {
  var jobId = uuid();
  async.auto({
    mark: function(callback) {
      store.update(
        {recipient: recipient, 'meta.jobId': null},
        {
          $set: {'meta.jobId': jobId},
          $push: {'meta.events': {
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
        recipient: true,
        content: true,
      };
      store.find({'meta.jobId': jobId}, projection)
        .toArray(callback);
    }]
  }, function(err, results) {
    callback(err, results.get);
  });
}
