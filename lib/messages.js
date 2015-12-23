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

var bedrock = require('bedrock');
var database = require('bedrock-mongodb');

require('./config');

var logger = bedrock.loggers.get('app');

var api = {};
module.exports = api;

// create the collection to store messages
bedrock.events.on('bedrock-mongodb.ready', function(callback) {
  logger.debug('Creating messages collection.');
  async.auto({
    openCollections: function(callback) {
      database.openCollection('messages', function(err) {
        if(err) {
          callback(err);
        }
      });
      callback();
    },
    createIndexes: ['openCollections', function(callback) {
      database.createIndexes([{
        collection: 'messages',
        fields: {recipient: 1},
        options: {unique: false, background: false}
      }])
    }]
  }, function(err) {
    callback(err);
  });
});

/*
 * Stores a message with its recipient in the database.
 */
api.storeMessage = function(recipient, message) {
  record = {
    recipient: database.hash(recipient),
    message: message
  };

  database.collections.messages.insert(record, database.writeOptions, 
    function(err) {
      if(err) {

      }
    });
};

/*
 * Retrieve all messages associated with recipient.
 */
api.getMessages = function(recipient) {
  var messages = database.collections.message.find({recipient: recipient}, 
    function(err, result) {
      if(err) {

      }
    });

  var messagesToReturn = [];

  messages.forEach(function(item) {
    messagesToReturn.push(item);
  });

  return messagesToReturn;
};

/*
 * Send messages to recipient
 */ 
api.sendMessages = function(recipient, messages) {

};