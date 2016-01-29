/*
 * Copyright (c) 2015 Digital Bazaar, Inc. All rights reserved.
 */
 /* globals describe, before, after, it, should, beforeEach, afterEach */
 /* jshint node: true */

'use strict';

var _ = require('lodash');
var async = require('async');
var bedrock = require('bedrock');
var brIdentity = require('bedrock-identity');
var brMessages = require('../lib/messages');
var config = bedrock.config;
var database = require('bedrock-mongodb');
var helpers = require('./helpers');
var mockData = require('./mock.data');
var util = bedrock.util;
var uuid = require('node-uuid').v4;

var store = database.collections.messages;
var storeBatch = database.collections.messagesBatch;

describe('bedrock-messages message batching functions', function() {
  before(function(done) {
    helpers.prepareDatabase(mockData, done);
  });
  after(function(done) {
    helpers.removeCollections(done);
  });
  describe('batchMessage function', function() {
    afterEach(function(done) {
      helpers.removeCollections(
        {collections: ['messagesBatch', 'messages']}, done);
    });
    it('calls batchMessage', function(done) {
      var testMessage = util.clone(mockData.messages.alpha);
      var testBatch = util.clone(mockData.batches.alpha);
      async.auto({
        insertMessage: function(callback) {
          store.insert(testMessage, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(testBatch, callback);
        },
        act: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._batchMessage(
            testBatch.value, testMessage.value, callback);
        }],
        messageQuery: ['act', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['act', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          var message = results.messageQuery.value;
          message.meta.batch.id.should.equal(0);
          message.meta.batch.state.should.equal('ready');
          var batch = results.batchQuery.value;
          batch.id.should.equal(0);
          batch.recipient.should.equal(message.recipient);
          should.exist(batch.messages);
          batch.messages.should.be.an('object');
          _.isEmpty(batch.messages).should.be.true;
          callback();
        }]
      }, done);
    });
  }); // end batchMessage
  describe('getUnbatchedMessage function', function() {
    afterEach(function(done) {
      helpers.removeCollections(
        {collections: ['messagesBatch', 'messages']}, done);
    });
    it('returns a dirty batch', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      batch.value.dirty = true;
      batch.value.messages[message.value.id] = true;
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        getUnbatched: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._getUnbatchedMessage(null, callback);
        }],
        test: ['getUnbatched', function(callback, results) {
          should.exist(results.getUnbatched);
          results.getUnbatched.should.be.an('object');
          should.exist(results.getUnbatched.batch);
          results.getUnbatched.batch.should.be.an('object');
          results.getUnbatched.batch.should.deep.equal(batch.value);
          should.exist(results.getUnbatched.message);
          results.getUnbatched.message.should.be.an('object');
          results.getUnbatched.message.should.deep.equal(message.value);
          callback();
        }]
      }, done);
    });
    it('returns a pending message', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'pending';
      batch.value.id = 1;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        getUnbatched: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._getUnbatchedMessage(null, callback);
        }],
        test: ['getUnbatched', function(callback, results) {
          should.exist(results.getUnbatched);
          results.getUnbatched.should.be.an('object');
          should.exist(results.getUnbatched.batch);
          results.getUnbatched.batch.should.be.an('object');
          results.getUnbatched.batch.should.deep.equal(batch.value);
          should.exist(results.getUnbatched.message);
          results.getUnbatched.message.should.be.an('object');
          results.getUnbatched.message.should.deep.equal(message.value);
          callback();
        }]
      }, done);
    });
    it('returns a pending messagage again', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'pending';
      batch.value.id = 0;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        getUnbatched: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._getUnbatchedMessage(null, callback);
        }],
        test: ['getUnbatched', function(callback, results) {
          should.exist(results.getUnbatched);
          results.getUnbatched.should.be.an('object');
          should.exist(results.getUnbatched.batch);
          results.getUnbatched.batch.should.be.an('object');
          results.getUnbatched.batch.should.deep.equal(batch.value);
          should.exist(results.getUnbatched.message);
          results.getUnbatched.message.should.be.an('object');
          results.getUnbatched.message.should.deep.equal(message.value);
          callback();
        }]
      }, done);
    });
    it('returns null if no dirty batch or pending messages', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        getUnbatched: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._getUnbatchedMessage(null, callback);
        }],
        test: ['getUnbatched', function(callback, results) {
          should.not.exist(results.getUnbatched);
          callback();
        }]
      }, done);
    });
  }); // end getUnbatchedMessage
  describe('deliverBatch function', function() {
    afterEach(function(done) {
      helpers.removeCollections(
        {collections: ['messagesBatch', 'messages']}, done);
    });
    it('return empty array if state is ready but message is in join map',
      function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      batch.value.messages[message.value.id] = true;
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        closeBatch: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._closeBatch(batch.value.recipient, callback);
        }],
        readBatch: ['closeBatch', function(callback) {
          brMessages._readBatch(batch.value.recipient, callback);
        }],
        test: ['readBatch', function(callback, results) {
          should.exist(results.readBatch);
          results.readBatch.id.should.equal(0);
          should.not.exist(results.closeBatch);
          callback();
        }]
      }, done);
    });
    it('return message and increment batch if message is ready and not in the join map',
      function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      // batch.value.messages[message.value.id] = true;
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        closeBatch: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._closeBatch(batch.value.recipient, callback);
        }],
        readBatch: ['closeBatch', function(callback) {
          brMessages._readBatch(batch.value.recipient, callback);
        }],
        test: ['readBatch', function(callback, results) {
          should.exist(results.readBatch);
          results.readBatch.id.should.equal(1);
          should.exist(results.closeBatch);
          results.closeBatch.should.be.an('object');
          should.exist(results.closeBatch.batch);
          results.closeBatch.batch.should.equal(0);
          should.exist(results.closeBatch.messages);
          results.closeBatch.messages.should.be.an('array');
          results.closeBatch.messages.should.have.length(1);
          delete message.value.meta;
          results.closeBatch.messages[0].value.should.deep.equal(message.value);
          callback();
        }]
      }, done);
    });
    it('does not increments the batch if no message is returned',
      function(done) {
      var batch = util.clone(mockData.batches.alpha);
      // var message = util.clone(mockData.messages.alpha);
      // batch.value.messages[message.value.id] = true;
      // message.value.meta.batch.state = 'ready';
      async.auto({
        // insertMessage: function(callback) {
        //   store.insert(message, callback);
        // },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        closeBatch: ['insertBatch', function(callback) {
          brMessages._closeBatch(batch.value.recipient, callback);
        }],
        readBatch: ['closeBatch', function(callback) {
          brMessages._readBatch(batch.value.recipient, callback);
        }],
        test: ['readBatch', function(callback, results) {
          should.exist(results.readBatch);
          results.readBatch.id.should.equal(0);
          should.not.exist(results.closeBatch);
          callback();
        }]
      }, done);
    });
  }); // end deliverBatch
});
