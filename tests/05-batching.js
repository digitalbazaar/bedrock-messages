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

describe.only('bedrock-messages message batching functions', function() {
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
    it('batches a msg when msg state is pending and msg is not in map',
      function(done) {
      // state in the mock message is 'pending'
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
    it('batches a msg when state is pending and msg is in join map',
      function(done) {
      var message = util.clone(mockData.messages.alpha);
      var batch = util.clone(mockData.batches.alpha);
      batch.value.dirty = true;
      batch.value.messages[message.value.id] = true;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        act: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._batchMessage(batch.value, message.value, callback);
        }],
        messageQuery: ['act', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['act', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          var m = results.messageQuery.value;
          m.meta.batch.id.should.equal(0);
          m.meta.batch.state.should.equal('ready');
          var b = results.batchQuery.value;
          b.id.should.equal(0);
          b.recipient.should.equal(message.value.recipient);
          should.exist(b.messages);
          b.messages.should.be.an('object');
          _.isEmpty(b.messages).should.be.true;
          callback();
        }]
      }, done);
    });
    it('does nothing when msg state is ready and msg is in join map',
      function(done) {
      var message = util.clone(mockData.messages.alpha);
      var batch = util.clone(mockData.batches.alpha);
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
        act: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._batchMessage(batch.value, message.value, callback);
        }],
        messageQuery: ['act', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['act', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          var m = results.messageQuery.value;
          m.meta.batch.id.should.equal(0);
          m.meta.batch.state.should.equal('ready');
          var b = results.batchQuery.value;
          b.id.should.equal(0);
          b.recipient.should.equal(message.value.recipient);
          should.exist(b.messages);
          b.messages.should.be.an('object');
          should.exist(b.messages[message.value.id]);
          callback();
        }]
      }, done);
    });
    it('does nothing when batch is "delivered" first by another process', function(done) {
      var message = util.clone(mockData.messages.alpha);
      var batch = util.clone(mockData.batches.alpha);
      batch.value.dirty = true;
      batch.value.messages[message.value.id] = true;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        otherProcessUpdate: ['insertMessage', 'insertBatch', function(callback) {
          // Other process increments batch ID
          var q = {id: database.hash(batch.value.recipient)};
          var u = {$inc: {'value.id': 1}};
          storeBatch.update(q, u, callback);
        }],
        act: ['otherProcessUpdate', function(callback) {
          brMessages._batchMessage(batch.value, message.value, callback);
        }],
        messageQuery: ['act', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['act', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          var m = results.messageQuery.value;
          m.meta.batch.id.should.equal(message.value.meta.batch.id);
          m.meta.batch.state.should.equal(message.value.meta.batch.state);
          var b = results.batchQuery.value;
          b.id.should.equal(batch.value.id + 1);
          b.recipient.should.equal(message.value.recipient);
          should.exist(b.messages);
          b.messages.should.be.an('object');
          should.exist(b.messages[message.value.id]);
          callback();
        }]
      }, done);
    });
    it('message goes to "ready" state and message stays in join list if batch increments mid-function', 
      function(done) {
      var message = util.clone(mockData.messages.alpha);
      var batch = util.clone(mockData.batches.alpha);
      batch.value.dirty = true;
      batch.value.messages[message.value.id] = true;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        updateBatch: ['insertMessage', 'insertBatch', function(callback) {
          brMessages
            ._batchMessagePhase1(batch.value, message.value, null, callback);
        }],
        updateMessage: ['updateBatch', function(callback, results) {
          brMessages
            ._batchMessagePhase2(batch.value, message.value, null, callback, {'updateBatch': results.updateBatch});
        }],
        otherProcessUpdate: ['updateMessage', function(callback) {
          var q = {id: database.hash(batch.value.recipient)};
          var u = {$inc: {'value.id': 1}};
          storeBatch.update(q, u, callback);
        }],
        removeFromMap: ['otherProcessUpdate', function(callback, results) {
          brMessages
          ._batchMessagePhase3(batch.value, message.value, null, callback, {'updateMessage': results.updateMessage});
        }],
        messageQuery: ['removeFromMap', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['removeFromMap', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          var m = results.messageQuery.value;
          m.meta.batch.id.should.equal(message.value.meta.batch.id);
          m.meta.batch.state.should.equal('ready');
          var b = results.batchQuery.value;
          b.id.should.equal(batch.value.id + 1);
          b.recipient.should.equal(message.value.recipient);
          b.dirty.should.equal(true);
          should.exist(b.messages);
          b.messages.should.be.an('object');
          should.exist(b.messages[message.value.id]);
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
    it('creates a batch if there is pending messages but no batch for recipient', function(done) {
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'pending';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        getUnbatched: ['insertMessage', function(callback) {
          brMessages._getUnbatchedMessage(null, callback);
        }],
        batchQuery: ['getUnbatched', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['batchQuery', function(callback, results) {
          should.exist(results.getUnbatched);
          should.exist(results.batchQuery);
          //var b = results.batchQuery.value;
          //b.id.should.equal(0);
          //b.recipient.should.equal(message.value.recipient);
          //should.exist(b.messages);
          //b.messages.should.be.an('object');
          //should.exist(b.messages[message.value.id]);

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
    it('return message if message is ready and not in the join map',
      function(done) {
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
          should.exist(results.closeBatch.id);
          results.closeBatch.id.should.equal(0);
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
      async.auto({
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
    it('test increment message id mid function', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        readBatch: ['insertMessage', 'insertBatch', function(callback) {
          brMessages
            ._closeBatchPhase1(batch.value.recipient, null, callback);
        }],
        findMessage: ['readBatch', function(callback, results) {
          brMessages
            ._closeBatchPhase2(batch.value.recipient, null, callback, results);
        }],
        updateBatch: ['findMessage', function(callback, results) {
          brMessages
            ._closeBatchPhase3(batch.value.recipient, null, callback, results);
        }],
        otherProcessUpdate: ['updateBatch', function(callback) {
          var u = {$inc: {'value.meta.batch.id': 1}};
          store.update({}, u, callback);
        }],
        getMessages: ['otherProcessUpdate', function(callback, results) {
          brMessages
          ._closeBatchPhase4(batch.value.recipient, null, callback, results);
        }],
        messageQuery: ['getMessages', function(callback) {
          store.findOne({}, callback);
        }],
        batchQuery: ['getMessages', function(callback) {
          storeBatch.findOne({}, callback);
        }],
        test: ['messageQuery', 'batchQuery', function(callback, results) {
          // TODO test messages return -- messages should not be included
          // but that's ok because they'll be picked up on the next closeBatch
          var m = results.messageQuery.value;
          m.meta.batch.id.should.equal(message.value.meta.batch.id + 1);
          m.meta.batch.state.should.equal('ready');
          var b = results.batchQuery.value;
          b.id.should.equal(batch.value.id + 1);
          b.recipient.should.equal(message.value.recipient);
          should.exist(b.messages);
          b.messages.should.be.an('object');
          callback();
        }]
      }, done);
    });
  }); // end deliverBatch
  describe('cleanupJob function', function() {
    afterEach(function(done) {
      helpers.removeCollections(
        {collections: ['messagesBatch', 'messages']}, done);
    });
    it('batches where msg state is ready and msg is in join map',
      function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      batch.value.messages[message.value.id] = true;
      batch.value.dirty = true;
      message.value.meta.batch.state = 'ready';
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        cleanup: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._cleanupJob(callback);
        }],
        readBatch: ['cleanup', function(callback) {
          brMessages._readBatch(batch.value.recipient, callback);
        }],
        readMessage: ['cleanup', function(callback) {
          store.find({}).toArray(callback);
        }],
        test: ['readBatch', 'readMessage', function(callback, results) {
          should.exist(results.readBatch);
          var b = results.readBatch;
          b.id.should.equal(0);
          _.isEmpty(b.messages).should.be.true;
          b.dirty.should.be.false;
          // message should be returned to 'ready' state
          var m = results.readMessage[0].value;
          m.should.deep.equal(message.value);
          callback();
        }]
      }, done);
    });
  }); // end cleanupJob
  describe('resetMessage function', function() {
    afterEach(function(done) {
      helpers.removeCollections(
        {collections: ['messagesBatch', 'messages']}, done);
    });
    it('resets a message back to pending with new batch id', function(done) {
      var batch = util.clone(mockData.batches.alpha);
      var message = util.clone(mockData.messages.alpha);
      message.value.meta.batch.state = 'ready';
      batch.value.id = 1;
      async.auto({
        insertMessage: function(callback) {
          store.insert(message, callback);
        },
        insertBatch: function(callback) {
          storeBatch.insert(batch, callback);
        },
        act: ['insertMessage', 'insertBatch', function(callback) {
          brMessages._resetMessage(batch.value, message.value, callback);
        }],
        testResults: ['act', function(callback, results) {
          should.exist(results.act);
          results.act.should.be.an('object');
          should.exist(results.act.message);
          results.act.message.should.deep.equal(message.value);
          should.exist(results.act.batch);
          results.act.batch.should.deep.equal(batch.value);
          callback();
        }],
        findMessage: ['act', function(callback) {
          store.find({id: message.id}).toArray(callback);
        }],
        testMessage: ['findMessage', function(callback, results) {
          results.findMessage[0].value.meta.batch.state.should.equal('pending');
          results.findMessage[0].value.meta.batch.id.should.equal(1);
          callback();
        }]
      }, done);
    });
  }); // end resetMessage
});
