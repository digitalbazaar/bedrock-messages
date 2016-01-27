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
      var recipient = uuid();
      async.auto({
        store: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: recipient}), callback);
        },
        act: ['store', function(callback) {
          brMessages._batchMessage(callback);
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
          batch.recipient.should.equal(recipient);
          should.exist(batch.messages);
          batch.messages.should.be.an('object');
          _.isEmpty(batch.messages).should.be.true;
          callback();
        }]
      }, done);
    });
  }); // end batchMessage
});
