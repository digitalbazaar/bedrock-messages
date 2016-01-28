/*
 * Copyright (c) 2015 Digital Bazaar, Inc. All rights reserved.
 */
 /* globals describe, before, after, it, should, beforeEach, afterEach */
 /* jshint node: true */

'use strict';

var _ = require('lodash');
var async = require('async');
var bedrock = require('bedrock');
var brKey = require('bedrock-key');
var brMessages = require('../lib/messages');
var config = bedrock.config;
var util = bedrock.util;
var helpers = require('./helpers');
var brIdentity = require('bedrock-identity');
var database = require('bedrock-mongodb');
var request = require('request');
var mockData = require('./mock.data');
var uuid = require('node-uuid').v4;
request = request.defaults({json: true});

process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

var store = database.collections.messages;

var messagesSearchEndpoint =
  config.server.baseUri + config.messages.endpoints.messagesSearch;
var messagesBatchEndpoint =
  config.server.baseUri + config.messages.endpoints.messagesBatch;
var messagesEndpoint =
  config.server.baseUri + config.messages.endpoints.messages;

describe('bedrock-messages HTTP API', function() {
  describe('unauthenticated requests', function() {
    it('should respond with 400 - PermissionDenied', function(done) {
      var user = mockData.identities.rsa4096;
      request.post({
        url: messagesSearchEndpoint + '/' + user.identity.id + '/new'
      }, function(err, res, body) {
        should.not.exist(err);
        res.statusCode.should.equal(400);
        should.exist(body);
        body.should.be.an('object');
        body.type.should.be.a('string');
        body.type.should.equal('PermissionDenied');
        done();
      });
    });
  });

  describe('authenticated requests', function() {
    before('Prepare the database', function(done) {
      helpers.prepareDatabase(mockData, done);
    });
    after('Remove test data', function(done) {
      helpers.removeCollections(done);
    });

    it('return empty array if there are no new messages', function(done) {
      var user = mockData.identities.rsa4096;
      request.post(
        helpers.createHttpSigRequest(
          messagesSearchEndpoint + '/' + user.identity.id + '/new', user),
        function(err, res, body) {
        should.not.exist(err);
        res.statusCode.should.equal(200);
        should.exist(body);
        body.should.be.an('array');
        body.should.have.length(0);
        done();
      });
    });
    it('return one new message', function(done) {
      var user = mockData.identities.rsa4096;
      async.auto({
        insert: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: user.identity.id}), callback);
        },
        get: ['insert', function(callback) {
          request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id + '/new', user),
            function(err, res, body) {
            should.not.exist(err);
            res.statusCode.should.equal(200);
            should.exist(body);
            body.should.be.an('array');
            body.should.have.length(1);
            done();
          });
        }]
      }, done);
    });
    it('return seven new messages', function(done) {
      var user = mockData.identities.rsa4096;
      var numberOfMessages = 7;
      async.auto({
        insert: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: user.identity.id}), next);
          }, function(err) {
            callback();
          });
        },
        get: ['insert', function(callback) {
          request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id + '/new', user),
            function(err, res, body) {
            should.not.exist(err);
            res.statusCode.should.equal(200);
            should.exist(body);
            body.should.be.an('array');
            body.should.have.length(numberOfMessages);
            done();
          });
        }]
      }, done);
    });
    it('does not allow access to another user\'s messages',
      function(done) {
      var user = mockData.identities.rsa4096;
      var badUserId = 'did:' + uuid();
      request.post(
        helpers.createHttpSigRequest(
          messagesSearchEndpoint + '/' + badUserId + '/new', user),
        function(err, res, body) {
          should.not.exist(err);
          res.statusCode.should.equal(403);
          should.exist(body);
          body.should.be.an('object');
          should.exist(body.type);
          body.type.should.be.a('string');
          body.type.should.equal('PermissionDenied');
          should.exist(body.details.sysPermission);
          body.details.sysPermission.should.be.a('string');
          body.details.sysPermission.should.equal('MESSAGE_ACCESS');
          done();
        });
    });
    it('return zero messages if no NEW messages', function(done) {
      var user = mockData.identities.rsa4096;
      request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id + '/new', user),
            function(err, res, body) {
            should.not.exist(err);
            res.statusCode.should.equal(200);
            should.exist(body);
            body.should.have.length(0);
            body.should.be.an('array');
            done();
          });
    });
    it('return 9 total message both old and new', function(done) {
      var user = mockData.identities.rsa4096;
      async.auto({
        insert: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: user.identity.id}), callback);
        },
        get: ['insert', function(callback) {
          request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
            should.not.exist(err);
            res.statusCode.should.equal(200);
            should.exist(body);
            body.should.be.an('array');
            body.should.have.length(9);
            done();
          });
        }]
      }, done);
    });
    it('return 9 total messages with brpassport', function(done) {
      var user = mockData.identities.rsa4096;
      request.post(
        helpers.createHttpSigRequest(
          messagesSearchEndpoint, user),
        function(err, res, body) {
          should.not.exist(err);
          res.statusCode.should.equal(200);
          should.exist(body);
          body.should.be.an('array');
          body.should.have.length(9);
          done();
        });
    });
    it('not allow access to another users message with brpassport',
      function(done) {
      var user = mockData.identities.rsa4096;
      var invalidUser = mockData.identities.rsa4096v2;
      request.post(
        helpers.createHttpSigRequest(messagesSearchEndpoint, invalidUser),
        function(err, res, body) {
          should.not.exist(err);
          res.statusCode.should.equal(400);
          should.exist(body);
          should.exist(body.type);
          body.type.should.be.a('string');
          body.type.should.equal('PermissionDenied');
          done();
        });
    });
    it('should delete a batch of messages', function(done) {
      var user = mockData.identities.rsa4096;
      var messageIdOne;
      var messageIdTwo;
      var messageIdThree;
      var messageIdBatch;
      async.auto({
        getMessageIds: function(callback) {
          request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id,user),
            function(err, res, body) {
              messageIdOne = body[0].id;
              messageIdTwo = body[1].id;
              messageIdThree = body[2].id;
              messageIdBatch = [messageIdOne, messageIdTwo, messageIdThree];
              callback();
            });
        },
        del: ['getMessageIds', function(callback) {
          request.del(
            helpers.createHttpSigDelRequest
              (messagesBatchEndpoint, user,messageIdBatch),
            function(err, res, body) {
            should.not.exist(err);
            should.exist(body);
            body.n.should.equal(3);
            callback();
          });
        }],
        get: ['del', function(callback) {
          request.post(
            helpers.createHttpSigRequest(
              messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              should.not.exist(err);
              res.statusCode.should.equal(200);
              should.exist(body);
              body.should.be.an('array');
              body.should.have.length(6);
              done();
            });
        }]
      }, done);
    });
    it('should delete a single message', function(done) {
      var user = mockData.identities.rsa4096;
      var messageId;
      async.auto({
        getMessageId: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageId = body[0].id;
              callback();
            });
        },
        del: ['getMessageId', function(callback) {
          request.del(
            helpers.createHttpSigRequest(
              messagesEndpoint + '/' + messageId, user),
            function(err, res, body) {
              body.result.ok.should.equal(1);
              body.result.n.should.equal(1);
              done();
            });
        }]
      }, done);
    });
    it('not allow another user to delete message', function(done) {
      var user = mockData.identities.rsa4096;
      var invalidUser = mockData.identities.rsa4096v2;
      var messageId;
      async.auto({
        getMessageId: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageId = body[0].id;
              callback();
            });
        },
        del: ['getMessageId', function(callback) {
          request.del(
            helpers.createHttpSigRequest(
              messagesEndpoint + '/' + messageId, invalidUser),
            function(err, res, body) {
              should.not.exist(err);
              res.statusCode.should.equal(400);
              body.message.should.equal('Request authentication error.');
              body.type.should.equal('PermissionDenied');
              done();
            });
        }]
      }, done);
    });
    it('archive one message', function(done) {
      var user = mockData.identities.rsa4096;
      var messageId;
      async.auto({
        getMessageId: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageId = body[0].id;
              callback();
            });
        },
        update: ['getMessageId', function(callback) {
          request.post(
            helpers.createHttpSigUpdateRequest(
              messagesEndpoint + '/' + messageId, user, messageId,'archive'),
            function(err, res, body) {
              should.not.exist(err);
              should.exist(body);
              body.ok.should.equal(1);
              body.nModified.should.equal(1);
              done();
            });
        }]
      }, done);
    });
    it('invalid update operation', function(done) {
      var user = mockData.identities.rsa4096;
      var messageId;
      async.auto({
        getMessageIds: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageId = body[0].id;
              callback();
            });
        },
        update: ['getMessageIds', function(callback) {
          request.post(
            helpers.createHttpSigUpdateRequest(
              messagesEndpoint + '/' + messageId, user, messageId,'randomOp'),
            function(err, res, body) {
              should.not.exist(err);
              should.exist(body);
              body.message.should.equal('No suitable update operation');
              body.type.should.equal('MessageUpdate');
              done();
            });
        }]
      }, done);
    });
    it('archive batch of messages', function(done) {
      var user = mockData.identities.rsa4096;
      var messageIdOne;
      var messageIdTwo;
      var messageIdBatch;
      async.auto({
        getMessagesId: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageIdOne = body[0].id;
              messageIdTwo = body[1].id;
              messageIdBatch = [messageIdOne, messageIdTwo];
              callback();
            });
        },
        update: ['getMessagesId', function(callback) {
          request.post(
            helpers.createHttpSigUpdateBatchRequest(
              messagesBatchEndpoint, user, messageIdBatch,'archive'),
            function(err, res, body) {
              should.not.exist(err);
              should.exist(body);
              body.ok.should.equal(1);
              body.nModified.should.equal(2);
              done();
            });
        }]
      }, done);
    });
    it('mixed results on update batch of messages', function(done) {
      var user = mockData.identities.rsa4096;
      var messageIdOne;
      var messageIdTwo = 'invalidMessageId';
      var messageIdBatch;
      async.auto({
        getMessageId: function(callback) {
          request.post(helpers.createHttpSigRequest(
            messagesSearchEndpoint + '/' + user.identity.id, user),
            function(err, res, body) {
              messageIdOne = body[0].id;
              messageIdBatch = [messageIdOne, messageIdTwo];
              callback();
            });
        },
        update: ['getMessageId', function(callback) {
          request.post(
            helpers.createHttpSigUpdateBatchRequest(
              messagesBatchEndpoint, user, messageIdBatch, 'archive'),
            function(err, res, body) {
              should.not.exist(err);
              should.exist(body);
              body.message.should.equal('An internal server error occurred.');
              body.type.should.equal('bedrock.InternalServerError');
              done();
            });
        }]
      }, done);
    });
  });
});
