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
var storeInvalid = database.collections.invalidMessages;

var referenceMessage = {
  _id: '',
  id: '',
  '@context': '',
  date: '',
  meta: '',
  recipient: '',
  sender: '',
  subject: '',
  type: '',
  content: {
    body: '',
    holder: '',
    link: '',
  }
};

describe('bedrock-messages API requests', function() {
  before(function(done) {
    helpers.prepareDatabase(mockData, done);
  });
  after(function(done) {
    helpers.removeCollections(done);
  });
  describe('store function', function() {
    it('store a message into mongodb', function(done) {
      var recipient = uuid();
      var message = helpers.createMessage({recipient: recipient});
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        query: ['store', function(callback) {
          store.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          var r = results.query;
          r.should.be.an('array');
          r.should.have.length(1);
          r[0].should.be.an('object');
          should.exist(r[0].id);
          r[0].id.should.be.a('string');
          should.exist(r[0].value);
          r[0].value.should.be.an('object');
          var message = r[0].value;
          should.exist(message['@context']);
          message['@context'].should.be.a('string');
          should.exist(message.date);
          message.date.should.be.a('string');
          should.exist(message.recipient);
          message.recipient.should.be.a('string');
          should.exist(message.sender);
          message.sender.should.be.a('string');
          should.exist(message.subject);
          message.subject.should.be.a('string');
          should.exist(message.type);
          message.type.should.be.a('string');
          // check content
          should.exist(message.content);
          message.content.should.be.an('object');
          should.exist(message.content.body);
          message.content.body.should.be.a('string');
          should.exist(message.content.holder);
          message.content.holder.should.be.a('string');
          should.exist(message.content.link);
          message.content.link.should.be.a('string');
          // check meta
          should.exist(message.meta);
          message.meta.should.be.an('object');
          should.exist(message.meta.events);
          message.meta.events.should.be.an('array');
          message.meta.events.should.have.length(1);
          message.meta.events[0].should.be.an('object');
          // check meta events
          var event = message.meta.events[0];
          should.exist(event.type);
          event.type.should.be.a('string');
          event.type.should.equal('created');
          should.exist(event.date);
          event.date.should.be.a('number');
          should.exist(event.batch);
          event.batch.should.be.a('string');
          callback();
        }]
      }, done);
    });
    it('store seven messages using single operations', function(done) {
      var recipient = uuid();
      var numberOfMessages = 7;
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        store: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: recipient}), next);
          }, function(err) {
            callback();
          });
        },
        query: ['store', function(callback) {
          store.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          var r = results.query;
          r.should.be.an('array');
          r.should.have.length(numberOfMessages);
          callback();
        }]
      }, done);
    });
    it('store seven valid messages as an array', function(done) {
      var recipient = uuid();
      var numberOfMessages = 7;
      var testMessages = [];
      for(var i = 0; i < numberOfMessages; i++) {
        testMessages.push(helpers.createMessage({recipient: recipient}));
      }
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        store: function(callback) {
          brMessages.store(testMessages, callback);
        },
        query: ['store', function(callback, results) {
          // check store results
          results.store.valid.should.equal(7);
          store.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          var r = results.query;
          r.should.be.an('array');
          r.should.have.length(numberOfMessages);
          callback();
        }]
      }, done);
    });
    it('one invalid message is stored in invalidMessage table', function(done) {
      var holder = uuid();
      var message = helpers.createMessage({holder: holder});
      // delete the recipient property
      delete message.recipient;
      var query = {
        'value.message.content.holder': holder
      };
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        query: ['store', function(callback) {
          storeInvalid.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          results.query.should.be.an('array');
          results.query.should.have.length(1);
          callback();
        }]
      }, done);
    });
    it('seven invalid message stored individually in invalidMessage collection',
      function(done) {
      var holder = uuid();
      var numberOfMessages = 7;
      var message = helpers.createMessage({holder: holder});
      var query = {
        'value.message.content.holder': holder
      };
      async.auto({
        store: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            var message = helpers.createMessage({holder: holder});
            delete message.recipient;
            brMessages.store(message, next);
          }, function(err) {
            callback();
          });
        },
        query: ['store', function(callback) {
          storeInvalid.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          results.query.should.be.an('array');
          results.query.should.have.length(7);
          callback();
        }]
      }, done);
    });
    it('store seven invalid messages as an array', function(done) {
      var holder = uuid();
      var numberOfMessages = 7;
      var testMessages = [];
      for(var i = 0; i < numberOfMessages; i++) {
        var message = helpers.createMessage({holder: holder});
        delete message.recipient;
        testMessages.push(message);
      }
      var query = {
        'value.message.content.holder': holder
      };
      async.auto({
        store: function(callback) {
          brMessages.store(testMessages, callback);
        },
        queryValid: ['store', function(callback, results) {
          // check store results
          results.store.valid.should.equal(0);
          store.find(query, {}).toArray(callback);
        }],
        queryInvalid: ['store', function(callback, results) {
          results.store.invalid.should.equal(7);
          var invalidQuery = {
            'value.meta.events.batch': results.store.batch
          };
          storeInvalid.find(invalidQuery).toArray(callback);
        }],
        test: ['queryValid', 'queryInvalid', function(callback, results) {
          should.exist(results.queryValid);
          var validResults = results.queryValid;
          validResults.should.be.an('array');
          validResults.should.have.length(0);
          var invalidResults = results.queryInvalid;
          invalidResults.should.be.an('array');
          invalidResults.should.have.length(7);
          callback();
        }]
      }, done);
    });
    it('store a mix of valid and invalid messages', function(done) {
      var numberOfValidMessages = 7;
      var numberOfInvalidMessages = 3;
      var testMessages = [];
      var i = 0;
      var message = null;
      for(i = 0; i < numberOfInvalidMessages; i++) {
        message = helpers.createMessage();
        delete message.recipient;
        testMessages.push(message);
      }
      for(i = 0; i < numberOfValidMessages; i++) {
        message = helpers.createMessage();
        testMessages.push(message);
      }
      async.auto({
        store: function(callback) {
          brMessages.store(testMessages, callback);
        },
        queryValid: ['store', function(callback, results) {
          // check store results
          var validQuery = {
            'value.meta.events.batch': results.store.batch
          };
          results.store.valid.should.equal(numberOfValidMessages);
          store.find(validQuery, {}).toArray(callback);
        }],
        queryInvalid: ['store', function(callback, results) {
          results.store.invalid.should.equal(numberOfInvalidMessages);
          var invalidQuery = {
            'value.meta.events.batch': results.store.batch
          };
          storeInvalid.find(invalidQuery).toArray(callback);
        }],
        test: ['queryValid', 'queryInvalid', function(callback, results) {
          should.exist(results.queryValid);
          var validResults = results.queryValid;
          validResults.should.be.an('array');
          validResults.should.have.length(numberOfValidMessages);
          var invalidResults = results.queryInvalid;
          invalidResults.should.be.an('array');
          invalidResults.should.have.length(numberOfInvalidMessages);
          callback();
        }]
      }, done);
    });
  });

  describe('get function', function() {
    afterEach(function(done) {
      helpers.removeCollection('messages', done);
    });
    it('retrieve one NEW messages by recipient', function(done) {
      var body = uuid();
      var holder = uuid();
      var link = uuid();
      var recipient = mockData.identities.rsa4096.identity.id;
      var sender = uuid();
      var subject = uuid();
      var type = uuid();
      var message = helpers.createMessage({
        body: body,
        holder: holder,
        link: link,
        recipient: recipient,
        sender: sender,
        subject: subject,
        type: type
      });
      async.auto({
        getIdentity: function(callback) {
          brIdentity.get(
            null, mockData.identities.rsa4096.identity.id, callback);
        },
        store: function(callback) {
          brMessages.store(message, callback);
        },
        query: ['store', 'getIdentity', function(callback, results) {
          brMessages._get(results.getIdentity[0], recipient, callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          results.query.should.be.an('array');
          results.query.should.have.length(1);
          results.query[0].should.be.an('object');
          var message = results.query[0];
          should.exist(message.id);
          message.id.should.be.a('string');
          should.exist(message['@context']);
          message['@context'].should.be.a('string');
          should.exist(message.date);
          message.date.should.be.a('string');
          should.exist(message.recipient);
          message.recipient.should.be.a('string');
          message.recipient.should.equal(recipient);
          should.exist(message.sender);
          message.sender.should.be.a('string');
          message.sender.should.equal(sender);
          should.exist(message.subject);
          message.subject.should.be.a('string');
          message.subject.should.equal(subject);
          should.exist(message.type);
          message.type.should.be.a('string');
          message.type.should.equal(type);
          should.exist(message.content);
          // check content
          message.content.should.be.an('object');
          var content = message.content;
          should.exist(content.body);
          content.body.should.be.a('string');
          content.body.should.equal(body);
          should.exist(content.holder);
          content.holder.should.be.a('string');
          content.holder.should.equal(holder);
          should.exist(message.meta);
          message.meta.should.be.an('object');
          callback();
        }]
      }, done);
    });
    it('message should not contain any unwanted properties', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      async.auto({
        store: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: recipient}), callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, mockData.identities.rsa4096.identity.id, callback);
        },
        test: ['store', 'getIdentity', function(callback, results) {
          brMessages._get(
            results.getIdentity[0], recipient, function(err, results) {
            should.not.exist(err);
            var message = results[0];
            message.should.be.an('object');
            // check message
            _.difference(Object.keys(message), Object.keys(referenceMessage))
              .should.have.length(0);
            // check content
            _.difference(
              Object.keys(message.content),
              Object.keys(referenceMessage.content))
              .should.have.length(0);
            callback(err);
          });
        }]
      }, done);
    });
    it('get seven new messages', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      var numberOfMessages = 7;
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        insert: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: recipient}), next);
          }, callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, mockData.identities.rsa4096.identity.id, callback);
        },
        get: ['insert', 'getIdentity', function(callback, results) {
          brMessages._get(results.getIdentity[0], recipient, callback);
        }],
        test: ['get', function(callback, results) {
          should.exist(results.get);
          var r = results.get;
          r.should.be.an('array');
          r.should.have.length(numberOfMessages);
          callback();
        }]
      }, done);
    });
    it('should not allow access to another users messages', function(done) {
      var recipient = uuid();
      var numberOfMessages = 7;
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        insert: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: recipient}), next);
          }, callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, mockData.identities.rsa4096.identity.id, callback);
        },
        get: ['insert', 'getIdentity', function(callback, results) {
          brMessages._get(
            results.getIdentity[0], recipient, function(err, result) {
            should.exist(err);
            err.should.be.an('object');
            err.name.should.be.a('string');
            err.name.should.equal('PermissionDenied');
            should.exist(err.details);
            err.details.should.be.an('object');
            err.details.sysPermission.should.be.a('string');
            err.details.sysPermission.should.equal('MESSAGE_ACCESS');
            callback();
          });
        }]
      }, done);
    });
  });

  describe('update function', function() {
    it('archive one message', function(done) {
      var body = uuid();
      var holder = uuid();
      var link = uuid();
      var recipient = uuid();
      var sender = uuid();
      var subject = uuid();
      var type = uuid();
      var message = helpers.createMessage({
        body: body,
        holder: holder,
        link: link,
        recipient: recipient,
        sender: sender,
        subject: subject,
        type: type
      });
      var request = {
        operation: 'archive',
        message: message
      };
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        query: ['store', function(callback) {
          brMessages._update(request, callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          should.exist(results.query.result);
          should.exist(results.query.message);
          should.exist(results.query.operation);
          should.not.exist(results.query.error);

          results.query.result.should.be.a('object');
          results.query.message.should.be.a('object');
          results.query.operation.should.be.a('string');

          callback();
        }]
      }, done);
    });
    it('update with an unrecognized operation', function(done) {
      var body = uuid();
      var holder = uuid();
      var link = uuid();
      var recipient = uuid();
      var sender = uuid();
      var subject = uuid();
      var type = uuid();
      var message = helpers.createMessage({
        body: body,
        holder: holder,
        link: link,
        recipient: recipient,
        sender: sender,
        subject: subject,
        type: type
      });
      var request = {
        operation: 'foo',
        message: message
      };
      async.series([
        function(callback) {
          brMessages.store(message, callback);
        },
        function(callback) {
          brMessages._update(request, function(err, results) {
            should.exist(err);
            should.exist(results.error);
            should.exist(results.result);
            should.exist(results.message);
            should.exist(results.operation);

            err.details.httpStatusCode.should.equal(400);

            results.error.should.be.a('object');
            results.result.should.be.a('string');
            results.message.should.be.a('object');
            results.operation.should.be.a('string');

            callback();
          });
        }
      ], done);
    });
  });

  describe('update batch function', function() {
    it('archive two messages', function(done) {
      var message1 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var message2 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var messages = [message1, message2];
      var request = {
        operation: 'archive',
        messages: messages
      };
      async.auto({
        store: function(callback) {
          brMessages.store(messages, callback);
        },
        query: ['store', function(callback) {
          brMessages._batchUpdate(request, callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          should.exist(results.query.result);
          should.exist(results.query.messages);
          should.exist(results.query.operation);
          should.not.exist(results.query.error);

          results.query.result.should.be.a('object');
          results.query.messages.should.be.a('array');
          results.query.messages.should.have.length(2);
          results.query.operation.should.be.a('string');

          callback();
        }]
      }, done);
    });
    it('update batch with an unrecognized operation', function(done) {
      var message1 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var message2 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var messages = [message1, message2];
      var request = {
        operation: 'foo',
        messages: messages
      };
      async.series([
        function(callback) {
          brMessages.store(messages, callback);
        },
        function(callback) {
          brMessages._batchUpdate(request, function(err, results) {
            should.exist(err);
            should.exist(results.error);
            should.exist(results.result);
            should.exist(results.messages);
            should.exist(results.operation);

            err.details.httpStatusCode.should.equal(400);

            results.error.should.be.a('object');
            results.result.should.be.a('string');
            results.messages.should.be.a('array');
            results.messages.should.have.length(2);
            results.operation.should.be.a('string');

            callback();
          });
        }
      ], done);
    });
  });

  describe('delete function', function() {
    it('delete one message', function(done) {
      var body = uuid();
      var holder = uuid();
      var link = uuid();
      var recipient = uuid();
      var sender = uuid();
      var subject = uuid();
      var type = uuid();
      var message = helpers.createMessage({
        body: body,
        holder: holder,
        link: link,
        recipient: recipient,
        sender: sender,
        subject: subject,
        type: type
      });
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        query: ['store', function(callback) {
          brMessages._delete(message.id, callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          should.exist(results.query.result);
          should.not.exist(results.query.error);

          results.query.result.should.be.a('object');

          callback();
        }]
      }, done);
    });
  });

  describe('delete batch function', function() {
    it('archive two messages', function(done) {
      var message1 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var message2 = helpers.createMessage({
        body: uuid(),
        holder: uuid(),
        link: uuid(),
        recipient: uuid(),
        sender: uuid(),
        subject: uuid(),
        type: uuid()
      });
      var messages = [message1, message2];
      var request = {
        messages: messages
      };
      async.auto({
        store: function(callback) {
          brMessages.store(messages, callback);
        },
        query: ['store', function(callback) {
          brMessages._batchDelete(request, callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          should.exist(results.query.result);
          should.exist(results.query.messages);
          should.not.exist(results.query.error);

          results.query.result.should.be.a('object');
          results.query.messages.should.be.a('array');
          results.query.messages.should.have.length(2);

          callback();
        }]
      }, done);
    });
  });

  describe('getNew Function', function() {
    afterEach(function(done) {
      helpers.removeCollection('messages', done);
    });
    it('retrieve one NEW messages by recipient', function(done) {
      var body = uuid();
      var recipient = mockData.identities.rsa4096.identity.id;
      var sender = uuid();
      var link = uuid();
      var holder = uuid();
      var subject = uuid();
      var type = uuid();
      var message = helpers.createMessage({
        body: body,
        holder: holder,
        link: link,
        recipient: recipient,
        sender: sender,
        subject: subject,
        type: type
      });
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, recipient, callback);
        },
        test: ['store', 'getIdentity', function(callback, results) {
          brMessages._getNew(
            results.getIdentity[0], recipient, function(err, results) {
            should.not.exist(err);
            should.exist(results);
            results.should.be.an('array');
            results.should.have.length(1);
            var message = results[0];
            message.should.be.an('object');
            should.exist(message['@context']);
            message['@context'].should.be.a('string');
            should.exist(message.date);
            message.date.should.be.a('string');
            should.exist(message.recipient);
            message.recipient.should.be.a('string');
            message.recipient.should.equal(recipient);
            should.exist(message.sender);
            message.sender.should.be.a('string');
            message.sender.should.equal(sender);
            should.exist(message.subject);
            message.subject.should.be.a('string');
            message.subject.should.equal(subject);
            should.exist(message.type);
            message.type.should.be.a('string');
            message.type.should.equal(type);
            should.exist(message.content);
            // check content
            message.content.should.be.an('object');
            var content = message.content;
            should.exist(content.holder);
            content.holder.should.be.a('string');
            content.holder.should.equal(holder);
            should.exist(content.body);
            content.body.should.be.a('string');
            content.body.should.equal(body);
            callback(err);
          });
        }]
      }, done);
    });
    it('message should not contain any unwanted properties', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      async.auto({
        store: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: recipient}), callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, recipient, callback);
        },
        test: ['store', 'getIdentity', function(callback, results) {
          brMessages._getNew(
            results.getIdentity[0], recipient, function(err, results) {
            should.not.exist(err);
            var message = results[0];
            message.should.be.an('object');
            // check message
            _.difference(Object.keys(message), Object.keys(referenceMessage))
              .should.have.length(0);
            // check content
            _.difference(
              Object.keys(message.content),
              Object.keys(referenceMessage.content))
              .should.have.length(0);
            callback(err);
          });
        }]
      }, done);
    });
    it('getNew adds a delivered event', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      var query = {
        recipient: database.hash(recipient)
      };
      var message = helpers.createMessage({recipient: recipient});
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, recipient, callback);
        },
        get: ['store', 'getIdentity', function(callback, results) {
          brMessages._getNew(
            results.getIdentity[0], recipient, function(err, results) {
            results[0].recipient.should.equal(recipient);
            callback(err);
          });
        }],
        query: ['get', function(callback) {
          store.find(query, {}).toArray(callback);
        }],
        test: ['query', function(callback, results) {
          should.exist(results.query);
          results.query.should.have.length(1);
          var r = results.query[0].value;
          r.meta.events.should.have.length(2);
          var events = r.meta.events;
          events[0].type.should.equal('created');
          events[1].type.should.equal('delivered');
          callback();
        }]
      }, done);
    });
    it('not receive the same new message a second time', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      var message = helpers.createMessage({recipient: recipient});
      async.auto({
        store: function(callback) {
          brMessages.store(message, callback);
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, recipient, callback);
        },
        getFirst: ['store', 'getIdentity', function(callback, results) {
          brMessages._getNew(
            results.getIdentity[0], recipient, function(err, results) {
            results[0].recipient.should.equal(recipient);
            callback(err);
          });
        }],
        test: ['getFirst', function(callback, results) {
          brMessages._getNew(
            results.getIdentity[0], recipient, function(err, results) {
            should.not.exist(err);
            should.exist(results);
            results.should.be.an('array');
            results.should.have.length(0);
            callback(err);
          });
        }]
      }, done);
    });
    it('get seven new messages', function(done) {
      var recipient = mockData.identities.rsa4096.identity.id;
      var numberOfMessages = 7;
      var query = {
        recipient: database.hash(recipient)
      };
      async.auto({
        store: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: recipient}), next);
          }, function(err) {
            callback();
          });
        },
        getIdentity: function(callback) {
          brIdentity.get(
            null, recipient, callback);
        },
        get: ['store', 'getIdentity', function(callback, results) {
          brMessages._getNew(results.getIdentity[0], recipient, callback);
        }],
        test: ['get', function(callback, results) {
          should.exist(results.get);
          var r = results.get;
          r.should.be.an('array');
          r.should.have.length(numberOfMessages);
          callback();
        }]
      }, done);
    });
  });
});
