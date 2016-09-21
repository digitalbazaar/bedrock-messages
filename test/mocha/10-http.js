/*
 * Copyright (c) 2015-2016 Digital Bazaar, Inc. All rights reserved.
 */
/* globals describe, before, after, it, should */
/* jshint node: true */

'use strict';

var async = require('async');
var bedrock = require('bedrock');
var brMessages = require('bedrock-messages');
var config = bedrock.config;
var helpers = require('./helpers');
var request = require('request');
var mockData = require('./mock.data');
var url = require('url');
var util = bedrock.util;
var uuid = require('uuid').v4;
request = request.defaults({json: true, strictSSL: false});

var urlObj = {
  protocol: 'https',
  host: config.server.host,
  pathname: config.messages.endpoints.messages
};

describe('bedrock-messages HTTP API', function() {
  describe('unauthenticated requests', function() {
    it('should respond with 400 - PermissionDenied', done => {
      var user = mockData.identities.rsa4096;
      var clonedUrlObj = util.clone(urlObj);
      clonedUrlObj.query = {
        recipient: user.identity.id
      };
      request.get({
        url: url.format(clonedUrlObj)
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
    before('Prepare the database', done => {
      helpers.prepareDatabase(mockData, done);
    });
    after('Remove test data', done => {
      helpers.removeCollections(done);
    });

    it('return empty array if there are no new messages', done => {
      var user = mockData.identities.rsa4096;
      var clonedUrlObj = util.clone(urlObj);
      clonedUrlObj.query = {
        recipient: user.id,
        state: 'new'
      };
      request.get(
        helpers.createHttpSigRequest(url.format(clonedUrlObj), user),
        function(err, res, body) {
          should.not.exist(err);
          res.statusCode.should.equal(200);
          should.exist(body);
          body.should.be.an('array');
          body.should.have.length(0);
          done();
        });
    });
    it('return one new message', done => {
      var user = mockData.identities.rsa4096;
      var clonedUrlObj = util.clone(urlObj);
      clonedUrlObj.query = {
        recipient: user.identity.id,
        state: 'new'
      };
      async.auto({
        insert: function(callback) {
          brMessages.store(
            helpers.createMessage({recipient: user.identity.id}), callback);
        },
        get: ['insert', function(callback) {
          request.get(
            helpers.createHttpSigRequest(url.format(clonedUrlObj), user),
            function(err, res, body) {
              should.not.exist(err);
              res.statusCode.should.equal(200);
              should.exist(body);
              body.should.be.an('array');
              body.should.have.length(1);
              callback();
            });
        }]
      }, done);
    });
    // only returns 7 new messages, not including the 1 that was already
    // retrieved during the previous test
    it('return seven new messages', done => {
      var user = mockData.identities.rsa4096;
      var clonedUrlObj = util.clone(urlObj);
      clonedUrlObj.query = {
        recipient: user.identity.id,
        state: 'new'
      };
      var numberOfMessages = 7;
      async.auto({
        insert: function(callback) {
          async.times(numberOfMessages, function(n, next) {
            brMessages.store(
              helpers.createMessage({recipient: user.identity.id}), next);
          }, callback);
        },
        get: ['insert', function(callback) {
          request.get(
            helpers.createHttpSigRequest(url.format(clonedUrlObj), user),
            function(err, res, body) {
              should.not.exist(err);
              res.statusCode.should.equal(200);
              should.exist(body);
              body.should.be.an('array');
              body.should.have.length(numberOfMessages);
              callback();
            });
        }]
      }, done);
    });
    // no query specified here, all messages for the authenticated identity
    // should be returned
    it('returns eight messages for authenticated identity', done => {
      var user = mockData.identities.rsa4096;
      var clonedUrlObj = util.clone(urlObj);
      request.get(
        helpers.createHttpSigRequest(url.format(clonedUrlObj), user),
        function(err, res, body) {
          should.not.exist(err);
          res.statusCode.should.equal(200);
          should.exist(body);
          body.should.be.an('array');
          body.should.have.length(8);
          done();
        });
    });
    it('does not allow access to another user\'s messages', done => {
      var user = mockData.identities.rsa4096;
      var badUserId = 'did:' + uuid();
      var clonedUrlObj = util.clone(urlObj);
      clonedUrlObj.query = {
        recipient: badUserId,
        state: 'new'
      };
      request.get(
        helpers.createHttpSigRequest(url.format(clonedUrlObj), user),
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
  });
});
