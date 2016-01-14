/*
 * Copyright (c) 2015 Digital Bazaar, Inc. All rights reserved.
 */

var config = require('bedrock').config;
var path = require('path');

config.messages = {};
// these messages will be inserted into the database at startup
config.messages.newMessages = [];
config.messages.endpoints = {};
config.messages.endpoints.messages = '/messages';
config.messages.endpoints.messagesSearch = '/messages-search';
config.messages.endpoints.messagesBatch = '/messages/batch';

// load validation schemas
config.validation.schema.paths.push(path.join(__dirname, '..', 'schemas'));
