/*
 * Copyright (c) 2015 Digital Bazaar, Inc. All rights reserved.
 */
// var validation = require('bedrock-validation');
var schemas = require('bedrock-validation').schemas;

var schema = {
  type: 'object',
  title: 'Event',
  properties: {
    '@context': {type: 'string', required: true},
    recipient: {type: 'string', required: true},
    content: {
      required: true,
      properties: {
        link: {type: 'string', required: true},
        holder: {type: 'string', required: true},
        sender: {type: 'string', required: true},
        date: schemas.w3cDateTime({required: true})
      }
    }
  }
};

module.exports = function() {
  return schema;
};
