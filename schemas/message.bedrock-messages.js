/*
 * Copyright (c) 2015-2016 Digital Bazaar, Inc. All rights reserved.
 */
var schemas = require('bedrock-validation').schemas;

var schema = {
  type: 'object',
  title: 'Event',
  properties: {
    '@context': {type: 'string', required: true},
    date: schemas.w3cDateTime({required: true}),
    recipient: {type: 'string', required: true},
    sender: {type: 'string', required: true},
    subject: {type: 'string', required: false},
    type: {type: 'string', required: true},
    content: {
      required: true,
      properties: {
        body: {type: 'string', required: false}
      }
    }
  }
};

module.exports = function() {
  return schema;
};
