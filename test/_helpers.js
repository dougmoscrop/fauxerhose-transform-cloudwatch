'use strict';

const stream = require('stream');
const zlib = require('zlib');

const sinon = require('sinon');
const fauxerhose = require('fauxerhose');

module.exports.zip = obj => zlib.gzipSync(JSON.stringify(obj)).toString('base64');

module.exports.encode = function(obj) {
  const str = typeof obj === 'object' ? JSON.stringify(obj) : obj;
  return Buffer.from(str).toString('base64');
};

module.exports.run = function(options) {
  const { Records = [], transform, writeErr } = options;

  const records = [];

  const write = sinon.spy(function(record, encoding, callback) {
    records.push(record);
    callback(writeErr);
  });

  const destination = class Destination extends stream.Writable {
    constructor() {
      super({ objectMode: true });
    }
    _write(record, encoding, callback) {
      return write(record, encoding, callback);
    }
  };

  return fauxerhose({ destination, transform })({ Records })
    .then(() => {
      return records;
    });
};