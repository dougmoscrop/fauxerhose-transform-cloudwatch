'use strict';

const assert = require('assert');
const { Transform } = require('stream');

const unzip = require('./lib/unzip');

const identity = i => i;

module.exports = (options = {}) => {
  const { parse = identity } = options;

  assert(typeof parse === 'function', 'fauxerhose-transform-cloudwatch: parse must be a function');

  return class CloudWatchTransform extends Transform {

    constructor() {
      super({ objectMode: true });
    }

    _transform(record, encoding, callback) {
      Promise.resolve()
        .then(() => {
          return unzip(record);
        })
        .then(unzipped => {
          return JSON.parse(unzipped.toString('utf8'));
        })
        .then(parsed => {
          if (parsed.messageType === 'CONTROL_MESSAGE') {
            return;
          }
          if (parsed.messageType === 'DATA_MESSAGE') {
            const { owner, logGroup, logStream } = parsed;

            if (Array.isArray(parsed.logEvents)) {
              parsed.logEvents.forEach(logEvent => {
                if (logEvent.message && logEvent.id) {
                  const record = { owner, logGroup, logStream, logEvent };
                  const data = parse(record);

                  this.emit('data', data);
                } else {
                  throw new Error('missing message or id');
                }
              });
            } else {
              throw new Error(`expected logEvents to be an Array but got ${typeof parsed.logEvents}`);
            }
          } else {
            throw new Error(`unknown messageType ${parsed.messageType}`);
          }
        })
        .then(() => callback())
        .catch(e => callback(e));
    }
  };
};
