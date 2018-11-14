'use strict';

const unzip = require('./lib/unzip');

const identity = i => i;

module.exports = function (options = {}) {
  const { parse = identity } = options;

  if (typeof parse !== 'function') {
    throw new Error('cloudwatch-transform: parse must be a function');
  }
  
  return function cloudwatchTransform(record) {
    return Promise.resolve()
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
                const parsed = parse(record);
                this.emit('data', parsed);
              } else {
                this.emit('invalid', { reason: 'missing message or id', logEvent });
              }
            });
          } else {
            throw new Error(`expected logEvents to be an array but got ${typeof parsed.logEvents}`);
          }
        } else {
          throw new Error(`unknown messageType ${parsed.messageType}`);
        }
      })
      .catch(e => {
        this.emit('invalid', { reason: e.message, record });
      });
  };
};
