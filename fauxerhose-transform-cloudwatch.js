'use strict';

const unzip = require('./lib/unzip');

module.exports = function () {
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
                this.emit('data', { owner, logGroup, logStream, logEvent });
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
