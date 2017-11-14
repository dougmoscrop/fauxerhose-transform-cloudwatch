'use strict';

const zlib = require('zlib');

module.exports = function unzip(record) {
  return new Promise((resolve, reject) => {
    zlib.unzip(record, (err, unzipped) => {
      if (err) {
        reject(err);
      } else {
        resolve(unzipped);
      }
    });
  });
};