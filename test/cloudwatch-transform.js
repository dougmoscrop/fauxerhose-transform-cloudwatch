'use strict';

const test = require('ava');

const cloudwatch = require('..');

const helpers = require('./_helpers');

test('skips a record that is not zipped', t => {
  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data: Buffer.from('abc').toString('base64') } }]
  })
    .then(({ invalid }) => {
      t.deepEqual(invalid.length, 1);
    });
});

test('skips a zip record with no messageType', t => {
  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data: helpers.zip({ test: 'value' }) } }]
  })
    .then(({ invalid }) => {
      t.true(invalid.length === 1);
      t.deepEqual(invalid[0].reason, 'unknown messageType undefined');
    });
});

test('skips a zip record with wrong messageType', t => {
  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data: helpers.zip({ messageType: 'CONTROL' }) } }]
  })
    .then(({ invalid }) => {
      t.true(invalid.length === 1);
      t.deepEqual(invalid[0].reason, 'unknown messageType CONTROL');
    });
});

test('skips a zip record with missing logEvents', t => {
  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data: helpers.zip({ messageType: 'DATA_MESSAGE' }) } }]
  })
    .then(({ invalid }) => {
      t.true(invalid.length === 1);
      t.deepEqual(invalid[0].reason, 'expected logEvents to be an array but got undefined');
    });
});

test('accepts a zip record with empty logEvents', t => {
  const data = helpers.zip({
    messageType: 'DATA_MESSAGE',
    logEvents: []
  });

  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data } }]
  })
    .then(({ valid, invalid }) => {
      t.deepEqual(valid, []);
      t.deepEqual(invalid, []);
    });
});

test('skips control messages', t => {
  const data = helpers.zip({
    messageType: 'CONTROL_MESSAGE',
  });

  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data } }]
  })
    .then(({ valid, invalid }) => {
      t.deepEqual(valid, []);
      t.deepEqual(invalid, []);
    });
});

test('zip record with logEvents', t => {
  const data = helpers.zip({
    messageType: 'DATA_MESSAGE',
    owner: '1234567890',
    logGroup: 'test',
    logStream: 'test-foo',
    logEvents: [{}, { id: 'abc' }, { message: 'test' }, { id: 'test', message: 'foo' }, { id: 'test2', message: JSON.stringify({ test: 'message ' }) }, {}]
  });

  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data } }]
  })
    .then(({ valid, invalid }) => {
      t.deepEqual(valid, [
        {
          owner: '1234567890',
          logGroup: 'test',
          logStream: 'test-foo',
          logEvent: { id: 'test', message: 'foo' }
        }, {
          owner: '1234567890',
          logGroup: 'test',
          logStream: 'test-foo',
          logEvent: { id: 'test2', message: '{"test":"message "}' }
        }
      ]);
      t.deepEqual(invalid.length, 4);
      t.deepEqual(invalid[0].reason, 'missing message or id');
      t.deepEqual(invalid[1].reason, 'missing message or id');
      t.deepEqual(invalid[2].reason, 'missing message or id');
      t.deepEqual(invalid[3].reason, 'missing message or id');
    });
});