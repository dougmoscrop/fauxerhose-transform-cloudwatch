'use strict';

const test = require('ava');

const cloudwatch = require('..');

const helpers = require('./_helpers');

test('throws with no messageType', async t => {
  try {
    await helpers.run({
      transform: cloudwatch(),
      Records: [{ kinesis: { data: helpers.zip({ test: 'value' }) } }]
    });
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'unknown messageType undefined');
  }
});

test('throws with no messageType', async t => {
  try {
    await helpers.run({
      transform: cloudwatch(),
      Records: [{ kinesis: { data: helpers.zip({ messageType: 'BEEP', test: 'value' }) } }]
    });
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'unknown messageType BEEP');
  }
});

test('throws on a record that is not zipped', async t => {
  try {
    await helpers.run({
      transform: cloudwatch(),
      Records: [{ kinesis: { data: Buffer.from('abc').toString('base64') } }]
    });
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'incorrect header check');
  }
});

test('throws on a record with missing logEvents', async t => {
  try {
    await helpers.run({
      transform: cloudwatch(),
      Records: [{ kinesis: { data: helpers.zip({ messageType: 'DATA_MESSAGE' }) } }]
    });
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'expected logEvents to be an Array but got undefined');
  }
});

test('throws on a message missing id', async t => {
  try {
    const data = helpers.zip({
      messageType: 'DATA_MESSAGE',
      owner: '1234567890',
      logGroup: 'test',
      logStream: 'test-foo',
      logEvents: [{ message: 'test' }]
    });

    await helpers.run({
      transform: cloudwatch(),
      Records: [{ kinesis: { data } }]
    });
    t.fail('should not reach here');
  } catch (err) {
    t.is(err.message, 'missing message or id');
  }
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
    .then(records => {
      t.deepEqual(records, []);
    });
});

test('skips control record with empty logEvents', t => {
  const data = helpers.zip({
    messageType: 'CONTROL_MESSAGE',
  });

  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data } }]
  })
    .then(records => {
      t.deepEqual(records, []);
    });
});

test('zip record with logEvents', t => {
  const data = helpers.zip({
    messageType: 'DATA_MESSAGE',
    owner: '1234567890',
    logGroup: 'test',
    logStream: 'test-foo',
    logEvents: [{ id: 'test', message: 'foo' }, { id: 'test2', message: JSON.stringify({ test: 'message ' }) }]
  });

  return helpers.run({
    transform: cloudwatch(),
    Records: [{ kinesis: { data } }]
  })
    .then(records => {
      t.deepEqual(records, [
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
    });
});

test('supports a parse helper', t => {
  const data = helpers.zip({
    messageType: 'DATA_MESSAGE',
    owner: '1234567890',
    logGroup: 'test',
    logStream: 'test-foo',
    logEvents: [{ id: 'test', message: JSON.stringify({ test: 'message' }) }]
  });

  const parse = (record) => {
    record.logEvent.message = JSON.parse(record.logEvent.message);
    return record;
  };

  return helpers.run({
    transform: cloudwatch({ parse }),
    Records: [{ kinesis: { data } }]
  })
    .then(records => {
      t.deepEqual(records, [
        {
          owner: '1234567890',
          logGroup: 'test',
          logStream: 'test-foo',
          logEvent: { id: 'test', message: { test: 'message' } }
        }
      ]);
    });
});

test('throws when parse is not a function', t => {
  const parse = 'junk';

  const err = t.throws(() => cloudwatch({ parse }));
  t.is(err.message, 'fauxerhose-transform-cloudwatch: parse must be a function');
});