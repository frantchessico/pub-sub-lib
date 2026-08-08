const assert = require('node:assert/strict');
const test = require('node:test');
const { decodeClientMessage } = require('../dist/ws/protocol');

test('decodeClientMessage rejects invalid JSON and invalid message shape', () => {
  assert.throws(() => decodeClientMessage('{'), /valid JSON/);
  assert.throws(() => decodeClientMessage(JSON.stringify({ type: 'ack', room: 'order:1' })), /sequence/);
  assert.throws(() => decodeClientMessage(JSON.stringify({ type: 'publish', event: { room: 'order:1' } })), /event.type/);
});

test('decodeClientMessage accepts valid messages', () => {
  const message = decodeClientMessage(JSON.stringify({
    type: 'replay_request',
    room: 'order:1',
    fromSequence: 10,
    limit: 100,
  }));

  assert.equal(message.type, 'replay_request');
  assert.equal(message.room, 'order:1');
});

