const assert = require('node:assert/strict');
const test = require('node:test');
const { parseSseBlock, formatSseCursor, parseSseCursor, serializeSinceMap, deserializeSinceMap } = require('../dist/sse-stream');

test('parseSseBlock parses SSE block', () => {
  const block = ['id: user%3A1@7', 'event: user:order:update', 'data: {"ok":true}', ''].join('\n');
  const message = parseSseBlock(block);
  assert.equal(message.event, 'user:order:update');
  assert.equal(message.id, 'user%3A1@7');
  assert.deepEqual(JSON.parse(message.data), { ok: true });
});

test('serializeSinceMap roundtrips multi-room cursors', () => {
  const map = new Map([
    ['user:1', 10],
    ['vendor:2', 3],
  ]);
  const serialized = serializeSinceMap(map);
  const restored = deserializeSinceMap(serialized);
  assert.equal(restored.get('user:1'), 10);
  assert.equal(restored.get('vendor:2'), 3);
});

test('formatSseCursor encodes room and sequence', () => {
  const cursor = formatSseCursor('driver:abc', 99);
  assert.equal(parseSseCursor(cursor).room, 'driver:abc');
  assert.equal(parseSseCursor(cursor).sequence, 99);
});
