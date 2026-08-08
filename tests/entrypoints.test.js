const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

test('client entrypoint exposes only browser-safe API surface', () => {
  const client = require('../dist/client');

  assert.equal(typeof client.createRealtimeClient, 'function');
  assert.equal(typeof client.room, 'function');
  assert.equal(typeof client.connectRealtime, 'undefined');
  assert.equal(typeof client.createRealtime, 'undefined');
  assert.equal(typeof client.createWebSocketGateway, 'undefined');
  assert.equal(typeof client.PostgresTransport, 'undefined');
  assert.equal(typeof client.MongoTransport, 'undefined');
  assert.equal(typeof client.RedisTransport, 'undefined');
});

test('server entrypoint exposes backend API surface without frontend client', () => {
  const server = require('../dist/server');

  assert.equal(typeof server.connectRealtime, 'function');
  assert.equal(typeof server.createRealtime, 'function');
  assert.equal(typeof server.createWebSocketGateway, 'function');
  assert.equal(typeof server.PostgresTransport, 'function');
  assert.equal(typeof server.MongoTransport, 'function');
  assert.equal(typeof server.RedisTransport, 'function');
  assert.equal(typeof server.createRealtimeClient, 'undefined');
});

test('client bundle does not import server factories, transports or gateway modules', () => {
  const clientSource = fs.readFileSync(path.join(__dirname, '../dist/client.js'), 'utf8');

  assert(!clientSource.includes('./factory'));
  assert(!clientSource.includes('./transports/'));
  assert(!clientSource.includes('./ws/gateway'));
  assert(!clientSource.includes('./ws/connection'));
});

