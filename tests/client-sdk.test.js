const assert = require('node:assert/strict');
const test = require('node:test');
const { createRealtimeClient } = require('../dist/client-sdk');

class FakeWebSocket {
  static instances = [];

  constructor() {
    this.readyState = 0;
    this.sent = [];
    this.listeners = new Map();
    FakeWebSocket.instances.push(this);
  }

  send(message) {
    this.sent.push(JSON.parse(message));
  }

  close() {
    this.readyState = 3;
    this.emit('close', {});
  }

  addEventListener(event, handler) {
    const handlers = this.listeners.get(event) ?? [];
    handlers.push(handler);
    this.listeners.set(event, handlers);
  }

  emit(event, payload) {
    for (const handler of this.listeners.get(event) ?? []) {
      handler(payload);
    }
  }

  open() {
    this.readyState = 1;
    this.emit('open', {});
  }
}

test('RealtimeClient queues outbound messages until socket opens', () => {
  FakeWebSocket.instances = [];
  const client = createRealtimeClient({
    url: 'ws://example.test',
    subscriberId: 'device-1',
    WebSocket: FakeWebSocket,
  });

  client.connect();
  client.publish({ room: 'order:1', type: 'status.changed', payload: { status: 'ready' } });

  const socket = FakeWebSocket.instances[0];
  assert.equal(socket.sent.length, 0);

  socket.open();
  assert.equal(socket.sent[0].type, 'init');
  assert.equal(socket.sent[1].type, 'publish');
});

test('RealtimeClient reports malformed server messages instead of throwing', () => {
  FakeWebSocket.instances = [];
  const errors = [];
  const client = createRealtimeClient({
    url: 'ws://example.test',
    subscriberId: 'device-1',
    WebSocket: FakeWebSocket,
    onError: (error) => errors.push(error),
  });

  client.connect();
  const socket = FakeWebSocket.instances[0];
  socket.open();
  socket.emit('message', { data: '{' });

  assert.equal(errors.length, 1);
});

test('RealtimeClient acknowledges only after async handlers resolve', async () => {
  FakeWebSocket.instances = [];
  let resolveHandler;
  const client = createRealtimeClient({
    url: 'ws://example.test',
    subscriberId: 'device-1',
    WebSocket: FakeWebSocket,
  });

  client.connect();
  const socket = FakeWebSocket.instances[0];
  socket.open();
  client.subscribe('order:1', () => new Promise((resolve) => {
    resolveHandler = resolve;
  }));

  socket.emit('message', {
    data: JSON.stringify({
      type: 'event',
      data: {
        id: 'evt_1',
        room: 'order:1',
        sequence: 1,
        type: 'status.changed',
        emittedAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 1000).toISOString(),
        payload: {},
      },
    }),
  });

  assert(!socket.sent.some((message) => message.type === 'ack' && message.sequence === 1));
  resolveHandler();
  await new Promise((resolve) => setImmediate(resolve));
  assert(socket.sent.some((message) => message.type === 'ack' && message.sequence === 1));
});
