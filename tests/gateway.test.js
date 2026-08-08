const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const { RealtimeGateway, serveRealtime } = require('../dist/ws/gateway');

class FakeWs extends EventEmitter {
  constructor() {
    super();
    this.sent = [];
    this.closed = null;
    this.readyState = 1;
    this.bufferedAmount = 0;
  }

  send(message) {
    this.sent.push(JSON.parse(message));
  }

  close(code, reason) {
    this.closed = { code, reason };
    this.readyState = 3;
    this.emit('close');
  }
}

function createTransport() {
  return {
    publish: async (event) => ({ ...event, id: event.id ?? 'evt_1', sequence: event.sequence ?? 1, emittedAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 1000).toISOString() }),
    subscribe: () => () => {},
    publishEphemeral: async (event) => ({ ...event, id: event.id ?? 'eph_1', emittedAt: new Date().toISOString() }),
    subscribeEphemeral: () => () => {},
    replay: async () => ({ events: [] }),
    ack: async () => {},
    close: async () => {},
    health: () => ({ provider: 'redis', status: 'healthy' }),
    snapshotMetrics: () => ({ published: 0, received: 0, acked: 0, gapsDetected: 0, errors: 0, replayed: 0, duplicatesDropped: 0, activeRooms: 0, activeListeners: 0, averageDeliveryLagMs: 0 }),
  };
}

test('RealtimeGateway enforces connection limit without throwing', () => {
  const gateway = new RealtimeGateway({ transport: createTransport(), limits: { maxConnections: 1 } });
  const first = new FakeWs();
  const second = new FakeWs();

  assert(gateway.handleConnection(first));
  assert.equal(gateway.handleConnection(second), null);
  assert.equal(second.closed.code, 1013);
  assert.equal(gateway.connectionCount(), 1);
});

test('Connection enforces payload size limit', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    limits: {
      initTimeoutMs: 1000,
      maxPayloadBytes: 20,
      heartbeatIntervalMs: 0,
    },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(ws.closed.code, 4409);
});

test('Connection enforces subscription limit', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    limits: {
      initTimeoutMs: 1000,
      maxSubscriptions: 1,
      heartbeatIntervalMs: 0,
    },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:1' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:2' }));
  await new Promise((resolve) => setImmediate(resolve));

  assert(ws.sent.some((message) => message.type === 'subscribed' && message.room === 'order:1'));
  assert(ws.sent.some((message) => message.type === 'error' && message.message.includes('subscription limit')));
});

test('Connection closes when socket buffered amount exceeds limit', async () => {
  const gateway = new RealtimeGateway({
    transport: {
      ...createTransport(),
      subscribe: (_options, handler) => {
        handler({
          id: 'evt_1',
          room: 'order:1',
          sequence: 1,
          type: 'status.changed',
          emittedAt: new Date().toISOString(),
          expiresAt: new Date(Date.now() + 1000).toISOString(),
          payload: {},
        });
        return () => {};
      },
    },
    limits: {
      initTimeoutMs: 1000,
      heartbeatIntervalMs: 0,
      maxSocketBufferedBytes: 1,
    },
  });
  const ws = new FakeWs();
  ws.bufferedAmount = 10;
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:1' }));
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(ws.closed.code, 1013);
});

test('Connection can disable client-side publish', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    limits: {
      initTimeoutMs: 1000,
      heartbeatIntervalMs: 0,
      allowClientPublish: false,
    },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  ws.emit('message', JSON.stringify({ type: 'publish', event: { room: 'order:1', type: 'status.changed', payload: {} } }));
  await new Promise((resolve) => setImmediate(resolve));

  assert(ws.sent.some((message) => message.type === 'error' && message.message.includes('publish is disabled')));
});

test('gateway sends current presence state to later subscribers', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    limits: {
      initTimeoutMs: 1000,
      heartbeatIntervalMs: 0,
    },
  });
  const first = new FakeWs();
  const second = new FakeWs();
  gateway.handleConnection(first);
  gateway.handleConnection(second);

  first.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  first.emit('message', JSON.stringify({ type: 'subscribe', room: 'chat:123' }));
  first.emit('message', JSON.stringify({
    type: 'presence_enter',
    room: 'chat:123',
    user: {
      userId: 'u1',
      metadata: { name: 'Francisco' },
    },
  }));
  await new Promise((resolve) => setImmediate(resolve));

  second.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-2' }));
  second.emit('message', JSON.stringify({ type: 'subscribe', room: 'chat:123' }));
  await new Promise((resolve) => setImmediate(resolve));

  const presence = second.sent.find((message) => message.type === 'presence' && message.room === 'chat:123');
  assert(presence);
  assert.deepEqual(presence.users.map((user) => user.userId), ['u1']);

  second.emit('message', JSON.stringify({
    type: 'presence_enter',
    room: 'chat:123',
    user: {
      userId: 'u2',
      metadata: { name: 'Maria' },
    },
  }));
  await new Promise((resolve) => setImmediate(resolve));

  const latestForSecond = second.sent.filter((message) => message.type === 'presence').at(-1);
  assert.deepEqual(latestForSecond.users.map((user) => user.userId), ['u1', 'u2']);
});

test('serveRealtime connects provider and starts ready-made gateway', async () => {
  let constructedWith;
  let connectionHandler;
  class FakeWebSocketServer {
    constructor(options) {
      constructedWith = options;
      this.closed = false;
    }

    on(event, handler) {
      if (event === 'connection') {
        connectionHandler = handler;
      }
    }

    close(callback) {
      this.closed = true;
      callback?.();
    }
  }

  const app = await serveRealtime({
    provider: 'redis',
    connection: {
      client: {
        async publish() {
          return 1;
        },
      },
    },
    websocket: {
      port: 9090,
      WebSocketServer: FakeWebSocketServer,
      limits: {
        heartbeatIntervalMs: 0,
      },
    },
  });

  assert.equal(constructedWith.port, 9090);
  assert.equal(typeof connectionHandler, 'function');
  assert.equal(app.gateway.connectionCount(), 0);
  await app.close();
});

// ---------------------------------------------------------------------------
// Autenticação e autorização de rooms
// ---------------------------------------------------------------------------

const flush = () => new Promise((resolve) => setImmediate(resolve));

test('Connection rejects init with an invalid token and closes', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: { jwt: async () => { throw new Error('bad token'); } },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1', token: 'forged' }));
  await flush();

  assert(ws.sent.some((message) => message.type === 'error'));
  assert(!ws.sent.some((message) => message.type === 'ready'));
  assert.equal(ws.closed?.code, 4401);
});

test('Connection rejects init without a token when auth is configured', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: { jwt: async () => ({ sub: 'user_1' }) },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1' }));
  await flush();

  assert(!ws.sent.some((message) => message.type === 'ready'));
  assert.equal(ws.closed?.code, 4401);
});

test('Connection cannot subscribe after a failed init', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: { jwt: async () => { throw new Error('bad token'); } },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1', token: 'forged' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:1' }));
  await flush();
  await flush();

  assert(!ws.sent.some((message) => message.type === 'subscribed'));
});

test('Connection enforces the room authorizer on subscribe', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: {
      jwt: async () => ({ sub: 'user_1' }),
      authorize: async (context) => context.room === 'order:mine',
    },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1', token: 'valid' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:mine' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:someone-else' }));
  await flush();
  await flush();

  assert(ws.sent.some((message) => message.type === 'subscribed' && message.room === 'order:mine'));
  assert(!ws.sent.some((message) => message.type === 'subscribed' && message.room === 'order:someone-else'));
  assert(ws.sent.some((message) => message.type === 'error' && message.message.includes('Access denied')));
});

test('Connection enforces the room authorizer on replay', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: {
      jwt: async () => ({ sub: 'user_1' }),
      authorize: async () => false,
    },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1', token: 'valid' }));
  ws.emit('message', JSON.stringify({ type: 'replay_request', room: 'order:someone-else', fromSequence: 0 }));
  await flush();
  await flush();

  assert(!ws.sent.some((message) => message.type === 'replay'));
  assert(ws.sent.some((message) => message.type === 'error' && message.message.includes('Access denied')));
});

test('Messages sent immediately after init are processed in order', async () => {
  const gateway = new RealtimeGateway({
    transport: createTransport(),
    auth: { jwt: async () => ({ sub: 'user_1' }), authorize: async () => true },
    limits: { initTimeoutMs: 1000, heartbeatIntervalMs: 0 },
  });
  const ws = new FakeWs();
  gateway.handleConnection(ws);

  // O cliente não espera pelo `ready` — a fila por ligação garante a ordem.
  ws.emit('message', JSON.stringify({ type: 'init', subscriberId: 'device-1', token: 'valid' }));
  ws.emit('message', JSON.stringify({ type: 'subscribe', room: 'order:1' }));
  await flush();
  await flush();

  assert(ws.sent.some((message) => message.type === 'ready'));
  assert(ws.sent.some((message) => message.type === 'subscribed' && message.room === 'order:1'));
});
