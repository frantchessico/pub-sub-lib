const assert = require('node:assert/strict');
const test = require('node:test');
const { RedisTransport } = require('../dist/transports/redis');

test('RedisTransport uses atomic per-room INCR sequence and dedupes local/pubsub delivery', async () => {
  const handlers = new Map();
  const counters = new Map();
  const client = {
    async incr(key) {
      const next = (counters.get(key) ?? 0) + 1;
      counters.set(key, next);
      return next;
    },
    async xAdd() {},
    async publish(channel, message) {
      handlers.get(channel)?.(message);
      return 1;
    },
    async subscribe(channel, handler) {
      handlers.set(channel, handler);
    },
    async unsubscribe(channel) {
      handlers.delete(channel);
    },
    async hSet() {},
  };

  const transport = new RedisTransport({ client });
  const received = [];
  transport.subscribe({ room: 'order:123' }, (event) => received.push(event));

  const first = await transport.publish({ room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });
  const second = await transport.publish({ room: 'order:123', type: 'status.changed', payload: { status: 'done' } });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(first.sequence, 1);
  assert.equal(second.sequence, 2);
  assert.equal(received.length, 2);
  assert.deepEqual(received.map((event) => event.sequence), [1, 2]);
  assert.equal(transport.snapshotMetrics().published, 2);
});

test('RedisTransport keeps shared channel subscription until last local listener unsubscribes', async () => {
  let subscribeCount = 0;
  let unsubscribeCount = 0;
  const client = {
    async incr() {
      return 1;
    },
    async xAdd() {},
    async publish() {
      return 1;
    },
    async subscribe() {
      subscribeCount += 1;
    },
    async unsubscribe() {
      unsubscribeCount += 1;
    },
  };

  const transport = new RedisTransport({ client });
  const unsubscribeA = transport.subscribe({ room: 'order:123' }, () => {});
  const unsubscribeB = transport.subscribe({ room: 'order:123' }, () => {});

  assert.equal(subscribeCount, 1);
  await unsubscribeA();
  assert.equal(unsubscribeCount, 0);
  await unsubscribeB();
  assert.equal(unsubscribeCount, 1);
});

test('RedisTransport returns cached envelope for repeated idempotent publish', async () => {
  const store = new Map();
  let sequence = 0;
  const client = {
    async incr() {
      sequence += 1;
      return sequence;
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async set(key, value) {
      store.set(key, value);
    },
    async xAdd() {},
    async publish() {
      return 1;
    },
  };

  const transport = new RedisTransport({ client });
  const first = await transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });
  const second = await transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });

  assert.equal(first.id, second.id);
  assert.equal(first.sequence, second.sequence);
  assert.equal(sequence, 1);
});

test('RedisTransport reserves idempotency key before publishing concurrent duplicate ids', async () => {
  const store = new Map();
  let sequence = 0;
  let publishCount = 0;
  const client = {
    async incr() {
      sequence += 1;
      return sequence;
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async set(key, value, options) {
      if (options?.NX && store.has(key)) {
        return null;
      }
      store.set(key, value);
      return 'OK';
    },
    async del(key) {
      store.delete(key);
    },
    async xAdd() {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return '1-0';
    },
    async publish() {
      publishCount += 1;
      return 1;
    },
  };

  const transport = new RedisTransport({ client });
  const [first, second] = await Promise.all([
    transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } }),
    transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } }),
  ]);

  assert.equal(first.id, second.id);
  assert.equal(first.sequence, second.sequence);
  assert.equal(sequence, 1);
  assert.equal(publishCount, 1);
});


test('RedisTransport applies listener backpressure limit', async () => {
  let sequence = 0;
  const client = {
    async incr() {
      sequence += 1;
      return sequence;
    },
    async xAdd() {},
    async publish() {
      return 1;
    },
  };
  const transport = new RedisTransport({
    client,
    resilience: {
      maxPendingPerListener: 1,
    },
  });
  let handled = 0;

  transport.subscribe({ room: 'order:123' }, () => {
    handled += 1;
    return new Promise(() => {});
  });

  await transport.publish({ room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });
  await transport.publish({ room: 'order:123', type: 'status.changed', payload: { status: 'done' } });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(handled, 1);
  assert.equal(transport.snapshotMetrics().errors, 1);
});

test('RedisTransport uses sequence stream index for replay when available', async () => {
  const store = new Map();
  const xRangeCalls = [];
  const client = {
    async incr() {
      return 1;
    },
    async set(key, value) {
      store.set(key, value);
    },
    async get(key) {
      return store.get(key) ?? null;
    },
    async xAdd() {
      return '10-0';
    },
    async publish() {
      return 1;
    },
    async xRange(key, start, end, options) {
      xRangeCalls.push({ key, start, end, options });
      return [
        {
          id: '10-0',
          message: {
            envelope: JSON.stringify({
              id: 'evt_1',
              room: 'order:123',
              sequence: 1,
              type: 'status.changed',
              emittedAt: new Date().toISOString(),
              expiresAt: new Date(Date.now() + 1000).toISOString(),
              payload: {},
            }),
          },
        },
      ];
    },
  };

  const transport = new RedisTransport({ client });
  await transport.publish({ room: 'order:123', type: 'status.changed', payload: {} });
  await transport.replay({ room: 'order:123', fromSequence: 1 });

  assert.equal(xRangeCalls[0].start, '10-0');
});

test('RedisTransport can run as fanout-only without stream persistence', async () => {
  let xAddCalled = false;
  const client = {
    async incr() {
      return 1;
    },
    async xAdd() {
      xAddCalled = true;
    },
    async publish() {
      return 1;
    },
  };

  const transport = new RedisTransport({ client, persistStreams: false });
  await transport.publish({ room: 'order:123', type: 'status.changed', payload: {} });
  const replay = await transport.replay({ room: 'order:123', fromSequence: 0 });

  assert.equal(xAddCalled, false);
  assert.deepEqual(replay.events, []);
});

test('RedisTransport stores snapshots natively with Redis keys', async () => {
  const store = new Map();
  const client = {
    async set(key, value) {
      store.set(key, value);
    },
    async get(key) {
      return store.get(key) ?? null;
    },
  };

  const transport = new RedisTransport({ client });
  await transport.snapshot('chat:123', {
    lastSequence: 42,
    state: { messages: ['hello'] },
  });
  const snapshot = await transport.getSnapshot('chat:123');

  assert.equal(snapshot.lastSequence, 42);
  assert.deepEqual(snapshot.state, { messages: ['hello'] });
});

test('RedisTransport stores DLQ natively in Redis streams', async () => {
  const streams = new Map();
  const client = {
    async xAdd(key, id, message) {
      const records = streams.get(key) ?? [];
      records.push({ id: id === '*' ? `${records.length + 1}-0` : id, message });
      streams.set(key, records);
      return records.at(-1).id;
    },
    async xRange(key) {
      return streams.get(key) ?? [];
    },
    async del(key) {
      streams.delete(key);
    },
  };

  const transport = new RedisTransport({ client });
  transport.subscribe({
    room: 'chat:123',
    catchUp: false,
    retry: { attempts: 0 },
  }, async () => {
    throw new Error('boom');
  });

  await transport.publish({ room: 'chat:123', type: 'message.created', payload: {} });
  await new Promise((resolve) => setImmediate(resolve));
  const failed = await transport.dlq('chat:123').list();

  assert.equal(failed.length, 1);
  assert.equal(failed[0].error, 'boom');
  assert.equal(await transport.dlq('chat:123').clear(), 1);
});
