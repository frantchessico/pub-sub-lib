const assert = require('node:assert/strict');
const test = require('node:test');
const { MongoTransport } = require('../dist/transports/mongo');

function createCollection(overrides = {}) {
  return {
    async insertOne() {},
    find() {
      return { sort: () => ({ limit: () => ({ async toArray() { return []; } }) }) };
    },
    async updateOne() {},
    async findOneAndUpdate() {
      return { sequence: 1 };
    },
    ...overrides,
  };
}

function createEvent(sequence) {
  return {
    id: `evt_${sequence}`,
    room: 'chat:123',
    sequence,
    type: 'message.created',
    emittedAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { sequence },
  };
}

test('transport stores snapshots and streams replay from the snapshot cursor', async () => {
  const events = [createEvent(10), createEvent(11), createEvent(12)];
  const transport = new MongoTransport({
    events: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit(value) {
              return {
                async toArray() {
                  return events
                    .filter((event) => event.sequence > query.sequence.$gt)
                    .slice(0, value);
                },
              };
            },
          }),
        };
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  await transport.snapshot('chat:123', {
    lastSequence: 10,
    state: { messages: ['hello'], participants: ['u1'] },
  });

  const snapshot = await transport.getSnapshot('chat:123');
  const streamed = [];
  for await (const event of transport.streamReplay({ room: 'chat:123', fromSequence: snapshot.lastSequence, batchSize: 1 })) {
    streamed.push(event.sequence);
  }

  assert.equal(snapshot.lastSequence, 10);
  assert.deepEqual(snapshot.state, { messages: ['hello'], participants: ['u1'] });
  assert.deepEqual(streamed, [11, 12]);
});

test('transport retries failed subscribers and moves exhausted events to DLQ', async () => {
  const transport = new MongoTransport({
    events: createCollection(),
    counters: createCollection(),
    subscribers: createCollection(),
  });
  let attempts = 0;

  transport.subscribe(
    {
      room: 'chat:123',
      catchUp: false,
      retry: {
        attempts: 2,
        strategy: 'fixed',
        baseDelayMs: 1,
      },
    },
    async () => {
      attempts += 1;
      throw new Error('consumer exploded');
    },
  );

  await transport.publish({
    room: 'chat:123',
    type: 'message.created',
    payload: { text: 'boom' },
  });
  await waitFor(async () => (await transport.dlq('chat:123').list()).length === 1);

  const failed = await transport.dlq('chat:123').list();
  const replayed = [];
  for await (const event of transport.dlq('chat:123').replay()) {
    replayed.push(event);
  }

  assert.equal(attempts, 3);
  assert.equal(failed[0].attempts, 3);
  assert.equal(failed[0].error, 'consumer exploded');
  assert.equal(replayed[0].payload.text, 'boom');
});

test('middleware can enrich published events before persistence', async () => {
  const inserted = [];
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne(doc) {
        inserted.push(doc);
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  transport.use(async (ctx, next) => {
    if (ctx.action === 'publish') {
      ctx.event.metadata = { ...ctx.event.metadata, traceId: 'trace_123' };
    }
    await next();
  });

  const event = await transport.publish({
    room: 'chat:123',
    type: 'message.created',
    payload: { text: 'hello' },
  });

  assert.equal(event.metadata.traceId, 'trace_123');
  assert.equal(inserted[0].metadata.traceId, 'trace_123');
});

test('middleware wraps publish in before/core/after order', async () => {
  const calls = [];
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne() {
        calls.push('core');
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  transport.use(async (ctx, next) => {
    calls.push(`before:${ctx.action}`);
    await next();
    calls.push(`after:${ctx.action}`);
  });

  await transport.publish({
    room: 'chat:123',
    type: 'message.created',
    payload: {},
  });

  assert.deepEqual(calls, ['before:publish', 'core', 'after:publish']);
});

test('middleware blocks publish before core operation', async () => {
  let inserted = false;
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne() {
        inserted = true;
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  transport.use(async () => {
    throw new Error('blocked');
  });

  await assert.rejects(
    () => transport.publish({
      room: 'chat:123',
      type: 'message.created',
      payload: {},
    }),
    /blocked/,
  );
  assert.equal(inserted, false);
});

test('middleware rejects duplicate next calls', async () => {
  const transport = new MongoTransport({
    events: createCollection(),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  transport.use(async (_ctx, next) => {
    await next();
    await next();
  });

  await assert.rejects(
    () => transport.publish({
      room: 'chat:123',
      type: 'message.created',
      payload: {},
    }),
    /next\(\) called multiple times/,
  );
});

test('connectRealtime accepts middleware array', async () => {
  const { createRealtime } = require('../dist/factory');
  const calls = [];
  const transport = createRealtime({
    provider: 'mongo',
    connection: {
      events: createCollection({
        async insertOne() {
          calls.push('core');
        },
      }),
      counters: createCollection(),
      subscribers: createCollection(),
    },
    middleware: [
      async (ctx, next) => {
        calls.push(`before:${ctx.action}`);
        await next();
        calls.push(`after:${ctx.action}`);
      },
    ],
  });

  await transport.publish({
    room: 'chat:123',
    type: 'message.created',
    payload: {},
  });

  assert.deepEqual(calls, ['before:publish', 'core', 'after:publish']);
});

test('ephemeral events are delivered without persistence', async () => {
  const inserted = [];
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne(doc) {
        inserted.push(doc);
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });
  const received = [];

  transport.subscribeEphemeral({ room: 'chat:123', eventTypes: ['typing'] }, (event) => {
    received.push(event);
  });

  await transport.publishEphemeral({
    room: 'chat:123',
    type: 'typing',
    payload: { userId: 'u1' },
  });
  await waitFor(() => received.length === 1);

  assert.equal(received[0].sequence, undefined);
  assert.deepEqual(received[0].payload, { userId: 'u1' });
  assert.equal(inserted.length, 0);
});

test('wildcard room subscriptions receive matching rooms only', async () => {
  const transport = new MongoTransport({
    events: createCollection(),
    counters: createCollection(),
    subscribers: createCollection(),
  });
  const received = [];

  transport.subscribe({ room: 'chat:*', catchUp: false }, (event) => {
    received.push(event.room);
  });

  await transport.publish({ room: 'chat:1', type: 'message.created', payload: {} });
  await transport.publish({ room: 'order:1', type: 'status.changed', payload: {} });
  await waitFor(() => received.length === 1);

  assert.deepEqual(received, ['chat:1']);
});

async function waitFor(predicate, timeoutMs = 1000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail('Timed out waiting for condition');
}
