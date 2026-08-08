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

test('MongoTransport supports MongoDB drivers that return document directly from findOneAndUpdate', async () => {
  const inserted = [];
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne(doc) {
        inserted.push(doc);
      },
    }),
    counters: createCollection({
      async findOneAndUpdate() {
        return { room: 'order:123', sequence: 7 };
      },
    }),
    subscribers: createCollection(),
  });

  const event = await transport.publish({
    room: 'order:123',
    type: 'status.changed',
    payload: { status: 'ready' },
  });

  assert.equal(event.sequence, 7);
  assert.equal(inserted[0].sequence, 7);
});

test('MongoTransport returns existing event when duplicate id insert races', async () => {
  const existing = {
    id: 'evt_fixed',
    room: 'order:123',
    sequence: 1,
    type: 'status.changed',
    emittedAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { status: 'ready' },
    metadata: { provider: 'mongo' },
  };
  let findCalls = 0;
  const transport = new MongoTransport({
    events: createCollection({
      async insertOne() {
        const error = new Error('duplicate');
        error.code = 11000;
        throw error;
      },
      find(query) {
        return {
          sort: () => ({
            limit: () => ({
              async toArray() {
                findCalls += 1;
                return query.id === 'evt_fixed' && findCalls > 1 ? [{ envelope: existing }] : [];
              },
            }),
          }),
        };
      },
    }),
    counters: createCollection(),
    subscribers: createCollection(),
  });

  const event = await transport.publish({
    id: 'evt_fixed',
    room: 'order:123',
    type: 'status.changed',
    payload: { status: 'ready' },
  });

  assert.equal(event.id, 'evt_fixed');
  assert.equal(event.sequence, 1);
});

test('MongoTransport subscribe replays events after last ack for returning subscriber', async () => {
  const oldEvent = {
    id: 'evt_1',
    room: 'bank:transactions',
    sequence: 1,
    type: 'transaction.created',
    emittedAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { ignored: true },
  };
  const pendingEvent = {
    id: 'evt_2',
    room: 'bank:transactions',
    sequence: 2,
    type: 'transaction.created',
    emittedAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { amount: 1500.75 },
  };
  const received = [];

  const transport = new MongoTransport({
    events: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit: () => ({
              async toArray() {
                return [oldEvent, pendingEvent]
                  .filter((event) => event.room === query.room)
                  .filter((event) => event.sequence > query.sequence.$gt);
              },
            }),
          }),
        };
      },
    }),
    counters: createCollection(),
    subscribers: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit: () => ({
              async toArray() {
                return query.room === 'bank:transactions' && query.subscriberId === 'user-1'
                  ? [{ room: query.room, subscriberId: query.subscriberId, lastAckSequence: 1 }]
                  : [];
              },
            }),
          }),
        };
      },
    }),
  });

  transport.subscribe(
    {
      room: 'bank:transactions',
      subscriberId: 'user-1',
      eventTypes: ['transaction.created'],
    },
    (event) => {
      received.push(event);
    },
  );
  await new Promise((resolve) => setImmediate(resolve));

  assert.deepEqual(received.map((event) => event.sequence), [2]);
  assert.deepEqual(received[0].payload, { amount: 1500.75 });
});

test('MongoTransport catch-up paginates returning subscriber backlog', async () => {
  const now = new Date().toISOString();
  const events = Array.from({ length: 7 }, (_, index) => ({
    id: `evt_${index + 1}`,
    room: 'bank:transactions',
    sequence: index + 1,
    type: 'transaction.created',
    emittedAt: now,
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { index: index + 1 },
  }));
  const limits = [];
  const received = [];

  const transport = new MongoTransport({
    events: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit(value) {
              limits.push(value);
              return {
                async toArray() {
                  return events
                    .filter((event) => event.room === query.room)
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
    subscribers: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit: () => ({
              async toArray() {
                return query.room === 'bank:transactions' && query.subscriberId === 'user-1'
                  ? [{ room: query.room, subscriberId: query.subscriberId, lastAckSequence: 0 }]
                  : [];
              },
            }),
          }),
        };
      },
    }),
  });

  transport.subscribe(
    {
      room: 'bank:transactions',
      subscriberId: 'user-1',
      eventTypes: ['transaction.created'],
      catchUp: { batchSize: 3 },
    },
    (event) => {
      received.push(event.sequence);
    },
  );

  await waitFor(() => received.length === 7);

  assert.deepEqual(received, [1, 2, 3, 4, 5, 6, 7]);
  assert.deepEqual(limits.filter((value) => value === 3), [3, 3, 3]);
});

test('MongoTransport exposes catchUp async iterator for manual batch processing', async () => {
  const now = new Date().toISOString();
  const events = Array.from({ length: 5 }, (_, index) => ({
    id: `evt_${index + 1}`,
    room: 'bank:transactions',
    sequence: index + 1,
    type: 'transaction.created',
    emittedAt: now,
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { index: index + 1 },
  }));

  const transport = new MongoTransport({
    events: createCollection({
      find(query) {
        return {
          sort: () => ({
            limit(value) {
              return {
                async toArray() {
                  return events
                    .filter((event) => event.room === query.room)
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
    subscribers: createCollection({
      find() {
        return {
          sort: () => ({
            limit: () => ({
              async toArray() {
                return [{ lastAckSequence: 2 }];
              },
            }),
          }),
        };
      },
    }),
  });

  const sequences = [];
  for await (const event of transport.catchUp({
    room: 'bank:transactions',
    subscriberId: 'user-1',
    batchSize: 2,
  })) {
    sequences.push(event.sequence);
  }

  assert.deepEqual(sequences, [3, 4, 5]);
});

async function waitFor(predicate, timeoutMs = 1000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail('Timed out waiting for condition');
}
