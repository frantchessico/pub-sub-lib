const assert = require('node:assert/strict');
const test = require('node:test');
const { HybridTransport } = require('../dist/transports/hybrid');

function createEvent(sequence) {
  return {
    id: `evt_${sequence}`,
    room: 'bank:transactions',
    sequence,
    type: 'transaction.created',
    emittedAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 1000).toISOString(),
    payload: { sequence },
  };
}

test('HybridTransport drains catch-up from durable storage and live events from realtime leg', async () => {
  const storageEvents = [createEvent(1), createEvent(2), createEvent(3)];
  let realtimeHandler;

  const storage = {
    async publish(event) {
      return event;
    },
    subscribe() {
      return () => {};
    },
    async replay({ fromSequence, limit }) {
      return {
        events: storageEvents
          .filter((event) => event.sequence > fromSequence)
          .slice(0, limit),
      };
    },
    async *catchUp({ batchSize }) {
      let cursor = 0;
      while (true) {
        const events = storageEvents
          .filter((event) => event.sequence > cursor)
          .slice(0, batchSize);
        if (!events.length) {
          return;
        }
        for (const event of events) {
          cursor = event.sequence;
          yield event;
        }
      }
    },
    async ack() {},
    async close() {},
  };
  const realtime = {
    async publish(event) {
      return event;
    },
    subscribe(options, handler) {
      assert.equal(options.catchUp, false);
      realtimeHandler = handler;
      return () => {};
    },
    async replay() {
      return { events: [] };
    },
    async *catchUp() {},
    async ack() {},
    async close() {},
  };

  const transport = new HybridTransport({ storage, realtime });
  const received = [];

  transport.subscribe(
    {
      room: 'bank:transactions',
      subscriberId: 'worker-1',
      catchUp: { batchSize: 2 },
    },
    (event) => {
      received.push(event.sequence);
    },
  );

  await waitFor(() => received.length === 3);
  realtimeHandler(createEvent(4));
  await waitFor(() => received.length === 4);

  assert.deepEqual(received, [1, 2, 3, 4]);
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
