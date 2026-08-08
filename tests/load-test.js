const { RedisTransport } = require('../dist/transports/redis');

const rooms = Number(process.env.REALTIME_LOAD_ROOMS ?? 50);
const subscribersPerRoom = Number(process.env.REALTIME_LOAD_SUBSCRIBERS_PER_ROOM ?? 10);
const eventsPerRoom = Number(process.env.REALTIME_LOAD_EVENTS_PER_ROOM ?? 100);
const targetEvents = Number(process.env.REALTIME_LOAD_TARGET_EVENTS ?? 0);
const effectiveEventsPerRoom = targetEvents > 0 ? Math.ceil(targetEvents / rooms) : eventsPerRoom;

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

async function main() {
  const transport = new RedisTransport({ client });
  let delivered = 0;
  const expected = rooms * subscribersPerRoom * effectiveEventsPerRoom;

  for (let roomIndex = 0; roomIndex < rooms; roomIndex += 1) {
    const room = `load:${roomIndex}`;
    for (let subscriberIndex = 0; subscriberIndex < subscribersPerRoom; subscriberIndex += 1) {
      transport.subscribe({ room }, () => {
        delivered += 1;
      });
    }
  }

  const startedAt = process.hrtime.bigint();
  for (let roomIndex = 0; roomIndex < rooms; roomIndex += 1) {
    const room = `load:${roomIndex}`;
    for (let eventIndex = 0; eventIndex < effectiveEventsPerRoom; eventIndex += 1) {
      await transport.publish({
        room,
        type: 'load.event',
        payload: { eventIndex },
      });
    }
  }
  await new Promise((resolve) => setImmediate(resolve));
  const elapsedMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;

  if (delivered !== expected) {
    throw new Error(`Expected ${expected} deliveries, got ${delivered}`);
  }

  const events = rooms * effectiveEventsPerRoom;
  const throughput = Math.round((events / elapsedMs) * 1000);
  console.log(JSON.stringify({
    rooms,
    subscribersPerRoom,
    eventsPerRoom: effectiveEventsPerRoom,
    events,
    delivered,
    elapsedMs: Math.round(elapsedMs),
    throughputEventsPerSecond: throughput,
    metrics: transport.snapshotMetrics(),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
