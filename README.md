# @savanapoint/zero-pub-sub

Realtime Engine for application events: publish once, deliver over WebSocket,
replay missed events, track acknowledgements, and keep provider details out of
frontend apps.

It is domain-agnostic. The library does not know what an order, chat, driver,
vendor or payment is. Your application owns the room names, event types,
payloads and authorization rules.

## Contents

- [Install](#install)
- [Quick Start](#quick-start)
- [Mental Model](#mental-model)
- [Backend API](#backend-api)
- [WebSocket Gateway](#websocket-gateway)
- [Frontend Client](#frontend-client)
- [Providers](#providers)
- [Hybrid Mode](#hybrid-mode)
- [Advanced Features](#advanced-features)
- [Security](#security)
- [Production Checklist](#production-checklist)
- [Troubleshooting](#troubleshooting)
- [Validation](#validation)

## Install

Install the library:

```bash
npm install @savanapoint/zero-pub-sub
```

Database drivers for MongoDB, PostgreSQL and Redis are bundled with the server
entrypoint so URI setup works out of the box. Install `ws` only when running the
WebSocket Gateway in Node/Bun:

```bash
npm install ws
```

The `/client` entrypoint remains browser-safe and does not import the server
factory or database transports.

Database URLs and database clients must stay on the backend. Frontend and mobile
apps should connect only to your WebSocket Gateway.

## Entry Points

Use explicit entrypoints in application code:

```ts
// Backend only: database connections, transports and WebSocket Gateway.
import { connectRealtime, createWebSocketGateway } from '@savanapoint/zero-pub-sub/server';

// Frontend/mobile only: WebSocket client, room helpers and shared event types.
import { createRealtimeClient, room } from '@savanapoint/zero-pub-sub/client';
```

The package root remains available for backwards compatibility, but new code
should prefer `/server` and `/client`. This keeps frontend bundles away from
backend-only APIs and makes accidental database usage in client apps obvious.

## Quick Start

### MongoDB

```ts
import { connectRealtime } from '@savanapoint/zero-pub-sub/server';
import { room } from '@savanapoint/zero-pub-sub/client';

const realtime = await connectRealtime({
  provider: 'mongo',
  connection: process.env.MONGO_URL!,
});

await realtime.publish({
  room: room('chat', '123'),
  type: 'message.created',
  payload: {
    id: 'msg_1',
    text: 'Hello',
    senderId: 'user_1',
  },
});
```

### PostgreSQL

```ts
import { connectRealtime } from '@savanapoint/zero-pub-sub/server';

const realtime = await connectRealtime({
  provider: 'postgres',
  connection: process.env.DATABASE_URL!,
});
```

### Redis

```ts
import { connectRealtime } from '@savanapoint/zero-pub-sub/server';

const realtime = await connectRealtime({
  provider: 'redis',
  connection: process.env.REDIS_URL!,
});
```

## Advanced Features

### Snapshots

Use snapshots when a room has a large event history and clients should start
from consolidated state instead of replaying everything from sequence `0`.

```ts
await realtime.snapshot('chat:123', {
  lastSequence: 100000,
  state: {
    messages: recentMessages,
    participants,
  },
});
```

On the backend:

```ts
const snapshot = await realtime.getSnapshot('chat:123');

for await (const event of realtime.streamReplay({
  room: 'chat:123',
  fromSequence: snapshot?.lastSequence ?? 0,
  batchSize: 1000,
})) {
  await applyIncrementalEvent(event);
}
```

On the frontend:

```ts
client.subscribe(
  'chat:123',
  (event) => applyIncrementalEvent(event),
  {
    sync: 'snapshot',
    onSnapshot(snapshot) {
      hydrateRoom(snapshot?.state);
    },
  },
);
```

Flow:

```text
snapshot -> replay after snapshot.lastSequence -> live stream
```

Mongo URI setup creates durable `realtime_snapshots` and `realtime_dlq`
collections automatically. Other providers expose the same API through the
shared transport layer.

### Retry and DLQ

Retries are configured per subscription:

```ts
realtime.subscribe(
  {
    room: 'chat:123',
    retry: {
      attempts: 5,
      strategy: 'exponential',
      baseDelayMs: 100,
    },
  },
  async (event) => {
    await handleEvent(event);
  },
);
```

If the handler keeps failing after all attempts, the event is moved to DLQ:

```ts
const failed = await realtime.dlq('chat:123').list({ limit: 100 });

for await (const event of realtime.dlq('chat:123').replay({
  limit: 100,
})) {
  await repair(event);
}
```

Supported retry strategies:

```ts
'fixed' | 'linear' | 'exponential'
```

### Flow Control

Use flow control when consumers can be slower than producers:

```ts
realtime.subscribe(
  {
    room: 'chat:123',
    flowControl: {
      maxInFlight: 100,
      strategy: 'buffer',
      maxBufferSize: 1000,
    },
  },
  async (event) => {
    await handleEvent(event);
  },
);
```

Strategies:

- `pause`: rejects delivery when the handler backlog is full.
- `drop`: drops events when the listener is saturated.
- `buffer`: buffers up to `maxBufferSize`.

### Streaming Replay

`streamReplay()` is an alias for paginated catch-up and is the API to use for
large operational drains:

```ts
for await (const event of realtime.streamReplay({
  room: 'chat:123',
  fromSequence: 0,
  batchSize: 1000,
})) {
  await processEvent(event);
}
```

It does not load the full backlog into memory.

### Ephemeral Events

Ephemeral events are fanout-only. They are not persisted and do not participate
in replay or ack.

```ts
client.publishEphemeral({
  room: 'chat:123',
  type: 'typing',
  payload: {
    userId: 'u1',
  },
});

client.subscribeEphemeral('chat:123', (event) => {
  showTyping(event.payload);
}, {
  eventTypes: ['typing'],
});
```

Use ephemeral events for typing indicators, cursor movement and transient UI
state.

### Presence

Presence is built on ephemeral events:

```ts
const presence = client.presence('chat:123');

presence.enter({
  userId: 'u1',
  metadata: {
    name: 'Francisco',
  },
});

presence.subscribe((users) => {
  console.log(users);
});

presence.leave();
```

Presence users have this shape:

```ts
type PresenceUser = {
  userId: string;
  metadata?: Record<string, unknown>;
  lastSeenAt: string;
};
```

### Multi-Room Subscribe

Frontend clients can subscribe to multiple rooms with one call:

```ts
client.subscribe(['chat:1', 'chat:2'], (event) => {
  console.log(event.room, event.payload);
});
```

Wildcard room subscriptions support scoped patterns:

```ts
client.subscribe('chat:*', (event) => {
  console.log(event.room, event.payload);
});
```

Rules:

- Only `scope:*` patterns are supported.
- Global `*` is not supported.
- Auth still runs against the wildcard room string.
- Backend code should only allow wildcard scopes that are safe for that user or tenant.

### E2EE Payloads

The library supports encrypted payload markers. Encryption itself should happen
in your client or application layer before publishing:

```ts
client.publish({
  room: 'chat:123',
  type: 'message.created',
  encrypted: true,
  payload: encryptedPayload,
});
```

The server stores and routes the payload without needing to understand it.

### Middleware

Middleware lets backend code intercept, modify, validate, block or observe
realtime lifecycle operations. It runs server-side only.

Configure middleware at construction time:

```ts
const realtime = await connectRealtime({
  provider: 'postgres',
  connection: process.env.DATABASE_URL!,
  middleware: [
    async (ctx, next) => {
      console.log('before', ctx.action);
      await next();
      console.log('after', ctx.action);
    },
  ],
});
```

Or register later:

```ts
realtime.use(async (ctx, next) => {
  if (ctx.action === 'publish' && ctx.event) {
    ctx.event.metadata = {
      ...ctx.event.metadata,
      traceId: crypto.randomUUID(),
    };
  }

  await next();
});
```

Execution order:

```text
operation -> middleware 1 -> middleware 2 -> core -> middleware 2 -> middleware 1
```

Supported actions include:

```ts
'publish'
'subscribe'
'deliver'
'replay'
'catchUp'
'ack'
'snapshot'
'getSnapshot'
'ephemeralPublish'
'ephemeralDeliver'
'presenceEnter'
'presenceLeave'
'dlqWrite'
'dlqReplay'
'close'
```

Example: block large payloads:

```ts
realtime.use(async (ctx, next) => {
  if (ctx.action === 'publish') {
    const size = Buffer.byteLength(JSON.stringify(ctx.event?.payload));
    if (size > 64 * 1024) {
      throw new Error('Payload too large');
    }
  }

  await next();
});
```

Official helpers are available from `/server`:

```ts
import {
  loggerMiddleware,
  metricsMiddleware,
  payloadSizeMiddleware,
  tenantMiddleware,
} from '@savanapoint/zero-pub-sub/server';
```

### Metrics

`snapshotMetrics()` includes advanced counters:

```ts
{
  retryCount: 3,
  dlqSize: 1,
  snapshotUsage: 4,
  replayLatencyMs: 12,
}
```

## Recipes

### Chat

Use durable messages plus ephemeral typing and presence:

```ts
await realtime.publish({
  room: 'chat:123',
  type: 'message.created',
  payload: { text: 'hello', senderId: 'u1' },
});

client.publishEphemeral({
  room: 'chat:123',
  type: 'typing',
  payload: { userId: 'u1' },
});

client.presence('chat:123').enter({
  userId: 'u1',
  metadata: { name: 'Francisco' },
});
```

### Notifications

Use one room per user:

```ts
await realtime.publish({
  room: 'notifications:user_123',
  type: 'notification.created',
  payload: { title: 'Payment received' },
});
```

### Banking Events

Use idempotent event IDs, ack after processing and DLQ for failed consumers:

```ts
await realtime.publish({
  id: 'txn_9843A9XK21:created',
  room: 'bank:transactions',
  type: 'transaction.created',
  payload: transaction,
});
```

### Workers

Use `streamReplay()` for large recovery jobs:

```ts
for await (const event of realtime.streamReplay({
  room: 'bank:transactions',
  fromSequence: 0,
  batchSize: 1000,
})) {
  await processEvent(event);
  await realtime.ack(event.room, event.sequence, 'settlement-worker-1');
}
```

### Multi-Tenant

Put the tenant in metadata and enforce it with middleware:

```ts
realtime.use(async (ctx, next) => {
  if (ctx.action === 'publish' && !ctx.event?.metadata?.tenantId) {
    throw new Error('Missing tenantId');
  }

  await next();
});
```

### Load Testing

Run the default in-memory load test:

```bash
npm run load:test
```

Run a 1M-event profile:

```bash
REALTIME_LOAD_ROOMS=100 \
REALTIME_LOAD_SUBSCRIBERS_PER_ROOM=1 \
REALTIME_LOAD_TARGET_EVENTS=1000000 \
npm run load:test
```

### Subscribe on the Backend

```ts
const unsubscribe = realtime.subscribe(
  {
    room: 'chat:123',
    subscriberId: 'worker-1',
  },
  async (event) => {
    console.log(event.sequence, event.type, event.payload);
    await realtime.ack(event.room, event.sequence, 'worker-1');
  },
);
```

When `subscriberId` is provided, the transport reads the last acked sequence for
that subscriber and replays pending events in pages before continuing with live
events. Use `fromSequence` only when you want to override that cursor manually.

For large backlogs, tune the page size instead of loading everything at once:

```ts
realtime.subscribe(
  {
    room: 'chat:123',
    subscriberId: 'worker-1',
    catchUp: {
      batchSize: 1000,
    },
  },
  async (event) => {
    await processEvent(event);
    await realtime.ack(event.room, event.sequence, 'worker-1');
  },
);
```

### Close Connections

```ts
await unsubscribe();
await realtime.close();
```

When you use `connectRealtime`, the library owns the clients it creates and
closes them from `realtime.close()` by default.

## Mental Model

### Rooms

A room is the delivery key. It always uses:

```text
scope:resourceId
```

Examples:

```ts
import { buildRoom, room } from '@savanapoint/zero-pub-sub/client';

room('chat', '123');         // chat:123
room('order', '456');        // order:456
buildRoom('payment', '789'); // payment:789
```

The scope must match:

```text
[A-Za-z0-9][A-Za-z0-9._-]{0,63}
```

The resource id can contain additional `:` characters, but it cannot be empty.

### Events

An event is an append-only realtime message.

```ts
await realtime.publish({
  room: 'order:order_123',
  type: 'order.status.changed',
  payload: {
    status: 'ready',
  },
});
```

The stored envelope has this shape:

```ts
type RealtimeEnvelope<TPayload = unknown> = {
  id: string;
  type: string;
  room: string;
  sequence: number;
  emittedAt: string;
  expiresAt: string;
  payload: TPayload;
  metadata?: {
    provider?: 'postgres' | 'mongo' | 'redis';
    producer?: string;
    correlationId?: string;
    traceId?: string;
    tenantId?: string;
    [key: string]: unknown;
  };
};
```

### Sequences

Sequences are per room. A client receiving `chat:123` expects:

```text
1, 2, 3, 4...
```

If it sees a gap, the frontend client asks the gateway for replay.

### Replay

Replay fetches old events for a room:

```ts
const { events } = await realtime.replay({
  room: 'chat:123',
  fromSequence: 25,
  limit: 100,
});
```

Only events with `sequence > fromSequence` are returned.

Replay is a single page. Use `catchUp()` when you want to drain a large backlog
page by page.

```ts
for await (const event of realtime.catchUp({
  room: 'chat:123',
  subscriberId: 'worker-1',
  batchSize: 1000,
})) {
  await processEvent(event);
  await realtime.ack(event.room, event.sequence, 'worker-1');
}
```

### Ack

Ack records the last processed sequence per subscriber and room:

```ts
await realtime.ack('chat:123', 26, 'device-1');
```

## Backend API

### `connectRealtime(options)`

Use this for the best developer experience. It accepts connection strings,
creates provider clients internally, applies sensible defaults, and owns client
lifecycle.

```ts
const realtime = await connectRealtime({
  provider: 'mongo',
  connection: process.env.MONGO_URL!,
});
```

Supported providers:

```ts
type Provider = 'mongo' | 'postgres' | 'redis' | 'hybrid';
```

### `createRealtime(options)`

Use this when your app already owns connected database clients.

```ts
const realtime = createRealtime({
  provider: 'postgres',
  connection: {
    client: pgPool,
    listenerClient: pgListenerClient,
  },
});
```

### `publish(event)`

```ts
const envelope = await realtime.publish({
  id: 'evt_optional_idempotency_key',
  room: 'chat:123',
  type: 'message.created',
  ttlMs: 24 * 60 * 60 * 1000,
  payload: {
    text: 'Hello',
  },
  metadata: {
    producer: 'api',
    correlationId: 'req_123',
  },
});
```

If `id` is provided, transports use it as an idempotency key where supported.

### `subscribe(options, handler)`

```ts
const unsubscribe = realtime.subscribe(
  {
    room: 'chat:123',
    subscriberId: 'worker-1',
    eventTypes: ['message.created'],
    limit: 100,
  },
  async (event) => {
    console.log(event.payload);
  },
);
```

Subscribe catch-up behavior:

- If `fromSequence` is provided, replay starts after that sequence.
- If `fromSequence` is omitted and `subscriberId` is provided, replay starts
  after the subscriber's last acked sequence.
- If neither is available, replay starts after sequence `0`.
- Catch-up is paginated. `limit` is treated as the default page size for
  backwards compatibility.
- Use `catchUp: { batchSize: 1000 }` to control page size.
- Use `catchUp: { maxBatches: 10 }` to cap one subscription's startup work.
- Use `catchUp: false` for live-only subscriptions.
- After catch-up, the same subscription continues receiving live events.

For worker-style consumers that must process a very large backlog before going
live, prefer the explicit iterator:

```ts
for await (const event of realtime.catchUp({
  room: 'bank:transactions',
  subscriberId: 'settlement-worker-1',
  eventTypes: ['transaction.created'],
  batchSize: 1000,
})) {
  await settleTransaction(event.payload);
  await realtime.ack(event.room, event.sequence, 'settlement-worker-1');
}
```

`catchUp()` never stores the full backlog in memory. It calls `replay()` with a
bounded `limit`, advances the cursor to the last delivered sequence, yields to
the event loop between pages, and continues until there are no more events.

### `replay(options)`

```ts
const result = await realtime.replay({
  room: 'chat:123',
  fromSequence: 100,
  toSequence: 150,
  eventTypes: ['message.created'],
  limit: 100,
});
```

### `catchUp(options)`

```ts
for await (const event of realtime.catchUp({
  room: 'chat:123',
  subscriberId: 'worker-1',
  fromSequence: 100,
  toSequence: 10000,
  eventTypes: ['message.created'],
  batchSize: 1000,
  maxBatches: 100,
})) {
  await handle(event);
}
```

Options:

- `subscriberId`: uses the stored ack cursor when `fromSequence` is omitted.
- `fromSequence`: overrides the stored cursor.
- `toSequence`: stops after this sequence.
- `eventTypes`: filters by event type.
- `batchSize`: page size; defaults to `500` and is capped at `10000`.
- `maxBatches`: optional circuit breaker for a single drain run.

This is the recommended API for backfills, worker recovery and operational
jobs where millions of messages may be pending.

### `ack(room, sequence, subscriberId)`

```ts
await realtime.ack('chat:123', 101, 'device-1');
```

### `health()`

```ts
const health = await realtime.health?.();

console.log(health);
```

Example response:

```ts
{
  provider: 'postgres',
  status: 'healthy',
  details: {
    listenerReady: true,
    activeRooms: 2,
    activeListeners: 5,
  },
}
```

### `snapshotMetrics()`

```ts
const metrics = realtime.snapshotMetrics?.();
```

Metrics include:

```ts
{
  published: 10,
  received: 10,
  acked: 8,
  gapsDetected: 0,
  errors: 0,
  replayed: 2,
  duplicatesDropped: 1,
  activeRooms: 1,
  activeListeners: 3,
  averageDeliveryLagMs: 12,
}
```

### `close()`

```ts
await realtime.close();
```

Always call this during graceful shutdown.

## WebSocket Gateway

The gateway is the recommended frontend entrypoint. It keeps provider clients,
database credentials and direct database listeners out of frontend apps.

```ts
import {
  connectRealtime,
  createWebSocketGateway,
} from '@savanapoint/zero-pub-sub/server';

const transport = await connectRealtime({
  provider: 'postgres',
  connection: process.env.DATABASE_URL!,
});

const { gateway, server } = createWebSocketGateway({
  port: 8080,
  transport,
  limits: {
    maxConnections: 10000,
    maxSubscriptions: 128,
    maxPayloadBytes: 64 * 1024,
    maxMessagesPerWindow: 100,
    rateLimitWindowMs: 1000,
    initTimeoutMs: 10000,
    heartbeatIntervalMs: 30000,
    heartbeatTimeoutMs: 10000,
    maxSocketBufferedBytes: 1024 * 1024,
    allowClientPublish: false,
  },
  auth: {
    scopes: ['chat', 'order'],
    async jwt(token) {
      return verifyJwt(token);
    },
    async authorize(ctx) {
      return canAccessRoom(ctx.claims, ctx.room, ctx.action);
    },
  },
});
```

`server` is `null` when the optional `ws` package is not installed and no
custom `WebSocketServer` constructor is passed.

### Attach to an Existing HTTP Server

```ts
import http from 'node:http';
import { createWebSocketGateway } from '@savanapoint/zero-pub-sub/server';

const httpServer = http.createServer(app);

createWebSocketGateway({
  server: httpServer,
  transport,
});

httpServer.listen(8080);
```

### Gateway Health

```ts
const health = await gateway.health();
```

Example:

```ts
{
  status: 'healthy',
  connections: 42,
  transport: {
    provider: 'redis',
    status: 'healthy',
  },
  metrics: {
    published: 120,
    received: 118,
  },
}
```

### Gateway Shutdown

```ts
await gateway.close();
```

This closes active WebSocket connections and then closes the transport.

### One-Call Gateway

Use `serveRealtime()` when you want the library to own provider connection and
WebSocket gateway setup in one call:

```ts
import { serveRealtime } from '@savanapoint/zero-pub-sub/server';

const app = await serveRealtime({
  provider: 'redis',
  connection: process.env.REDIS_URL!,
  websocket: {
    port: 8080,
  },
});

process.on('SIGTERM', () => {
  void app.close();
});
```

The application still runs the process, but it does not implement WebSocket
protocol, fanout, replay, presence or ack handling.

### CLI Gateway

Run a ready-made gateway without writing a server file:

```bash
npx zero-pub-sub gateway \
  --provider mongo \
  --connection "$MONGO_URL" \
  --port 8080
```

Environment-variable form:

```bash
ZERO_PUBSUB_PROVIDER=redis \
ZERO_PUBSUB_CONNECTION="$REDIS_URL" \
PORT=8080 \
npx zero-pub-sub gateway
```

### Hosted Cloud Mode

For hosted/SaaS deployments, use the cloud client contract:

```ts
import { createRealtimeCloudClient } from '@savanapoint/zero-pub-sub/client';

const client = createRealtimeCloudClient({
  appId: 'app_123',
  apiKey: 'public_key',
  subscriberId: 'device-1',
});
```

Backend publishing through a compatible cloud endpoint:

```ts
import { connectRealtimeCloud } from '@savanapoint/zero-pub-sub/server';

const realtime = connectRealtimeCloud({
  endpoint: 'https://realtime.example.com',
  secretKey: process.env.ZERO_SECRET_KEY!,
});
```

### Wire Protocol

Client to server:

```json
{ "type": "init", "subscriberId": "device-1", "token": "jwt" }
{ "type": "subscribe", "room": "chat:123", "eventTypes": ["message.created"], "fromSequence": 0 }
{ "type": "unsubscribe", "room": "chat:123" }
{ "type": "publish", "event": { "type": "message.created", "room": "chat:123", "payload": { "text": "Hello" } } }
{ "type": "ack", "room": "chat:123", "sequence": 10 }
{ "type": "replay_request", "room": "chat:123", "fromSequence": 10, "limit": 100 }
```

Server to client:

```json
{ "type": "ready", "subscriberId": "device-1" }
{ "type": "subscribed", "room": "chat:123" }
{ "type": "event", "data": { "id": "evt_1", "room": "chat:123", "sequence": 1, "type": "message.created", "payload": {} } }
{ "type": "replay", "room": "chat:123", "events": [] }
{ "type": "ack", "room": "chat:123", "sequence": 1 }
{ "type": "unsubscribed", "room": "chat:123" }
{ "type": "error", "message": "Access denied for room: chat:123", "code": "4408" }
```

## Frontend Client

Use `createRealtimeClient` from frontend or mobile code. Do not pass database
URLs to frontend apps.

```ts
import {
  createRealtimeClient,
  room,
} from '@savanapoint/zero-pub-sub/client';

const client = createRealtimeClient({
  url: 'wss://realtime.example.com',
  subscriberId: 'device-1',
  authToken: jwt,
  reconnect: true,
  reconnectDelayMs: 1000,
  onError(error) {
    console.error(error);
  },
});

client.connect();

const unsubscribe = client.subscribe(
  room('chat', '123'),
  (event) => {
    console.log(event.sequence, event.payload);
  },
  {
    eventTypes: ['message.created'],
    fromSequence: 0,
  },
);
```

### Publish from the Frontend

Frontend publishing is supported by the protocol, but production apps usually
publish through authenticated HTTP mutations and let the backend emit realtime
events after committing business state.

```ts
client.publish({
  room: 'chat:123',
  type: 'message.created',
  payload: {
    text: 'Hello',
  },
});
```

### Reconnect and Replay

The client:

- queues outbound messages until the socket opens
- reconnects by default
- tracks the last sequence per room
- buffers out-of-order events
- requests replay when it detects a sequence gap
- sends `ack` after flushing an event

Useful client options:

```ts
createRealtimeClient({
  url: 'wss://realtime.example.com',
  subscriberId: 'device-1',
  reconnect: true,
  reconnectDelayMs: 1000,
  maxBufferedEventsPerRoom: 1000,
  replayThrottleMs: 500,
  maxQueuedMessages: 1000,
  onError(error) {
    reportError(error);
  },
});
```

## Providers

### MongoDB

Best for durable replay when your app already uses MongoDB.

URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'mongo',
  connection: process.env.MONGO_URL!,
});
```

Advanced URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'mongo',
  connection: {
    uri: process.env.MONGO_URL!,
    database: 'zero_realtime',
    collectionPrefix: 'realtime',
    eventsCollection: 'realtime_events',
    countersCollection: 'realtime_counters',
    subscribersCollection: 'realtime_subscribers',
    autoCreateIndexes: true,
    closeClient: true,
  },
});
```

Default collections:

```text
realtime_events
realtime_counters
realtime_subscribers
```

Default indexes:

```text
events:      { room: 1, sequence: 1 } unique
events:      { id: 1 } unique
events:      { expiresAt: 1 }
subscribers: { room: 1, subscriberId: 1 } unique
```

Inject existing collections:

```ts
import { MongoClient } from 'mongodb';
import { createRealtime } from '@savanapoint/zero-pub-sub/server';

const mongo = new MongoClient(process.env.MONGO_URL!);
await mongo.connect();

const db = mongo.db('zero_realtime');

const realtime = createRealtime({
  provider: 'mongo',
  connection: {
    events: db.collection('realtime_events'),
    counters: db.collection('realtime_counters'),
    subscribers: db.collection('realtime_subscribers'),
  },
});
```

MongoDB change streams are used when available. Change streams require a replica
set or a managed MongoDB deployment that supports them. Local fanout still works
inside the current process without change streams.

### PostgreSQL

Best for durable replay and strict per-room ordering.

URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'postgres',
  connection: process.env.DATABASE_URL!,
});
```

Advanced URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'postgres',
  connection: {
    uri: process.env.DATABASE_URL!,
    ssl: process.env.NODE_ENV === 'production'
      ? { rejectUnauthorized: false }
      : false,
    autoMigrate: true,
    eventsTable: 'realtime_events',
    subscribersTable: 'realtime_subscribers',
    notifyChannel: 'realtime_events',
    closeClients: true,
  },
});
```

`connectRealtime` creates:

- one `pg.Pool` for queries and writes
- one dedicated `pg.Client` for `LISTEN/NOTIFY`
- default tables and indexes when `autoMigrate` is true

`autoMigrate` defaults to `true` outside production and `false` when
`NODE_ENV=production`. Set it explicitly in production so schema changes are
part of your deploy process.

Inject existing clients:

```ts
import { Pool, Client } from 'pg';
import { createRealtime } from '@savanapoint/zero-pub-sub/server';

const client = new Pool({ connectionString: process.env.DATABASE_URL });
const listenerClient = new Client({ connectionString: process.env.DATABASE_URL });

await listenerClient.connect();

const realtime = createRealtime({
  provider: 'postgres',
  connection: {
    client,
    listenerClient,
  },
});
```

Manual schema:

```bash
psql "$DATABASE_URL" -f migrations/001_postgres_initial.sql
psql "$DATABASE_URL" -f migrations/002_postgres_prune_function.sql
```

Retention cleanup:

```sql
SELECT realtime_prune_expired_events();
```

Run that periodically from a cron job, scheduled worker or database scheduler.

### Redis

Best for low-latency fanout and high-volume realtime delivery.

URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'redis',
  connection: process.env.REDIS_URL!,
});
```

Advanced URI setup:

```ts
const realtime = await connectRealtime({
  provider: 'redis',
  connection: {
    uri: process.env.REDIS_URL!,
    streamPrefix: 'realtime:stream:',
    channelPrefix: 'realtime:room:',
    subscriberPrefix: 'realtime:subscriber:',
    sequencePrefix: 'realtime:sequence:',
    idempotencyPrefix: 'realtime:idempotency:',
    sequenceIndexPrefix: 'realtime:sequence-index:',
    persistStreams: true,
    closeClients: true,
  },
});
```

`connectRealtime` creates:

- one primary Redis client
- one duplicated subscriber client

Inject existing clients:

```ts
import { createClient } from 'redis';
import { createRealtime } from '@savanapoint/zero-pub-sub/server';

const client = createClient({ url: process.env.REDIS_URL });
const subscriberClient = client.duplicate();

await client.connect();
await subscriberClient.connect();

const realtime = createRealtime({
  provider: 'redis',
  connection: {
    client,
    subscriberClient,
  },
});
```

Redis uses:

```text
INCR    per-room sequence
XADD    replay stream
XRANGE  replay reads
PUBLISH realtime fanout
SUBSCRIBE realtime listeners
HSET    subscriber ack cursor
SET     idempotency cache and sequence-to-stream index
```

Set `persistStreams: false` when Redis is used only for fanout and another
provider owns durable replay. Hybrid mode does this automatically for Redis
URI connections used as the realtime leg.

## Hybrid Mode

Hybrid mode writes to a durable storage provider and delivers realtime events
through another provider.

Recommended production shape:

```ts
const realtime = await connectRealtime({
  provider: 'hybrid',
  storage: {
    provider: 'postgres',
    connection: process.env.DATABASE_URL!,
  },
  realtime: {
    provider: 'redis',
    connection: process.env.REDIS_URL!,
  },
});
```

Publish goes to storage first, then realtime fanout:

```text
publish -> PostgreSQL -> Redis -> WebSocket subscribers
```

Replay reads from storage:

```text
replay -> PostgreSQL
```

Catch-up also drains from storage, while live events continue through the
realtime leg. This prevents Redis fanout streams from becoming the long-term
source of truth when storage is PostgreSQL or MongoDB.

This is the strongest default for many production apps: PostgreSQL or MongoDB
for durable replay, Redis for low-latency fanout.

## Security

### Keep Database Credentials Server-Side

Never expose `DATABASE_URL`, `MONGO_URL` or `REDIS_URL` to frontend or mobile
apps. The browser should only know your WebSocket endpoint.

### Authenticate Connections

```ts
createWebSocketGateway({
  transport,
  auth: {
    async jwt(token) {
      return verifyJwt(token);
    },
  },
});
```

If `auth.jwt` is configured, the client must send:

```json
{ "type": "init", "subscriberId": "device-1", "token": "jwt" }
```

### Restrict Room Scopes

```ts
createWebSocketGateway({
  transport,
  auth: {
    scopes: ['chat', 'order'],
  },
});
```

This rejects rooms such as `admin:settings` unless `admin` is in the scope list.

### Authorize Per Room

```ts
createWebSocketGateway({
  transport,
  auth: {
    async authorize(ctx) {
      const { subscriberId, claims, room, action } = ctx;
      return canAccessRoom({ subscriberId, claims, room, action });
    },
  },
});
```

Actions are:

```ts
'subscribe' | 'publish' | 'replay' | 'ack'
```

## Production Checklist

- Use the WebSocket Gateway as the only frontend realtime entrypoint.
- Keep database connection strings in backend environment variables.
- Prefer `connectRealtime` for app-owned realtime infrastructure.
- Use `createRealtime` only when your app already owns provider clients.
- Use MongoDB or PostgreSQL for durable replay.
- Use Redis for high-volume fanout.
- Use Hybrid mode when you need both durable replay and low latency.
- Tune `catchUp.batchSize` for workers that may recover large backlogs.
- Ack only after successful processing so offline subscribers can resume safely.
- Configure gateway `auth.jwt`, `auth.scopes` and `auth.authorize`.
- Set gateway limits for payload size, connection count, subscription count and rate limits.
- Call `health()` in readiness checks.
- Export `snapshotMetrics()` to your monitoring system.
- Call `close()` during graceful shutdown.
- Prune expired events for durable providers.
- Keep Redis subscriber clients separate from write clients.
- Use a dedicated PostgreSQL listener client for `LISTEN/NOTIFY`.

## Troubleshooting

### `Missing bundled dependency "pg"`

Reinstall the package so its bundled database drivers are restored:

```bash
npm install @savanapoint/zero-pub-sub
```

The same applies to `mongodb` and `redis`. The `ws` package is still optional
and only needed when you run the WebSocket Gateway in Node/Bun.

### `WebSocket implementation is required`

In Node.js clients, pass a WebSocket implementation:

```ts
import WebSocket from 'ws';

const client = createRealtimeClient({
  url: 'ws://localhost:8080',
  subscriberId: 'device-1',
  WebSocket,
});
```

Browsers already provide `globalThis.WebSocket`.

### `Realtime connection must send init first`

The first protocol message must be:

```json
{ "type": "init", "subscriberId": "device-1", "token": "jwt" }
```

`createRealtimeClient` sends this automatically on open.

### No Events Across Multiple Backend Instances

Check provider-specific fanout:

- PostgreSQL needs a dedicated connected `listenerClient`.
- Redis needs a connected `subscriberClient`.
- MongoDB cross-process fanout needs change streams.
- Hybrid mode with Redis is recommended for multi-instance fanout.

### MongoDB Change Streams Do Not Fire Locally

MongoDB change streams require replica set support. Use a local replica set,
MongoDB Atlas, or Hybrid mode with Redis for fanout.

### PostgreSQL Tables Are Missing

Use URI setup with `autoMigrate: true`, or run:

```bash
psql "$DATABASE_URL" -f migrations/001_postgres_initial.sql
psql "$DATABASE_URL" -f migrations/002_postgres_prune_function.sql
```

### Redis Replay Returns No Events

Replay depends on Redis Streams. Make sure your client supports `xAdd` and
`xRange`, and that events were published through the Redis transport.

### Frontend Receives Sequence Gaps

The client automatically requests replay. If gaps persist, check:

- retention windows
- transport `limit`
- provider health
- event pruning
- whether multiple publishers are using the same room sequence provider

## Validation

Build:

```bash
npm run build
```

Test:

```bash
npm test
```

Package check:

```bash
npm run pack:check
```

If your local npm cache has permission issues, use a temporary cache:

```bash
npm --cache /private/tmp/zero-npm-cache pack --dry-run
```

Synthetic load test:

```bash
npm run load:test
```

Tune load test values:

```bash
REALTIME_LOAD_ROOMS=50 \
REALTIME_LOAD_SUBSCRIBERS_PER_ROOM=10 \
REALTIME_LOAD_EVENTS_PER_ROOM=100 \
npm run load:test
```

## License

MIT
