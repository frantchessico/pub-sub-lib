# @savanapoint/zero-pub-sub

Realtime Engine: a domain-agnostic event streaming system for apps.

It provides a consistent API for backend publishing, WebSocket delivery, replay, ack, ordering and frontend synchronization without exposing databases to the frontend.

## What It Is

- A lightweight Firebase Realtime Database / Ably alternative.
- A Kafka-like event log for application realtime workflows.
- A WebSocket gateway with replay and sequence guarantees.
- A multi-provider transport layer for PostgreSQL, MongoDB, Redis and Firestore.

## What It Is Not

- It is not coupled to Zero domains.
- It is not a Firebase-only library.
- It does not require frontend clients to know anything about SQL, MongoDB, Redis or Firestore.
- It does not expose database credentials or database listeners to frontend apps.

## Install

```bash
npm install @savanapoint/zero-pub-sub
```

Optional peer dependencies depend on the provider you use:

```bash
npm install ws
npm install firebase
```

Database clients are injected by your backend. The library does not force a specific PostgreSQL, MongoDB or Redis package.

## Core Concepts

### Rooms

Rooms are generic and always follow:

```text
scope:resourceId
```

Examples:

```ts
import { buildRoom, room } from '@savanapoint/zero-pub-sub';

room('chat', '123');         // chat:123
room('order', '456');        // order:456
buildRoom('payment', '789'); // payment:789
```

There are no built-in domain scopes such as `user`, `vendor`, `driver`, `chat` or `tracking`. Your application owns its scopes.

### Event Envelope

```ts
type RealtimeEnvelope<T> = {
  id: string;
  type: string;
  room: string;
  sequence: number;
  emittedAt: string;
  expiresAt: string;
  payload: T;
  metadata?: {
    provider?: 'postgres' | 'mongo' | 'redis' | 'firestore';
  };
};
```

## Backend Usage

### Create a Transport

```ts
import { createRealtime } from '@savanapoint/zero-pub-sub';

const realtime = createRealtime({
  provider: 'postgres',
  connection: {
    client: pgClient,
  },
});
```

### Publish

```ts
await realtime.publish({
  room: room('order', 'order_123'),
  type: 'status.changed',
  payload: {
    status: 'ready',
  },
});
```

### Replay

```ts
const { events } = await realtime.replay({
  room: room('order', 'order_123'),
  fromSequence: 10,
  limit: 100,
});
```

## WebSocket Gateway

```ts
import { createRealtime, createWebSocketGateway } from '@savanapoint/zero-pub-sub';

const transport = createRealtime({
  provider: 'postgres',
  connection: { client: pgClient },
});

const { gateway, server } = createWebSocketGateway({
  port: 8080,
  transport,
  auth: {
    scopes: ['order', 'chat', 'payment'],
    async authorize(ctx) {
      return true;
    },
  },
});
```

### Protocol

Client to server:

```json
{ "type": "init", "subscriberId": "device-1", "token": "jwt" }
{ "type": "subscribe", "room": "order:123" }
{ "type": "publish", "event": { "type": "status.changed", "room": "order:123", "payload": {} } }
{ "type": "ack", "room": "order:123", "sequence": 10 }
{ "type": "replay_request", "room": "order:123", "fromSequence": 10 }
```

Server to client:

```json
{ "type": "ready", "subscriberId": "device-1" }
{ "type": "event", "data": {} }
{ "type": "replay", "room": "order:123", "events": [] }
{ "type": "error", "message": "" }
```

## Frontend Client

```ts
import { createRealtimeClient, room } from '@savanapoint/zero-pub-sub';

const client = createRealtimeClient({
  url: 'wss://realtime.example.com',
  subscriberId: 'device-1',
  authToken: jwt,
  reconnect: true,
});

client.connect();

const unsubscribe = client.subscribe(room('order', '123'), (event) => {
  console.log(event.sequence, event.payload);
});
```

The client detects sequence gaps, buffers out-of-order events, requests replay automatically and flushes events in order.

## Providers

### PostgreSQL

Persistent event log, replay and per-room sequence.

```ts
createRealtime({
  provider: 'postgres',
  connection: { client: pgClient },
});
```

### MongoDB

Events collection, counters collection and optional change streams.

```ts
createRealtime({
  provider: 'mongo',
  connection: {
    events,
    counters,
    subscribers,
  },
});
```

### Redis

Redis Streams for replay and Pub/Sub for fast fan-out.

```ts
createRealtime({
  provider: 'redis',
  connection: { client: redisClient },
});
```

### Hybrid

Redis for realtime delivery, PostgreSQL for durable storage and replay.

```ts
createRealtime({
  provider: 'hybrid',
  storage: { provider: 'postgres', connection: { client: pgClient } },
  realtime: { provider: 'redis', connection: { client: redisClient } },
});
```

### Firestore

Firestore is available as a legacy/fallback provider, not the recommended production primary transport.

```ts
createRealtime({
  provider: 'firestore',
  connection: {
    firestore,
    subscriberId: 'backend-worker',
  },
});
```

## Security

- Validate JWT during `init`.
- Use `auth.scopes` to whitelist room scopes.
- Use `auth.authorize(ctx)` to enforce per-room access.
- Do not expose provider clients or database credentials to frontend apps.

## Production Guidance

- Prefer WebSocket Gateway as the only frontend realtime entrypoint.
- Prefer PostgreSQL or MongoDB for durable replay.
- Use Redis for high-volume fan-out.
- Use Firestore only as compatibility or emergency fallback.
- Keep room scopes domain-owned by your app, not by this library.

## License

MIT
