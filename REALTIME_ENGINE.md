# Realtime Engine

`@savanapoint/zero-pub-sub` now includes a domain-agnostic realtime event streaming engine.

## Core

- Generic rooms: `scope:resourceId`
- Helpers: `buildRoom`, `parseRoom`, `encodeRoom`, `decodeRoom`
- Envelope:

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

## Backend

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
    scopes: ['chat', 'order', 'payment'],
    async authorize(ctx) {
      return true;
    },
  },
});
```

## Hybrid Redis + Postgres

```ts
const realtime = createRealtime({
  provider: 'hybrid',
  storage: { provider: 'postgres', connection: { client: pgClient } },
  realtime: { provider: 'redis', connection: { client: redisClient } },
});
```

## Client

```ts
import { buildRoom, createRealtimeClient } from '@savanapoint/zero-pub-sub';

const client = createRealtimeClient({
  url: 'ws://localhost:8080',
  subscriberId: 'device-1',
  authToken: jwt,
});

client.connect();

client.subscribe(buildRoom('chat', '123'), (event) => {
  console.log(event.payload);
});
```

The client buffers out-of-order events, detects sequence gaps, requests replay automatically and flushes events in order.

## WebSocket Protocol

Client to server:

```json
{ "type": "init", "subscriberId": "user-1", "token": "jwt" }
{ "type": "subscribe", "room": "chat:123" }
{ "type": "publish", "event": { "type": "message", "room": "chat:123", "payload": {} } }
{ "type": "ack", "room": "chat:123", "sequence": 10 }
{ "type": "replay_request", "room": "chat:123", "fromSequence": 10 }
```

Server to client:

```json
{ "type": "ready", "subscriberId": "user-1" }
{ "type": "event", "data": {} }
{ "type": "replay", "room": "chat:123", "events": [] }
{ "type": "error", "message": "" }
```

