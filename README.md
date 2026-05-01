# @savanapoint/zero-pub-sub

Firestore-backed realtime fallback transport for the Zero ecosystem.

This package is designed to complement a primary WebSocket/Socket.IO realtime channel. It persists short-lived realtime events in Firestore so applications can recover missed events, keep operating when WebSocket is unavailable, and reconcile state without aggressive polling.

## Installation

```bash
npm install @savanapoint/zero-pub-sub firebase
```

`firebase` is a peer dependency. Applications that already initialize Firebase can pass an existing `Firestore` instance to the transport.

## Core Concepts

- Socket.IO remains the primary realtime provider.
- Firestore fallback uses the same event envelope as the primary realtime layer.
- Events are routed by rooms such as `user:{id}`, `vendor:{id}`, `chat:{id}` and `tracking:{topic}`.
- Each subscriber has its own cursor. Events are not marked as globally read.
- Events include `id`, `sequence`, `updatedAt`, `emittedAt` and `expiresAt` for dedupe, replay and TTL.

## Usage

### Create a Transport

```ts
import { FirestoreFallbackTransport } from '@savanapoint/zero-pub-sub';

const fallback = new FirestoreFallbackTransport({
  firebaseConfig,
  subscriberId: 'user_123:device_abc',
  app: 'client',
  defaultTtlMs: 24 * 60 * 60 * 1000,
  maxBacklogEvents: 500,
});
```

With an existing Firestore instance:

```ts
import { getFirestore } from 'firebase/firestore';
import { FirestoreFallbackTransport } from '@savanapoint/zero-pub-sub';

const fallback = new FirestoreFallbackTransport({
  firestore: getFirestore(app),
  subscriberId: 'vendor_123:device_abc',
  app: 'vendor',
});
```

### Publish an Event

Publishing is intended for trusted backend code.

```ts
await fallback.publish({
  room: 'user:123',
  type: 'user:order:update',
  entityId: 'order_456',
  action: 'updated',
  version: 12,
  updatedAt: new Date(),
  payload: {
    orderId: 'order_456',
    status: 'ready',
  },
});
```

### Subscribe to a Room

```ts
const unsubscribe = fallback.subscribe(
  {
    room: 'user:123',
    eventTypes: ['user:order:update', 'user:delivery:update'],
    from: 'cursor',
    ackMode: 'after-callback',
  },
  async (event) => {
    ordersStore.apply(event);
  },
  (error) => {
    console.error('Fallback realtime error:', error);
  },
);

await unsubscribe();
```

### Manual Ack

```ts
const unsubscribe = fallback.subscribeWithAck(
  {
    room: 'chat:conversation_123',
    eventTypes: ['chat:message', 'chat:read'],
    from: 'cursor',
    ackMode: 'manual',
  },
  async (event, ack, nack) => {
    try {
      await chatStore.apply(event);
      await ack();
    } catch (error) {
      await nack(error);
    }
  },
);
```

### Replay Missed Events

```ts
const replay = await fallback.replay({
  room: 'vendor:123',
  fromSequence: 42,
  limit: 100,
});

if (replay.resyncRequired) {
  await resyncFromHttp('sequence_gap');
} else {
  replay.events.forEach((event) => vendorOpsStore.apply(event));
}
```

### Rooms

```ts
import { buildRoom, room } from '@savanapoint/zero-pub-sub';

room('chat', 'conversation_123');      // chat:conversation_123
room('order', 'order_123');            // order:order_123
buildRoom('payment', 'payment_123');   // payment:payment_123
```

## Firestore Schema

```text
realtimeRooms/{encodedRoom}
  events/{eventId}
  subscribers/{subscriberId}
```

Each room stores `lastSequence`. Each subscriber stores `lastAckSequence` and `lastSeenSequence`.

## Legacy API

The original default export is still available as a compatibility facade:

```ts
import PubSub from '@savanapoint/zero-pub-sub';

const pubSub = new PubSub(firebaseConfig, 'user_123:device_abc');

const unsubscribe = pubSub.subscribe('newsletter', ['user_123'], (message) => {
  console.log(message);
});

await pubSub.publish('newsletter', 'Hello, World!', ['user_123']);
await unsubscribe();
```

New integrations should use `FirestoreFallbackTransport` directly.

## Production Notes

- Use the backend as the trusted publisher.
- Configure Firestore Security Rules so clients can only read authorized rooms.
- Allow clients to update only their own subscriber cursor documents.
- Configure Firestore TTL for `events.expiresAt`.
- Do not use fallback for high-frequency ephemeral events unless necessary.
- Deduplicate events in the app using `event.id` and `sequence` per room.

## License

MIT
