import type { FirebaseOptions } from 'firebase/app';
import { FirestoreFallbackTransport } from './transport/FirestoreFallbackTransport';
import type { Unsubscribe } from './transport/types';

/**
 * Backward-compatible facade for the original PubSub API.
 *
 * New integrations should prefer FirestoreFallbackTransport directly because it
 * exposes envelopes, rooms, cursors, replay, ack, metrics and connection status.
 */
class PubSub {
  private readonly transport: FirestoreFallbackTransport;

  constructor(firebaseConfig: FirebaseOptions, subscriberId = 'legacy-subscriber') {
    this.transport = new FirestoreFallbackTransport({
      firebaseConfig,
      subscriberId,
      app: 'client',
    });
  }

  async publish(channel: string, message: string, subscribers: string[]): Promise<void> {
    await Promise.all(
      subscribers.map((subscriber) =>
        this.transport.publish({
          room: `user:${subscriber}`,
          type: `${channel}:message`,
          entityId: channel,
          action: 'created',
          payload: {
            channel,
            message,
            subscriber,
          },
        }),
      ),
    );
  }

  subscribe(
    channel: string,
    subscriberIds: string[],
    onMessage: (message: string) => void | Promise<void>,
  ): Unsubscribe {
    const unsubscribers = subscriberIds.map((subscriberId) =>
      this.transport.subscribe<{ channel: string; message: string }>(
        {
          room: `user:${subscriberId}`,
          subscriberId,
          eventTypes: [`${channel}:message`],
          from: 'cursor',
        },
        async (event) => {
          await onMessage(event.payload.message);
        },
      ),
    );

    return async () => {
      await Promise.all(unsubscribers.map((unsubscribe) => Promise.resolve(unsubscribe())));
    };
  }
}

export default PubSub;
