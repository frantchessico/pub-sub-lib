import { createRealtimeEnvelope } from '../core/envelope';
import type {
  PublishInput,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeStorageProvider,
  SubscribeOptions,
  Unsubscribe,
} from '../core/types';

export type ListenerEntry = {
  options: SubscribeOptions;
  handler: RealtimeEventHandler<any>;
};

export abstract class TransportBase {
  protected readonly listeners = new Map<string, Map<string, ListenerEntry>>();
  private listenerSequence = 0;

  protected createEnvelope<TPayload>(
    input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>,
    sequence: number,
    provider: RealtimeStorageProvider,
  ): RealtimeEnvelope<TPayload> {
    return createRealtimeEnvelope(input, sequence, provider);
  }

  protected fanout<TPayload>(event: RealtimeEnvelope<TPayload>): void {
    const roomListeners = this.listeners.get(event.room);
    if (!roomListeners) {
      return;
    }

    roomListeners.forEach((entry) => {
      if (entry.options.eventTypes?.length && !entry.options.eventTypes.includes(event.type)) {
        return;
      }

      void entry.handler(event);
    });
  }

  protected addListener<TPayload>(
    options: SubscribeOptions,
    handler: RealtimeEventHandler<TPayload>,
  ): Unsubscribe {
    const id = `listener_${++this.listenerSequence}`;
    const roomListeners = this.listeners.get(options.room) ?? new Map<string, ListenerEntry>();
    roomListeners.set(id, { options, handler });
    this.listeners.set(options.room, roomListeners);

    return () => {
      const current = this.listeners.get(options.room);
      current?.delete(id);
      if (current && current.size === 0) {
        this.listeners.delete(options.room);
      }
    };
  }

  protected clearListeners(): void {
    this.listeners.clear();
  }
}

