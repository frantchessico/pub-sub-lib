import { FirestoreFallbackTransport } from '../transport/FirestoreFallbackTransport';
import type { FirestoreFallbackTransportOptions } from '../transport/types';
import type {
  PublishInput,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeTransport,
  ReplayOptions,
  ReplayResult,
  SubscribeOptions,
  Unsubscribe,
} from '../core/types';

export class FirestoreTransport implements RealtimeTransport {
  private readonly transport: FirestoreFallbackTransport;

  constructor(options: FirestoreFallbackTransportOptions) {
    this.transport = new FirestoreFallbackTransport(options);
  }

  async publish<TPayload>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const envelope = await this.transport.publish(
      {
        room: event.room,
        type: event.type,
        action: 'updated',
        payload: event.payload,
        metadata: event.metadata as any,
      },
      {
        sequence: event.sequence,
        eventId: event.id,
      },
    );
    return envelope as unknown as RealtimeEnvelope<TPayload>;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    return this.transport.subscribe(
      {
        room: options.room,
        subscriberId: options.subscriberId,
        eventTypes: options.eventTypes,
        from: options.fromSequence !== undefined ? { sequence: options.fromSequence } : 'cursor',
        limit: options.limit,
      },
      handler as any,
    );
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const result = await this.transport.replay<TPayload>({
      room: options.room,
      fromSequence: options.fromSequence,
      toSequence: options.toSequence,
      eventTypes: options.eventTypes,
      limit: options.limit,
    });
    return { events: result.events as unknown as RealtimeEnvelope<TPayload>[] };
  }

  ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    return this.transport.ack(room, sequence, subscriberId);
  }

  close(): Promise<void> {
    return this.transport.close();
  }
}

