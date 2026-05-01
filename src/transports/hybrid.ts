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

export interface HybridTransportOptions {
  storage: RealtimeTransport;
  realtime: RealtimeTransport;
}

export class HybridTransport implements RealtimeTransport {
  constructor(private readonly options: HybridTransportOptions) {}

  async publish<TPayload>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const stored = await this.options.storage.publish(event);
    await this.options.realtime.publish(stored);
    return stored;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    return this.options.realtime.subscribe(options, handler);
  }

  replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    return this.options.storage.replay(options);
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await Promise.all([
      this.options.storage.ack(room, sequence, subscriberId),
      this.options.realtime.ack(room, sequence, subscriberId),
    ]);
  }

  async close(): Promise<void> {
    await Promise.all([this.options.storage.close(), this.options.realtime.close()]);
  }
}

