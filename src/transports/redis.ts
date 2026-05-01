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
import { TransportBase } from './base';

export interface RedisClientLike {
  xAdd?(key: string, id: string, message: Record<string, string>): Promise<string>;
  xRange?(key: string, start: string, end: string, options?: { COUNT?: number }): Promise<Array<{ id: string; message: Record<string, string> }>>;
  publish?(channel: string, message: string): Promise<number> | number;
  subscribe?(channel: string, handler: (message: string) => void): Promise<unknown> | unknown;
  unsubscribe?(channel: string): Promise<unknown> | unknown;
  hSet?(key: string, field: string, value: string): Promise<unknown>;
  quit?(): Promise<unknown>;
}

export interface RedisTransportOptions {
  client: RedisClientLike;
  streamPrefix?: string;
  channelPrefix?: string;
  subscriberPrefix?: string;
}

export class RedisTransport extends TransportBase implements RealtimeTransport {
  private readonly streamPrefix: string;
  private readonly channelPrefix: string;
  private readonly subscriberPrefix: string;

  constructor(private readonly options: RedisTransportOptions) {
    super();
    this.streamPrefix = options.streamPrefix ?? 'realtime:stream:';
    this.channelPrefix = options.channelPrefix ?? 'realtime:room:';
    this.subscriberPrefix = options.subscriberPrefix ?? 'realtime:subscriber:';
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const sequence = input.sequence ?? Date.now();
    const envelope = this.createEnvelope(input, sequence, 'redis');
    const serialized = JSON.stringify(envelope);
    await this.options.client.xAdd?.(this.streamKey(envelope.room), '*', { envelope: serialized });
    await this.options.client.publish?.(this.channelKey(envelope.room), serialized);
    this.fanout(envelope);
    return envelope;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    const localUnsubscribe = this.addListener(options, handler);
    const channel = this.channelKey(options.room);
    const redisHandler = (message: string) => {
      const event = JSON.parse(message) as RealtimeEnvelope<TPayload>;
      if (!options.eventTypes?.length || options.eventTypes.includes(event.type)) {
        void handler(event);
      }
    };
    void this.options.client.subscribe?.(channel, redisHandler);

    return async () => {
      localUnsubscribe();
      await this.options.client.unsubscribe?.(channel);
    };
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const records = await this.options.client.xRange?.(
      this.streamKey(options.room),
      '-',
      '+',
      { COUNT: options.limit ?? 500 },
    ) ?? [];

    const events = records
      .map((record) => JSON.parse(record.message.envelope) as RealtimeEnvelope<TPayload>)
      .filter((event) => event.sequence > options.fromSequence)
      .filter((event) => options.toSequence === undefined || event.sequence <= options.toSequence)
      .filter((event) => !options.eventTypes?.length || options.eventTypes.includes(event.type))
      .sort((a, b) => a.sequence - b.sequence);

    return { events };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.options.client.hSet?.(`${this.subscriberPrefix}${subscriberId}`, room, String(sequence));
  }

  async close(): Promise<void> {
    this.clearListeners();
    await this.options.client.quit?.();
  }

  private streamKey(room: string): string {
    return `${this.streamPrefix}${room}`;
  }

  private channelKey(room: string): string {
    return `${this.channelPrefix}${room}`;
  }
}

