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

export interface MongoCollectionLike<T = any> {
  insertOne(doc: T): Promise<unknown>;
  find(query: Record<string, unknown>, options?: Record<string, unknown>): { sort(sort: Record<string, 1 | -1>): { limit(limit: number): { toArray(): Promise<T[]> } } };
  updateOne(filter: Record<string, unknown>, update: Record<string, unknown>, options?: Record<string, unknown>): Promise<unknown>;
  findOneAndUpdate(filter: Record<string, unknown>, update: Record<string, unknown>, options?: Record<string, unknown>): Promise<{ value?: any }>;
  watch?(pipeline?: unknown[]): { on(event: string, handler: (change: any) => void): unknown; close(): Promise<void> | void };
}

export interface MongoTransportOptions {
  events: MongoCollectionLike;
  counters: MongoCollectionLike;
  subscribers: MongoCollectionLike;
}

export class MongoTransport extends TransportBase implements RealtimeTransport {
  private changeStream: { close(): Promise<void> | void } | null = null;

  constructor(private readonly options: MongoTransportOptions) {
    super();
    if (options.events.watch) {
      this.changeStream = options.events.watch([{ $match: { operationType: 'insert' } }]) as any;
      (this.changeStream as any).on?.('change', (change: any) => {
        const event = change?.fullDocument?.envelope ?? change?.fullDocument;
        if (event?.room) {
          this.fanout(event);
        }
      });
    }
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const counter = await this.options.counters.findOneAndUpdate(
      { room: input.room },
      { $inc: { sequence: 1 }, $setOnInsert: { room: input.room } },
      { upsert: true, returnDocument: 'after' },
    );
    const sequence = input.sequence ?? Number(counter.value?.sequence ?? 1);
    const envelope = this.createEnvelope(input, sequence, 'mongo');
    await this.options.events.insertOne({ ...envelope, envelope });
    this.fanout(envelope);
    return envelope;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    return this.addListener(options, handler);
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const query: Record<string, unknown> = {
      room: options.room,
      sequence: { $gt: options.fromSequence, ...(options.toSequence !== undefined ? { $lte: options.toSequence } : {}) },
    };
    if (options.eventTypes?.length) {
      query.type = { $in: options.eventTypes };
    }

    const docs = await this.options.events
      .find(query)
      .sort({ sequence: 1 })
      .limit(options.limit ?? 500)
      .toArray();

    return { events: docs.map((doc: any) => doc.envelope ?? doc) };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.options.subscribers.updateOne(
      { room, subscriberId },
      { $max: { lastAckSequence: sequence }, $set: { updatedAt: new Date() }, $setOnInsert: { room, subscriberId } },
      { upsert: true },
    );
  }

  async close(): Promise<void> {
    this.clearListeners();
    await this.changeStream?.close();
  }
}

