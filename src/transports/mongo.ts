import type {
  CatchUpOptions,
  DLQEvent,
  DLQListOptions,
  PublishInput,
  RealtimeHealth,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeTransport,
  ReplayOptions,
  ReplayResult,
  Snapshot,
  SnapshotInput,
  SubscribeOptions,
  Unsubscribe,
} from '../core/types';
import { TransportBase } from './base';
import type { ListenerEntry, TransportBaseOptions } from './base';

export interface MongoCollectionLike<T = any> {
  insertOne(doc: T): Promise<unknown>;
  find(query: Record<string, unknown>, options?: Record<string, unknown>): { sort(sort: Record<string, 1 | -1>): { limit(limit: number): { toArray(): Promise<T[]> } } };
  updateOne(filter: Record<string, unknown>, update: Record<string, unknown>, options?: Record<string, unknown>): Promise<unknown>;
  deleteMany?(filter: Record<string, unknown>): Promise<{ deletedCount?: number } | unknown>;
  findOneAndUpdate(filter: Record<string, unknown>, update: Record<string, unknown>, options?: Record<string, unknown>): Promise<{ value?: any } | any>;
  watch?(pipeline?: unknown[]): { on(event: string, handler: (change: any) => void): unknown; close(): Promise<void> | void };
}

export interface MongoTransportOptions {
  events: MongoCollectionLike;
  counters: MongoCollectionLike;
  subscribers: MongoCollectionLike;
  snapshots?: MongoCollectionLike;
  dlq?: MongoCollectionLike;
  resilience?: TransportBaseOptions;
}

export class MongoTransport extends TransportBase implements RealtimeTransport {
  private changeStream: { close(): Promise<void> | void } | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(private readonly options: MongoTransportOptions) {
    super(options.resilience, 'mongo');
    this.openChangeStream();
  }

  private openChangeStream(): void {
    if (this.closed || !this.options.events.watch) {
      return;
    }

    try {
      this.changeStream = this.options.events.watch([{ $match: { operationType: 'insert' } }]) as any;
      (this.changeStream as any).on?.('change', (change: any) => {
        const event = change?.fullDocument?.envelope ?? change?.fullDocument;
        if (event?.room) {
          this.fanout(event);
        }
      });
      (this.changeStream as any).on?.('error', () => this.reconnectChangeStream());
      (this.changeStream as any).on?.('close', () => this.reconnectChangeStream());
    } catch {
      this.reconnectChangeStream();
    }
  }

  private reconnectChangeStream(): void {
    if (this.closed || this.reconnectTimer) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void Promise.resolve(this.changeStream?.close()).catch(() => undefined);
      this.changeStream = null;
      this.openChangeStream();
    }, reconnectDelay());
    (this.reconnectTimer as any).unref?.();
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const published = await this.runMiddleware<RealtimeEnvelope<TPayload>>(
      { action: 'publish', room: input.room, event: input },
      async (ctx) => {
        const eventInput = (ctx.event ?? input) as PublishInput<TPayload> | RealtimeEnvelope<TPayload>;
        if (eventInput.id) {
          const existing = await this.findById<TPayload>(eventInput.id);
          if (existing) {
            ctx.envelope = existing;
            return existing;
          }
        }

        const counter = await this.options.counters.findOneAndUpdate(
          { room: eventInput.room },
          { $inc: { sequence: 1 }, $setOnInsert: { room: eventInput.room } },
          { upsert: true, returnDocument: 'after' },
        );
        const counterDocument = getMongoResultDocument(counter);
        const sequence = eventInput.sequence ?? Number(counterDocument?.sequence ?? 1);
        const envelope = this.createEnvelope(eventInput, sequence, 'mongo');
        await this.withRetry(
          async () => {
            try {
              await this.options.events.insertOne({ ...envelope, envelope });
            } catch (error) {
              if (isDuplicateKeyError(error) && eventInput.id) {
                return;
              }
              throw error;
            }
          },
          { provider: 'mongo', operation: 'insertOne', room: envelope.room },
        );
        if (eventInput.id) {
          const existing = await this.findById<TPayload>(eventInput.id);
          if (existing) {
            this.metrics.increment('published');
            this.fanout(existing);
            ctx.envelope = existing;
            return existing;
          }
        }
        this.metrics.increment('published');
        this.fanout(envelope);
        ctx.envelope = envelope;
        return envelope;
      }
    );
    if (!published) {
      throw new Error('Realtime publish was blocked by middleware');
    }
    return published;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    const { unsubscribe, entry } = this.addListenerEntry(options, handler);
    void this.deliverCatchUp(options, entry);
    return unsubscribe;
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const startedAt = Date.now();
    const result = await this.runMiddleware<ReplayResult<TPayload>>({
      action: 'replay',
      room: options.room,
      replay: {
        fromSequence: options.fromSequence,
        toSequence: options.toSequence,
        limit: options.limit,
        eventTypes: options.eventTypes,
      },
    }, async (ctx) => {
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

      const events = docs.map((doc: any) => doc.envelope ?? doc);
      this.metrics.increment('replayed', events.length);
      this.metrics.set('replayLatencyMs', Date.now() - startedAt);
      const replayResult = { events };
      ctx.result = replayResult;
      return replayResult;
    });
    return result ?? { events: [] };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.runMiddleware({
      action: 'ack',
      room,
      sequence,
      subscriberId,
      ack: { room, sequence, subscriberId },
    }, async () => {
      await this.options.subscribers.updateOne(
        { room, subscriberId },
        { $max: { lastAckSequence: sequence }, $set: { updatedAt: new Date() }, $setOnInsert: { room, subscriberId } },
        { upsert: true },
      );
      this.metrics.increment('acked');
    });
  }

  async snapshot<TState = unknown>(room: string, input: SnapshotInput<TState>): Promise<Snapshot<TState>> {
    if (!this.options.snapshots) {
      return super.snapshot(room, input);
    }
    const snapshot: Snapshot<TState> = {
      room,
      lastSequence: input.lastSequence,
      state: input.state,
      createdAt: new Date(input.createdAt ?? Date.now()).toISOString(),
    };
    await this.runMiddleware({ action: 'snapshot', room });
    await this.options.snapshots.updateOne(
      { room },
      { $set: snapshot },
      { upsert: true },
    );
    return snapshot;
  }

  async getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    if (!this.options.snapshots) {
      return super.getSnapshot<TState>(room);
    }
    const docs = await this.options.snapshots
      .find({ room })
      .sort({ lastSequence: -1 })
      .limit(1)
      .toArray();
    const snapshot = docs[0] as Snapshot<TState> | undefined;
    if (snapshot) {
      this.metrics.increment('snapshotUsage');
    }
    return snapshot ?? null;
  }

  async *catchUp<TPayload = unknown>(options: CatchUpOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    const batchSize = this.normalizeCatchUpBatchSize(options.batchSize);
    await this.runMiddleware({
      action: 'catchUp',
      room: options.room,
      subscriberId: options.subscriberId,
      catchUp: {
        fromSequence: options.fromSequence,
        toSequence: options.toSequence,
        batchSize,
        maxBatches: options.maxBatches,
      },
    });
    const maxBatches = options.maxBatches === undefined ? Number.POSITIVE_INFINITY : Math.max(0, options.maxBatches);
    let batches = 0;
    let cursor = options.fromSequence ?? await this.getLastAckSequence(options.room, options.subscriberId);

    while (batches < maxBatches) {
      const result = await this.replay<TPayload>({
        room: options.room,
        fromSequence: cursor,
        toSequence: options.toSequence,
        eventTypes: options.eventTypes,
        limit: batchSize,
      });
      if (result.events.length === 0) {
        return;
      }

      for (const event of result.events) {
        cursor = event.sequence;
        yield event;
      }

      batches += 1;
      if (result.events.length < batchSize || (options.toSequence !== undefined && cursor >= options.toSequence)) {
        return;
      }
      await this.yieldCatchUpPage();
    }
  }

  private async deliverCatchUp(options: SubscribeOptions, entry: ListenerEntry): Promise<void> {
    const catchUp = this.normalizeSubscribeCatchUp(options);
    if (!catchUp.enabled) {
      return;
    }

    try {
      for await (const event of this.catchUp({
        room: options.room,
        subscriberId: options.subscriberId,
        fromSequence: options.fromSequence,
        eventTypes: options.eventTypes,
        batchSize: catchUp.batchSize,
        maxBatches: catchUp.maxBatches,
      })) {
        if (entry.closed) {
          return;
        }
        await this.deliverToListenerAsync(entry, event);
      }
    } catch {
      this.metrics.increment('errors');
    }
  }

  private async getLastAckSequence(room: string, subscriberId?: string): Promise<number> {
    if (!subscriberId) {
      return 0;
    }
    const docs = await this.options.subscribers
      .find({ room, subscriberId })
      .sort({ lastAckSequence: -1 })
      .limit(1)
      .toArray();
    return Number((docs[0] as any)?.lastAckSequence ?? 0);
  }

  protected async writeDLQ<TPayload>(room: string, event: RealtimeEnvelope<TPayload>, error: string, attempts: number): Promise<void> {
    if (!this.options.dlq) {
      return super.writeDLQ(room, event, error, attempts);
    }
    const failed: DLQEvent<TPayload> = {
      id: `dlq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`,
      room,
      originalEvent: event,
      error,
      attempts,
      failedAt: new Date().toISOString(),
    };
    await this.options.dlq.insertOne(failed);
  }

  protected async listDLQ<TPayload>(room: string, options: DLQListOptions): Promise<DLQEvent<TPayload>[]> {
    if (!this.options.dlq) {
      return super.listDLQ<TPayload>(room, options);
    }
    const query: Record<string, unknown> = { room };
    if (options.fromFailedAt) {
      query.failedAt = { $gte: new Date(options.fromFailedAt).toISOString() };
    }
    return this.options.dlq
      .find(query)
      .sort({ failedAt: 1 })
      .limit(options.limit ?? 100)
      .toArray() as Promise<DLQEvent<TPayload>[]>;
  }

  protected async clearDLQ(room: string, options: DLQListOptions): Promise<number> {
    if (!this.options.dlq?.deleteMany) {
      return super.clearDLQ(room, options);
    }
    const query: Record<string, unknown> = { room };
    if (options.fromFailedAt) {
      query.failedAt = { $gte: new Date(options.fromFailedAt).toISOString() };
    }
    const result = await this.options.dlq.deleteMany(query) as { deletedCount?: number } | undefined;
    return result?.deletedCount ?? 0;
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.clearListeners();
    await this.changeStream?.close();
  }

  async health(): Promise<RealtimeHealth> {
    try {
      await this.options.counters.find({ room: '__health__' }).sort({ sequence: 1 }).limit(1).toArray();
      return {
        provider: 'mongo',
        status: 'healthy',
        details: {
          hasChangeStream: Boolean(this.changeStream),
          activeRooms: this.snapshotMetrics().activeRooms,
          activeListeners: this.snapshotMetrics().activeListeners,
        },
      };
    } catch (error) {
      return {
        provider: 'mongo',
        status: 'unhealthy',
        details: {
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  private async findById<TPayload>(id: string): Promise<RealtimeEnvelope<TPayload> | null> {
    const docs = await this.options.events
      .find({ id })
      .sort({ sequence: 1 })
      .limit(1)
      .toArray();
    const doc = docs[0] as any;
    return doc ? (doc.envelope ?? doc) : null;
  }
}

function reconnectDelay(): number {
  return 1000;
}

function isDuplicateKeyError(error: unknown): boolean {
  const maybe = error as { code?: number; name?: string };
  return maybe?.code === 11000 || maybe?.name === 'MongoServerError';
}

function getMongoResultDocument(result: { value?: any } | any): any {
  if (!result) {
    return null;
  }

  if (Object.prototype.hasOwnProperty.call(result, 'value')) {
    return result.value;
  }

  return result;
}
