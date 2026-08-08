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

export interface RedisClientLike {
  incr?(key: string): Promise<number> | number;
  get?(key: string): Promise<string | null> | string | null;
  set?(key: string, value: string, options?: Record<string, unknown>): Promise<unknown> | unknown;
  del?(key: string): Promise<unknown> | unknown;
  xAdd?(key: string, id: string, message: Record<string, string>): Promise<string>;
  xRange?(key: string, start: string, end: string, options?: { COUNT?: number }): Promise<Array<{ id: string; message: Record<string, string> }>>;
  publish?(channel: string, message: string): Promise<number> | number;
  subscribe?(channel: string, handler: (message: string) => void): Promise<unknown> | unknown;
  unsubscribe?(channel: string): Promise<unknown> | unknown;
  hSet?(key: string, field: string, value: string): Promise<unknown>;
  hGet?(key: string, field: string): Promise<string | null> | string | null;
  quit?(): Promise<unknown>;
}

export interface RedisTransportOptions {
  client: RedisClientLike;
  subscriberClient?: RedisClientLike;
  streamPrefix?: string;
  channelPrefix?: string;
  subscriberPrefix?: string;
  sequencePrefix?: string;
  idempotencyPrefix?: string;
  sequenceIndexPrefix?: string;
  snapshotPrefix?: string;
  dlqPrefix?: string;
  persistStreams?: boolean;
  closeClients?: boolean;
  resilience?: TransportBaseOptions;
}

export class RedisTransport extends TransportBase implements RealtimeTransport {
  private readonly streamPrefix: string;
  private readonly channelPrefix: string;
  private readonly subscriberPrefix: string;
  private readonly sequencePrefix: string;
  private readonly idempotencyPrefix: string;
  private readonly sequenceIndexPrefix: string;
  private readonly snapshotPrefix: string;
  private readonly dlqPrefix: string;
  private readonly persistStreams: boolean;
  private readonly channelSubscriptions = new Map<string, { count: number; handler: (message: string) => void }>();

  constructor(private readonly options: RedisTransportOptions) {
    super(options.resilience, 'redis');
    this.streamPrefix = options.streamPrefix ?? 'realtime:stream:';
    this.channelPrefix = options.channelPrefix ?? 'realtime:room:';
    this.subscriberPrefix = options.subscriberPrefix ?? 'realtime:subscriber:';
    this.sequencePrefix = options.sequencePrefix ?? 'realtime:sequence:';
    this.idempotencyPrefix = options.idempotencyPrefix ?? 'realtime:idempotency:';
    this.sequenceIndexPrefix = options.sequenceIndexPrefix ?? 'realtime:sequence-index:';
    this.snapshotPrefix = options.snapshotPrefix ?? 'realtime:snapshot:';
    this.dlqPrefix = options.dlqPrefix ?? 'realtime:dlq:';
    this.persistStreams = options.persistStreams ?? true;
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const published = await this.runMiddleware<RealtimeEnvelope<TPayload>>(
      { action: 'publish', room: input.room, event: input },
      async (ctx) => {
        const eventInput = (ctx.event ?? input) as PublishInput<TPayload> | RealtimeEnvelope<TPayload>;
        const existing = eventInput.id ? await this.getIdempotentEnvelope<TPayload>(eventInput.id) : null;
        if (existing) {
          ctx.envelope = existing;
          return existing;
        }
        const reserved = eventInput.id ? await this.reserveIdempotencyKey(eventInput.id) : true;
        if (!reserved && eventInput.id) {
          const waited = await this.waitForIdempotentEnvelope<TPayload>(eventInput.id);
          ctx.envelope = waited;
          return waited;
        }

        try {
          const sequence = eventInput.sequence ?? await this.nextSequence(eventInput.room);
          const envelope = this.createEnvelope(eventInput, sequence, 'redis');
          const serialized = JSON.stringify(envelope);
          await this.withRetry(async () => {
            const streamId = this.persistStreams
              ? await this.options.client.xAdd?.(this.streamKey(envelope.room), '*', { envelope: serialized })
              : undefined;
            if (streamId) {
              await this.setSequenceStreamId(envelope.room, envelope.sequence, streamId);
            }
            await this.options.client.publish?.(this.channelKey(envelope.room), serialized);
            await this.setIdempotentEnvelope(envelope);
          }, { provider: 'redis', operation: 'publish', room: envelope.room });
          this.metrics.increment('published');
          this.fanout(envelope);
          ctx.envelope = envelope;
          return envelope;
        } catch (error) {
          if (eventInput.id) {
            await this.options.client.del?.(this.idempotencyKey(eventInput.id));
          }
          throw error;
        }
      },
    );
    if (!published) {
      throw new Error('Realtime publish was blocked by middleware');
    }
    return published;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    const { unsubscribe: localUnsubscribe, entry } = this.addListenerEntry(options, handler);
    const channel = this.channelKey(options.room);
    const subscriber = this.options.subscriberClient ?? this.options.client;
    const existing = this.channelSubscriptions.get(channel);

    if (existing) {
      existing.count += 1;
    } else {
      const redisHandler = (message: string) => {
        try {
          const event = JSON.parse(message) as RealtimeEnvelope<TPayload>;
          this.fanout(event);
        } catch {
          this.metrics.increment('errors');
        }
      };
      this.channelSubscriptions.set(channel, { count: 1, handler: redisHandler });
      void subscriber.subscribe?.(channel, redisHandler);
    }

    void this.deliverCatchUp(options, entry);

    return async () => {
      localUnsubscribe();
      const current = this.channelSubscriptions.get(channel);
      if (!current) {
        return;
      }

      current.count -= 1;
      if (current.count <= 0) {
        this.channelSubscriptions.delete(channel);
        await subscriber.unsubscribe?.(channel);
      }
    };
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const startedAt = Date.now();
    await this.runMiddleware({ action: 'replay', room: options.room });
    if (!this.persistStreams) {
      return { events: [] };
    }
    const indexedStart = await this.getSequenceStreamId(options.room, options.fromSequence);
    const requestedLimit = options.limit ?? 500;
    const records = await this.options.client.xRange?.(
      this.streamKey(options.room),
      indexedStart ?? '-',
      '+',
      { COUNT: indexedStart ? requestedLimit + 1 : requestedLimit },
    ) ?? [];

    const events = records
      .map((record) => JSON.parse(record.message.envelope) as RealtimeEnvelope<TPayload>)
      .filter((event) => event.sequence > options.fromSequence)
      .filter((event) => options.toSequence === undefined || event.sequence <= options.toSequence)
      .filter((event) => !options.eventTypes?.length || options.eventTypes.includes(event.type))
      .sort((a, b) => a.sequence - b.sequence)
      .slice(0, requestedLimit);

    this.metrics.increment('replayed', events.length);
    this.metrics.set('replayLatencyMs', Date.now() - startedAt);
    return { events };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.runMiddleware({ action: 'ack', room, subscriberId });
    await this.options.client.hSet?.(`${this.subscriberPrefix}${subscriberId}`, room, String(sequence));
    this.metrics.increment('acked');
  }

  async snapshot<TState = unknown>(room: string, input: SnapshotInput<TState>): Promise<Snapshot<TState>> {
    const snapshot: Snapshot<TState> = {
      room,
      lastSequence: input.lastSequence,
      state: input.state,
      createdAt: new Date(input.createdAt ?? Date.now()).toISOString(),
    };
    await this.runMiddleware({
      action: 'snapshot',
      room,
      snapshot: { room, lastSequence: snapshot.lastSequence, state: snapshot.state },
    }, async (ctx) => {
      await this.options.client.set?.(this.snapshotKey(room), JSON.stringify(snapshot));
      ctx.result = snapshot;
    });
    return snapshot;
  }

  async getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    let snapshot: Snapshot<TState> | null = null;
    await this.runMiddleware({ action: 'getSnapshot', room, snapshot: { room } }, async (ctx) => {
      const serialized = await this.options.client.get?.(this.snapshotKey(room));
      snapshot = serialized ? JSON.parse(serialized) as Snapshot<TState> : null;
      ctx.result = snapshot;
    });
    if (snapshot) {
      this.metrics.increment('snapshotUsage');
    }
    return snapshot;
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
    if (!subscriberId || !this.options.client.hGet) {
      return 0;
    }
    const value = await this.options.client.hGet(`${this.subscriberPrefix}${subscriberId}`, room);
    return Number(value ?? 0);
  }

  async close(): Promise<void> {
    this.clearListeners();
    const subscriber = this.options.subscriberClient ?? this.options.client;
    await Promise.all(Array.from(this.channelSubscriptions.keys()).map((channel) => subscriber.unsubscribe?.(channel)));
    this.channelSubscriptions.clear();

    if (this.options.closeClients) {
      await Promise.all([
        this.options.client.quit?.(),
        this.options.subscriberClient && this.options.subscriberClient !== this.options.client
          ? this.options.subscriberClient.quit?.()
          : undefined,
      ]);
    }
  }

  async health(): Promise<RealtimeHealth> {
    try {
      const probeKey = `${this.sequencePrefix}health`;
      await this.options.client.incr?.(probeKey);
      return {
        provider: 'redis',
        status: this.options.client.incr ? 'healthy' : 'degraded',
        details: {
          hasAtomicSequence: Boolean(this.options.client.incr),
          activeRooms: this.snapshotMetrics().activeRooms,
          activeListeners: this.snapshotMetrics().activeListeners,
        },
      };
    } catch (error) {
      return {
        provider: 'redis',
        status: 'unhealthy',
        details: {
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  private streamKey(room: string): string {
    return `${this.streamPrefix}${room}`;
  }

  private channelKey(room: string): string {
    return `${this.channelPrefix}${room}`;
  }

  private sequenceKey(room: string): string {
    return `${this.sequencePrefix}${room}`;
  }

  private idempotencyKey(id: string): string {
    return `${this.idempotencyPrefix}${id}`;
  }

  private sequenceIndexKey(room: string, sequence: number): string {
    return `${this.sequenceIndexPrefix}${room}:${sequence}`;
  }

  private snapshotKey(room: string): string {
    return `${this.snapshotPrefix}${room}`;
  }

  private dlqKey(room: string): string {
    return `${this.dlqPrefix}${room}`;
  }

  private async nextSequence(room: string): Promise<number> {
    const sequence = await this.options.client.incr?.(this.sequenceKey(room));
    if (typeof sequence === 'number' && Number.isSafeInteger(sequence) && sequence > 0) {
      return sequence;
    }

    return Date.now();
  }

  private async getIdempotentEnvelope<TPayload>(id: string): Promise<RealtimeEnvelope<TPayload> | null> {
    const serialized = await this.options.client.get?.(this.idempotencyKey(id));
    if (!serialized) {
      return null;
    }

    try {
      const parsed = JSON.parse(serialized) as RealtimeEnvelope<TPayload> | { state?: string };
      return (parsed as { state?: string }).state === 'pending'
        ? null
        : parsed as RealtimeEnvelope<TPayload>;
    } catch {
      this.metrics.increment('errors');
      return null;
    }
  }

  private async setIdempotentEnvelope<TPayload>(event: RealtimeEnvelope<TPayload>): Promise<void> {
    if (!event.id || !this.options.client.set) {
      return;
    }

    await this.options.client.set(this.idempotencyKey(event.id), JSON.stringify(event));
  }

  private async reserveIdempotencyKey(id: string): Promise<boolean> {
    if (!this.options.client.set) {
      return true;
    }
    const result = await this.options.client.set(
      this.idempotencyKey(id),
      JSON.stringify({ state: 'pending', createdAt: new Date().toISOString() }),
      { NX: true, PX: 30000 },
    );
    return result !== null && result !== false;
  }

  private async waitForIdempotentEnvelope<TPayload>(id: string): Promise<RealtimeEnvelope<TPayload>> {
    for (let attempt = 0; attempt < 20; attempt += 1) {
      await delay(25);
      const existing = await this.getIdempotentEnvelope<TPayload>(id);
      if (existing) {
        return existing;
      }
    }
    throw new Error(`Timed out waiting for idempotent realtime event: ${id}`);
  }

  private async setSequenceStreamId(room: string, sequence: number, streamId: string): Promise<void> {
    if (!this.options.client.set) {
      return;
    }
    await this.options.client.set(this.sequenceIndexKey(room, sequence), streamId);
  }

  private async getSequenceStreamId(room: string, sequence: number): Promise<string | null> {
    if (sequence <= 0 || !this.options.client.get) {
      return null;
    }
    return this.options.client.get(this.sequenceIndexKey(room, sequence));
  }

  protected async writeDLQ<TPayload>(room: string, event: RealtimeEnvelope<TPayload>, error: string, attempts: number): Promise<void> {
    const failed: DLQEvent<TPayload> = {
      id: `dlq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`,
      room,
      originalEvent: event,
      error,
      attempts,
      failedAt: new Date().toISOString(),
    };
    await this.runMiddleware({
      action: 'dlqWrite',
      room,
      envelope: event,
      dlq: { room, originalEvent: event, attempts, reason: error },
    }, async () => {
      await this.options.client.xAdd?.(this.dlqKey(room), '*', { event: JSON.stringify(failed) });
    });
  }

  protected async listDLQ<TPayload>(room: string, options: DLQListOptions): Promise<DLQEvent<TPayload>[]> {
    const records = await this.options.client.xRange?.(
      this.dlqKey(room),
      '-',
      '+',
      { COUNT: options.limit ?? 100 },
    ) ?? [];
    const from = options.fromFailedAt ? new Date(options.fromFailedAt).getTime() : 0;
    return records
      .map((record) => JSON.parse(record.message.event) as DLQEvent<TPayload>)
      .filter((event) => new Date(event.failedAt).getTime() >= from);
  }

  protected async clearDLQ(room: string): Promise<number> {
    const current = await this.listDLQ(room, {});
    await this.options.client.del?.(this.dlqKey(room));
    return current.length;
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
