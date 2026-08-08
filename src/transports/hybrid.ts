import type {
  CatchUpOptions,
  DLQHandle,
  EphemeralEnvelope,
  PublishInput,
  PublishEphemeralInput,
  RealtimeHealthStatus,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeMiddleware,
  RealtimeTransport,
  ReplayOptions,
  ReplayResult,
  Snapshot,
  SnapshotInput,
  StreamReplayOptions,
  SubscribeOptions,
  Unsubscribe,
} from '../core/types';

export interface HybridTransportOptions {
  storage: RealtimeTransport;
  realtime: RealtimeTransport;
}

export class HybridTransport implements RealtimeTransport {
  constructor(private readonly options: HybridTransportOptions) {}

  use(middleware: RealtimeMiddleware): void {
    this.options.storage.use(middleware);
    this.options.realtime.use(middleware);
  }

  async publish<TPayload>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const stored = await this.options.storage.publish(event);
    await this.options.realtime.publish(stored);
    return stored;
  }

  async publishEphemeral<TPayload>(event: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>): Promise<EphemeralEnvelope<TPayload>> {
    return this.options.realtime.publishEphemeral(event);
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    let closed = false;
    const delivered = new Set<string>();
    const deliveryQueue: string[] = [];
    const deliver = async (event: RealtimeEnvelope<TPayload>) => {
      if (closed || hasDelivered(delivered, deliveryQueue, event.id)) {
        return;
      }
      await handler(event);
    };

    const unsubscribeLive = this.options.realtime.subscribe(
      { ...options, catchUp: false },
      (event) => {
        void deliver(event as RealtimeEnvelope<TPayload>);
      },
    );

    if (options.catchUp !== false) {
      void (async () => {
        try {
          for await (const event of this.catchUp<TPayload>({
            room: options.room,
            subscriberId: options.subscriberId,
            fromSequence: options.fromSequence,
            eventTypes: options.eventTypes,
            batchSize: getSubscribeCatchUpBatchSize(options),
            maxBatches: getSubscribeCatchUpMaxBatches(options),
          })) {
            if (closed) {
              return;
            }
            await deliver(event);
          }
        } catch {
          // The storage transport owns detailed metrics/errors. Hybrid keeps live delivery available.
        }
      })();
    }

    return async () => {
      closed = true;
      await unsubscribeLive();
    };
  }

  subscribeEphemeral<TPayload = unknown>(
    options: Pick<SubscribeOptions, 'room' | 'eventTypes' | 'flowControl'>,
    handler: (event: EphemeralEnvelope<TPayload>) => void | Promise<void>,
  ): Unsubscribe {
    return this.options.realtime.subscribeEphemeral(options, handler);
  }

  replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    return this.options.storage.replay(options);
  }

  async *catchUp<TPayload = unknown>(options: CatchUpOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    const batchSize = normalizeLimit(options.batchSize ?? 500);
    const maxBatches = options.maxBatches === undefined ? Number.POSITIVE_INFINITY : Math.max(0, options.maxBatches);
    let batches = 0;
    let cursor = options.fromSequence ?? 0;

    if (options.fromSequence === undefined && options.subscriberId) {
      for await (const event of this.options.storage.catchUp<TPayload>({
        ...options,
        batchSize,
        maxBatches,
      })) {
        yield event;
      }
      return;
    }

    while (batches < maxBatches) {
      const result = await this.options.storage.replay<TPayload>({
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
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
  }

  streamReplay<TPayload = unknown>(options: StreamReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    return this.catchUp<TPayload>(options);
  }

  snapshot<TState = unknown>(room: string, snapshot: SnapshotInput<TState>): Promise<Snapshot<TState>> {
    return this.options.storage.snapshot(room, snapshot);
  }

  getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    return this.options.storage.getSnapshot(room);
  }

  dlq(room: string): DLQHandle {
    return this.options.storage.dlq(room);
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await Promise.all([
      this.options.storage.ack(room, sequence, subscriberId),
      this.options.realtime.ack(room, sequence, subscriberId),
    ]);
  }

  snapshotMetrics() {
    const storage = this.options.storage.snapshotMetrics?.();
    const realtime = this.options.realtime.snapshotMetrics?.();
    return {
      published: storage?.published ?? realtime?.published ?? 0,
      received: realtime?.received ?? storage?.received ?? 0,
      acked: Math.max(storage?.acked ?? 0, realtime?.acked ?? 0),
      gapsDetected: (storage?.gapsDetected ?? 0) + (realtime?.gapsDetected ?? 0),
      errors: (storage?.errors ?? 0) + (realtime?.errors ?? 0),
      replayed: storage?.replayed ?? 0,
      duplicatesDropped: (storage?.duplicatesDropped ?? 0) + (realtime?.duplicatesDropped ?? 0),
      activeRooms: realtime?.activeRooms ?? storage?.activeRooms ?? 0,
      activeListeners: realtime?.activeListeners ?? storage?.activeListeners ?? 0,
      averageDeliveryLagMs: realtime?.averageDeliveryLagMs ?? storage?.averageDeliveryLagMs ?? 0,
      retryCount: (storage?.retryCount ?? 0) + (realtime?.retryCount ?? 0),
      dlqSize: storage?.dlqSize ?? 0,
      snapshotUsage: storage?.snapshotUsage ?? 0,
      replayLatencyMs: storage?.replayLatencyMs ?? 0,
    };
  }

  async health() {
    const [storage, realtime] = await Promise.all([
      Promise.resolve(this.options.storage.health?.()),
      Promise.resolve(this.options.realtime.health?.()),
    ]);
    const statuses = [storage?.status, realtime?.status].filter(Boolean);
    const status: RealtimeHealthStatus = statuses.includes('unhealthy')
      ? 'unhealthy'
      : statuses.includes('degraded')
        ? 'degraded'
        : 'healthy';

    return {
      provider: 'hybrid' as const,
      status,
      details: { storage, realtime },
    };
  }

  async close(): Promise<void> {
    await Promise.all([this.options.storage.close(), this.options.realtime.close()]);
  }
}

function getSubscribeCatchUpBatchSize(options: SubscribeOptions): number {
  const catchUp = typeof options.catchUp === 'object' ? options.catchUp : undefined;
  return normalizeLimit(catchUp?.batchSize ?? options.limit ?? 500);
}

function getSubscribeCatchUpMaxBatches(options: SubscribeOptions): number | undefined {
  const catchUp = typeof options.catchUp === 'object' ? options.catchUp : undefined;
  return catchUp?.maxBatches;
}

function normalizeLimit(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 500;
  }
  return Math.min(Math.floor(value), 10000);
}

function hasDelivered(ids: Set<string>, queue: string[], eventId: string): boolean {
  if (!eventId) {
    return false;
  }
  if (ids.has(eventId)) {
    return true;
  }
  ids.add(eventId);
  queue.push(eventId);
  while (queue.length > 5000) {
    const expired = queue.shift();
    if (expired) {
      ids.delete(expired);
    }
  }
  return false;
}
