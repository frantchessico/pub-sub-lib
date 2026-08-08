import { createRealtimeEnvelope } from '../core/envelope';
import { createMiddlewareContext, runMiddlewares, type MiddlewareContextInput } from '../core/middleware';
import { BackpressureError } from '../errors';
import type {
  DLQEvent,
  DLQHandle,
  DLQListOptions,
  DLQReplayOptions,
  EphemeralEnvelope,
  FlowControlOptions,
  PublishInput,
  PublishEphemeralInput,
  RealtimeMetrics,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeMiddleware,
  RealtimeStorageProvider,
  RetryOptions,
  Snapshot,
  SnapshotInput,
  SubscribeCatchUpOptions,
  SubscribeOptions,
  StreamReplayOptions,
  Unsubscribe,
} from '../core/types';
import { MetricsCounter } from '../core/metrics';

export type ListenerEntry = {
  options: SubscribeOptions;
  handler: RealtimeEventHandler<any>;
  pending: number;
  eventTypeSet?: Set<string>;
  deliveredIds: Set<string>;
  deliveredQueue: string[];
  bufferedEvents: RealtimeEnvelope[];
  closed: boolean;
};

export type EphemeralListenerEntry = {
  room: string;
  handler: (event: EphemeralEnvelope<any>) => void | Promise<void>;
  eventTypeSet?: Set<string>;
  pending: number;
  bufferedEvents: EphemeralEnvelope[];
  closed: boolean;
  flowControl?: FlowControlOptions;
};

export interface TransportBaseOptions {
  maxSeenEvents?: number;
  maxPendingPerListener?: number;
  maxPublishRetries?: number;
  retryBaseDelayMs?: number;
}

export abstract class TransportBase {
  protected readonly listeners = new Map<string, Map<string, ListenerEntry>>();
  protected readonly ephemeralListeners = new Map<string, Map<string, EphemeralListenerEntry>>();
  protected readonly metrics = new MetricsCounter();
  protected readonly snapshots = new Map<string, Snapshot>();
  protected readonly dlqEvents = new Map<string, DLQEvent[]>();
  private readonly middleware: RealtimeMiddleware[] = [];
  private readonly seenEventIds = new Set<string>();
  private readonly seenEventQueue: string[] = [];
  private readonly maxSeenEvents: number;
  private readonly maxPendingPerListener: number;
  private readonly maxPublishRetries: number;
  private readonly retryBaseDelayMs: number;
  private listenerSequence = 0;
  private activeListenersCount = 0;

  constructor(options: TransportBaseOptions | number = {}, protected readonly provider: RealtimeStorageProvider | 'hybrid' = 'redis') {
    const normalized = typeof options === 'number' ? { maxSeenEvents: options } : options;
    this.maxSeenEvents = normalized.maxSeenEvents ?? 5000;
    this.maxPendingPerListener = normalized.maxPendingPerListener ?? 1000;
    this.maxPublishRetries = normalized.maxPublishRetries ?? 2;
    this.retryBaseDelayMs = normalized.retryBaseDelayMs ?? 25;
  }

  protected createEnvelope<TPayload>(
    input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>,
    sequence: number,
    provider: RealtimeStorageProvider,
  ): RealtimeEnvelope<TPayload> {
    return createRealtimeEnvelope(input, sequence, provider);
  }

  protected fanout<TPayload>(event: RealtimeEnvelope<TPayload>): void {
    if (this.hasSeen(event.id)) {
      this.metrics.increment('duplicatesDropped');
      return;
    }
    this.markSeen(event.id);

    const roomListeners = this.listeners.get(event.room);
    if (roomListeners) {
      roomListeners.forEach((entry) => this.deliverToListener(entry, event));
    }

    this.listeners.forEach((entries, pattern) => {
      if (pattern !== event.room && isWildcardRoom(pattern) && roomMatches(pattern, event.room)) {
        entries.forEach((entry) => this.deliverToListener(entry, event));
      }
    });
  }

  protected addListener<TPayload>(
    options: SubscribeOptions,
    handler: RealtimeEventHandler<TPayload>,
  ): Unsubscribe {
    return this.addListenerEntry(options, handler).unsubscribe;
  }

  protected addListenerEntry<TPayload>(
    options: SubscribeOptions,
    handler: RealtimeEventHandler<TPayload>,
  ): { unsubscribe: Unsubscribe; entry: ListenerEntry } {
    const id = `listener_${++this.listenerSequence}`;
    const roomListeners = this.listeners.get(options.room) ?? new Map<string, ListenerEntry>();
    const entry: ListenerEntry = {
      options,
      handler,
      pending: 0,
      eventTypeSet: options.eventTypes?.length ? new Set(options.eventTypes) : undefined,
      deliveredIds: new Set<string>(),
      deliveredQueue: [],
      bufferedEvents: [],
      closed: false,
    };
    roomListeners.set(id, entry);
    this.listeners.set(options.room, roomListeners);
    this.activeListenersCount += 1;

    const unsubscribe = () => {
      entry.closed = true;
      const current = this.listeners.get(options.room);
      if (current?.delete(id)) {
        this.activeListenersCount -= 1;
      }
      if (current && current.size === 0) {
        this.listeners.delete(options.room);
      }
    };

    return { unsubscribe, entry };
  }

  protected deliverToListener<TPayload>(entry: ListenerEntry, event: RealtimeEnvelope<TPayload>): void {
    void this.deliverToListenerAsync(entry, event);
  }

  protected async deliverToListenerAsync<TPayload>(entry: ListenerEntry, event: RealtimeEnvelope<TPayload>): Promise<boolean> {
    if (entry.closed) {
      return false;
    }

    if (entry.eventTypeSet?.size && !entry.eventTypeSet.has(event.type)) {
      return false;
    }

    if (this.hasListenerSeen(entry, event.id)) {
      this.metrics.increment('duplicatesDropped');
      return false;
    }
    this.markListenerSeen(entry, event.id);

    if (!this.acceptByFlowControl(entry, event)) {
      return false;
    }

    entry.pending += 1;
    try {
      await this.runHandlerWithRetry(entry, event);
      this.metrics.increment('received');
      this.metrics.recordDeliveryLag(Date.now() - new Date(event.emittedAt).getTime());
      await this.drainBufferedEvents(entry);
      return true;
    } catch {
      this.metrics.increment('errors');
      return false;
    } finally {
      entry.pending -= 1;
    }
  }

  use(middleware: RealtimeMiddleware): void {
    this.middleware.push(middleware);
  }

  useMany(middlewares: RealtimeMiddleware[] = []): void {
    middlewares.forEach((middleware) => this.use(middleware));
  }

  async publishEphemeral<TPayload = unknown>(
    input: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>,
  ): Promise<EphemeralEnvelope<TPayload>> {
    const envelope: EphemeralEnvelope<TPayload> = {
      id: input.id ?? randomId('eph'),
      type: input.type,
      room: input.room,
      emittedAt: new Date(input.emittedAt ?? Date.now()).toISOString(),
      payload: input.payload,
      metadata: input.metadata,
    };
    const isPresenceEnter = envelope.type === 'presence.enter';
    const isPresenceLeave = envelope.type === 'presence.leave';
    const presencePayload = envelope.payload as { user?: { userId?: string; metadata?: Record<string, unknown> }; userId?: string };
    await this.runMiddleware({
      action: isPresenceEnter ? 'presenceEnter' : isPresenceLeave ? 'presenceLeave' : 'ephemeralPublish',
      room: envelope.room,
      event: envelope,
      presence: isPresenceEnter || isPresenceLeave
        ? {
          room: envelope.room,
          userId: presencePayload.user?.userId ?? presencePayload.userId,
          metadata: presencePayload.user?.metadata,
        }
        : undefined,
      ephemeral: {
        room: envelope.room,
        type: envelope.type,
        payload: envelope.payload,
      },
    }, async (ctx) => {
      this.fanoutEphemeral(envelope);
      ctx.result = envelope;
    });
    return envelope;
  }

  subscribeEphemeral<TPayload = unknown>(
    options: Pick<SubscribeOptions, 'room' | 'eventTypes' | 'flowControl'>,
    handler: (event: EphemeralEnvelope<TPayload>) => void | Promise<void>,
  ): Unsubscribe {
    const id = `ephemeral_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const roomListeners = this.ephemeralListeners.get(options.room) ?? new Map<string, EphemeralListenerEntry>();
    const entry: EphemeralListenerEntry = {
      room: options.room,
      handler,
      eventTypeSet: options.eventTypes?.length ? new Set(options.eventTypes) : undefined,
      pending: 0,
      bufferedEvents: [],
      closed: false,
      flowControl: options.flowControl,
    };
    roomListeners.set(id, entry);
    this.ephemeralListeners.set(options.room, roomListeners);
    return () => {
      entry.closed = true;
      const current = this.ephemeralListeners.get(options.room);
      current?.delete(id);
      if (current?.size === 0) {
        this.ephemeralListeners.delete(options.room);
      }
    };
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
      snapshot: {
        room,
        lastSequence: snapshot.lastSequence,
        state: snapshot.state,
      },
    }, async (ctx) => {
      this.snapshots.set(room, snapshot);
      ctx.result = snapshot;
    });
    return snapshot;
  }

  async getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    let snapshot: Snapshot<TState> | undefined;
    await this.runMiddleware({ action: 'getSnapshot', room, snapshot: { room } }, async (ctx) => {
      snapshot = this.snapshots.get(room) as Snapshot<TState> | undefined;
      ctx.result = snapshot ?? null;
    });
    if (snapshot) {
      this.metrics.increment('snapshotUsage');
    }
    return snapshot ?? null;
  }

  abstract catchUp<TPayload = unknown>(options: StreamReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>>;

  streamReplay<TPayload = unknown>(options: StreamReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    return this.catchUp<TPayload>(options);
  }

  dlq(room: string): DLQHandle {
    const thisBase = this;
    return {
      list: async <TPayload = unknown>(options: DLQListOptions = {}) => {
        let result: DLQEvent<TPayload>[] = [];
        await this.runMiddleware({ action: 'dlqReplay', room, dlq: { room } }, async (ctx) => {
          result = await this.listDLQ<TPayload>(room, options);
          ctx.result = result;
        });
        return result;
      },
      replay: async function* <TPayload = unknown>(this: void, options: DLQReplayOptions = {}) {
        let events: DLQEvent<TPayload>[] = [];
        await thisBase.runMiddleware({ action: 'dlqReplay', room, dlq: { room } }, async (ctx) => {
          events = await thisBase.listDLQ<TPayload>(room, options);
          ctx.result = events;
        });
        for (const failed of events) {
          yield failed.originalEvent;
        }
        if (options.deleteOnSuccess) {
          await thisBase.clearDLQ(room, options);
        }
      },
      clear: async (options: DLQListOptions = {}) => this.clearDLQ(room, options),
    };
  }

  protected async runMiddleware<T>(
    input: MiddlewareContextInput,
    core: (ctx: ReturnType<typeof createMiddlewareContext>) => Promise<T> | T = async () => undefined as T,
  ): Promise<T | undefined> {
    const ctx = createMiddlewareContext(this.provider, input);
    let result: T | undefined;
    await runMiddlewares(this.middleware, ctx, async () => {
      result = await core(ctx);
      if (ctx.result === undefined) {
        ctx.result = result;
      }
    });
    return (ctx.result as T | undefined) ?? result;
  }

  protected clearListeners(): void {
    this.listeners.forEach((roomListeners) => {
      roomListeners.forEach((entry) => {
        entry.closed = true;
      });
    });
    this.listeners.clear();
    this.ephemeralListeners.forEach((roomListeners) => {
      roomListeners.forEach((entry) => {
        entry.closed = true;
      });
    });
    this.ephemeralListeners.clear();
    this.activeListenersCount = 0;
  }

  protected normalizeSubscribeCatchUp(options: SubscribeOptions): Required<SubscribeCatchUpOptions> & { enabled: boolean } {
    if (options.catchUp === false) {
      return { enabled: false, batchSize: 0, maxBatches: 0 };
    }

    const config = typeof options.catchUp === 'object' ? options.catchUp : {};
    return {
      enabled: true,
      batchSize: normalizeLimit(config.batchSize ?? options.limit ?? 500),
      maxBatches: config.maxBatches === undefined ? Number.POSITIVE_INFINITY : Math.max(0, config.maxBatches),
    };
  }

  protected normalizeCatchUpBatchSize(batchSize?: number): number {
    return normalizeLimit(batchSize ?? 500);
  }

  protected async yieldCatchUpPage(): Promise<void> {
    await delay(0);
  }

  protected async withRetry<T>(operation: () => Promise<T>, context: Record<string, unknown> = {}): Promise<T> {
    let attempt = 0;
    let lastError: unknown;

    while (attempt <= this.maxPublishRetries) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        attempt += 1;
        if (attempt > this.maxPublishRetries) {
          break;
        }
        await delay(this.retryBaseDelayMs * (2 ** (attempt - 1)));
      }
    }

    this.metrics.increment('errors');
    throw lastError instanceof Error
      ? lastError
      : new Error(`Realtime operation failed: ${JSON.stringify(context)}`);
  }

  snapshotMetrics(): RealtimeMetrics {
    this.metrics.set('activeRooms', this.listeners.size);
    this.metrics.set('activeListeners', this.activeListenersCount);
    this.metrics.set('dlqSize', Array.from(this.dlqEvents.values()).reduce((sum, events) => sum + events.length, 0));
    return this.metrics.snapshot();
  }

  private acceptByFlowControl<TPayload>(entry: ListenerEntry, event: RealtimeEnvelope<TPayload>): boolean {
    const flow = normalizeFlowControl(entry.options.flowControl, this.maxPendingPerListener);
    if (entry.pending < flow.maxInFlight) {
      return true;
    }

    if (flow.strategy === 'drop') {
      this.metrics.increment('errors');
      return false;
    }

    if (flow.strategy === 'buffer') {
      if (entry.bufferedEvents.length >= flow.maxBufferSize) {
        this.metrics.increment('errors');
        return false;
      }
      entry.bufferedEvents.push(event);
      return false;
    }

    this.metrics.increment('errors');
    void Promise.reject(new BackpressureError('Realtime listener backlog exceeded', {
      room: event.room,
      eventId: event.id,
      pending: entry.pending,
    })).catch(() => undefined);
    return false;
  }

  private async runHandlerWithRetry<TPayload>(entry: ListenerEntry, event: RealtimeEnvelope<TPayload>): Promise<void> {
    const retry = normalizeRetry(entry.options.retry);
    let attempt = 0;
    let lastError: unknown;

    while (attempt <= retry.attempts) {
      try {
        await this.runMiddleware({
          action: 'deliver',
          room: event.room,
          subscriberId: entry.options.subscriberId,
          sequence: event.sequence,
          envelope: event,
        }, async () => {
          await entry.handler(event);
        });
        return;
      } catch (error) {
        lastError = error;
        if (attempt >= retry.attempts) {
          break;
        }
        this.metrics.increment('retryCount');
        attempt += 1;
        await delay(getRetryDelay(retry, attempt));
      }
    }

    await this.writeDLQ(
      event.room,
      event,
      lastError instanceof Error ? lastError.message : String(lastError),
      retry.attempts + 1,
    );
    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  }

  private async drainBufferedEvents(entry: ListenerEntry): Promise<void> {
    while (!entry.closed && entry.bufferedEvents.length > 0 && entry.pending < normalizeFlowControl(entry.options.flowControl, this.maxPendingPerListener).maxInFlight) {
      const event = entry.bufferedEvents.shift();
      if (!event) {
        return;
      }
      await this.deliverToListenerAsync(entry, event);
    }
  }

  private fanoutEphemeral<TPayload>(event: EphemeralEnvelope<TPayload>): void {
    const roomListeners = this.ephemeralListeners.get(event.room);
    if (!roomListeners) {
      return;
    }
    roomListeners.forEach((entry) => {
      void this.deliverEphemeral(entry, event);
    });
  }

  private async deliverEphemeral<TPayload>(entry: EphemeralListenerEntry, event: EphemeralEnvelope<TPayload>): Promise<void> {
    if (entry.closed || (entry.eventTypeSet?.size && !entry.eventTypeSet.has(event.type))) {
      return;
    }
    const flow = normalizeFlowControl(entry.flowControl, this.maxPendingPerListener);
    if (entry.pending >= flow.maxInFlight) {
      if (flow.strategy === 'buffer' && entry.bufferedEvents.length < flow.maxBufferSize) {
        entry.bufferedEvents.push(event);
      }
      return;
    }
    entry.pending += 1;
    try {
      await this.runMiddleware({
        action: 'ephemeralDeliver',
        room: event.room,
        event,
        ephemeral: {
          room: event.room,
          type: event.type,
          payload: event.payload,
        },
      }, async () => {
        await entry.handler(event);
      });
    } catch {
      this.metrics.increment('errors');
    } finally {
      entry.pending -= 1;
    }
  }

  protected async writeDLQ<TPayload>(room: string, event: RealtimeEnvelope<TPayload>, error: string, attempts: number): Promise<void> {
    const failed: DLQEvent<TPayload> = {
      id: randomId('dlq'),
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
      dlq: {
        room,
        originalEvent: event,
        attempts,
        reason: error,
      },
    }, async (ctx) => {
      const events = this.dlqEvents.get(room) ?? [];
      events.push(failed);
      this.dlqEvents.set(room, events);
      this.metrics.set('dlqSize', Array.from(this.dlqEvents.values()).reduce((sum, roomEvents) => sum + roomEvents.length, 0));
      ctx.result = failed;
    });
  }

  protected async listDLQ<TPayload>(room: string, options: DLQListOptions): Promise<DLQEvent<TPayload>[]> {
    const from = options.fromFailedAt ? new Date(options.fromFailedAt).getTime() : 0;
    return (this.dlqEvents.get(room) ?? [])
      .filter((event) => new Date(event.failedAt).getTime() >= from)
      .slice(0, options.limit ?? 100) as DLQEvent<TPayload>[];
  }

  protected async clearDLQ(room: string, options: DLQListOptions): Promise<number> {
    const current = this.dlqEvents.get(room) ?? [];
    const remove = await this.listDLQ(room, options);
    const removeIds = new Set(remove.map((event) => event.id));
    const kept = current.filter((event) => !removeIds.has(event.id));
    this.dlqEvents.set(room, kept);
    return remove.length;
  }

  private hasSeen(eventId: string): boolean {
    return Boolean(eventId && this.seenEventIds.has(eventId));
  }

  private markSeen(eventId: string): void {
    if (!eventId) {
      return;
    }

    this.seenEventIds.add(eventId);
    this.seenEventQueue.push(eventId);

    while (this.seenEventQueue.length > this.maxSeenEvents) {
      const expired = this.seenEventQueue.shift();
      if (expired) {
        this.seenEventIds.delete(expired);
      }
    }
  }

  private hasListenerSeen(entry: ListenerEntry, eventId: string): boolean {
    return Boolean(eventId && entry.deliveredIds.has(eventId));
  }

  private markListenerSeen(entry: ListenerEntry, eventId: string): void {
    if (!eventId) {
      return;
    }

    entry.deliveredIds.add(eventId);
    entry.deliveredQueue.push(eventId);

    while (entry.deliveredQueue.length > this.maxSeenEvents) {
      const expired = entry.deliveredQueue.shift();
      if (expired) {
        entry.deliveredIds.delete(expired);
      }
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeLimit(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 500;
  }
  return Math.min(Math.floor(value), 10000);
}

function normalizeRetry(options?: RetryOptions): Required<RetryOptions> {
  return {
    attempts: Math.max(0, options?.attempts ?? 0),
    strategy: options?.strategy ?? 'exponential',
    baseDelayMs: Math.max(0, options?.baseDelayMs ?? 100),
  };
}

function getRetryDelay(options: Required<RetryOptions>, attempt: number): number {
  if (options.strategy === 'fixed') {
    return options.baseDelayMs;
  }
  if (options.strategy === 'linear') {
    return options.baseDelayMs * attempt;
  }
  return options.baseDelayMs * (2 ** (attempt - 1));
}

function normalizeFlowControl(options: FlowControlOptions | undefined, fallbackMaxInFlight: number): Required<FlowControlOptions> {
  return {
    maxInFlight: Math.max(1, options?.maxInFlight ?? fallbackMaxInFlight),
    strategy: options?.strategy ?? 'pause',
    maxBufferSize: Math.max(0, options?.maxBufferSize ?? fallbackMaxInFlight),
  };
}

function randomId(prefix: string): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function isWildcardRoom(room: string): boolean {
  return room.endsWith(':*');
}

function roomMatches(pattern: string, room: string): boolean {
  if (!isWildcardRoom(pattern)) {
    return pattern === room;
  }
  return room.startsWith(pattern.slice(0, -1));
}
