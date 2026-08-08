import { validateRealtimeEnvelope } from '../core/envelope';
import type {
  CatchUpOptions,
  DLQEvent,
  DLQListOptions,
  RealtimeHealth,
  PublishInput,
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

export interface PostgresClientLike {
  query<T = any>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export interface PostgresListenClientLike extends PostgresClientLike {
  on?(event: 'notification', handler: (message: { channel?: string; payload?: string }) => void): unknown;
  off?(event: 'notification', handler: (message: { channel?: string; payload?: string }) => void): unknown;
  removeListener?(event: 'notification', handler: (message: { channel?: string; payload?: string }) => void): unknown;
}

export interface PostgresTransportOptions {
  client: PostgresClientLike;
  listenerClient?: PostgresListenClientLike;
  eventsTable?: string;
  subscribersTable?: string;
  notifyChannel?: string;
  resilience?: TransportBaseOptions;
}

export class PostgresTransport extends TransportBase implements RealtimeTransport {
  private readonly eventsTable: string;
  private readonly subscribersTable: string;
  private readonly notifyChannel: string;
  private readonly countersTable: string;
  private readonly snapshotsTable: string;
  private readonly dlqTable: string;
  private listenStarted = false;
  private listenPromise: Promise<void> | null = null;
  private readonly notificationHandler = (message: { channel?: string; payload?: string }) => {
    if (message.channel !== this.notifyChannel || !message.payload) {
      return;
    }

    try {
      this.fanout(JSON.parse(message.payload) as RealtimeEnvelope);
    } catch {
      this.metrics.increment('errors');
    }
  };

  constructor(private readonly options: PostgresTransportOptions) {
    super(options.resilience, 'postgres');
    this.eventsTable = assertPgIdentifier(options.eventsTable ?? 'realtime_events', 'eventsTable');
    this.subscribersTable = assertPgIdentifier(options.subscribersTable ?? 'realtime_subscribers', 'subscribersTable');
    this.countersTable = `${this.eventsTable}_counters`;
    this.snapshotsTable = `${this.eventsTable}_snapshots`;
    this.dlqTable = `${this.eventsTable}_dlq`;
    this.notifyChannel = assertPgIdentifier(options.notifyChannel ?? 'realtime_events', 'notifyChannel');
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const published = await this.runMiddleware<RealtimeEnvelope<TPayload>>(
      { action: 'publish', room: input.room, event: input },
      async (ctx) => {
        const eventInput = (ctx.event ?? input) as PublishInput<TPayload> | RealtimeEnvelope<TPayload>;
        const eventId = eventInput.id || undefined;
        const emittedAt = eventInput.emittedAt ? new Date(eventInput.emittedAt).toISOString() : new Date().toISOString();
        const ttlMs = 'ttlMs' in eventInput ? eventInput.ttlMs : undefined;
        const expiresAt = eventInput.expiresAt
          ? new Date(eventInput.expiresAt).toISOString()
          : new Date(new Date(emittedAt).getTime() + (ttlMs ?? 24 * 60 * 60 * 1000)).toISOString();
        const envelope = this.createEnvelope({ ...eventInput, id: eventId, emittedAt, expiresAt } as any, eventInput.sequence ?? 0, 'postgres');
        validateRealtimeEnvelope(envelope);

        const result = await this.withRetry(async () => this.options.client.query<any>(
          `WITH existing AS (
         SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata
         FROM ${this.eventsTable}
         WHERE id = $1
         LIMIT 1
       ),
       next_sequence AS (
         INSERT INTO ${this.countersTable}(room, sequence)
         SELECT $2, 1
         WHERE NOT EXISTS (SELECT 1 FROM existing)
         ON CONFLICT (room) DO UPDATE SET sequence = ${this.countersTable}.sequence + 1
         RETURNING sequence
       ),
       candidate AS (
         SELECT COALESCE($3::bigint, (SELECT sequence FROM next_sequence)) AS sequence
         WHERE NOT EXISTS (SELECT 1 FROM existing)
       ),
       inserted AS (
         INSERT INTO ${this.eventsTable}
           (id, room, sequence, type, emitted_at, expires_at, payload, metadata)
         SELECT $1, $2, sequence, $4, $5, $6, $7::jsonb, $8::jsonb
         FROM candidate
         ON CONFLICT (room, sequence) DO NOTHING
         RETURNING id, room, sequence, type, emitted_at, expires_at, payload, metadata
       ),
       chosen AS (
         SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata FROM existing
         UNION ALL
         SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata FROM inserted
         LIMIT 1
       ),
       notified AS (
         SELECT pg_notify($9, row_to_json(chosen)::text)
         FROM chosen
         WHERE NOT EXISTS (SELECT 1 FROM existing)
       )
       SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata
       FROM chosen`,
            [
              envelope.id,
              envelope.room,
              eventInput.sequence ?? null,
              envelope.type,
              envelope.emittedAt,
              envelope.expiresAt,
              JSON.stringify(envelope.payload),
              JSON.stringify(envelope.metadata ?? {}),
              this.notifyChannel,
            ],
        ), { provider: 'postgres', operation: 'publish', room: envelope.room });
        const publishedEnvelope = rowToEnvelope<TPayload>(result.rows[0]);
        this.metrics.increment('published');
        this.fanout(publishedEnvelope);
        ctx.envelope = publishedEnvelope;
        return publishedEnvelope;
      },
    );
    if (!published) {
      throw new Error('Realtime publish was blocked by middleware');
    }
    return published;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    void this.ensureListening();
    const { unsubscribe, entry } = this.addListenerEntry(options, handler);
    void this.deliverCatchUp(options, entry);
    return unsubscribe;
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    const startedAt = Date.now();
    await this.runMiddleware({ action: 'replay', room: options.room });
    const params: unknown[] = [options.room, options.fromSequence];
    const filters = ['room = $1', 'sequence > $2'];
    if (options.toSequence !== undefined) {
      params.push(options.toSequence);
      filters.push(`sequence <= $${params.length}`);
    }
    if (options.eventTypes?.length) {
      params.push(options.eventTypes);
      filters.push(`type = ANY($${params.length})`);
    }
    params.push(options.limit ?? 500);

    const result = await this.options.client.query<any>(
      `SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata
       FROM ${this.eventsTable}
       WHERE ${filters.join(' AND ')}
       ORDER BY sequence ASC
       LIMIT $${params.length}`,
      params,
    );

    const events = result.rows.map((row) => ({
        id: row.id,
        room: row.room,
        sequence: Number(row.sequence),
        type: row.type,
        emittedAt: new Date(row.emitted_at).toISOString(),
        expiresAt: new Date(row.expires_at).toISOString(),
        payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
        metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
      }));
    this.metrics.increment('replayed', events.length);
    this.metrics.set('replayLatencyMs', Date.now() - startedAt);

    return { events };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.runMiddleware({ action: 'ack', room, subscriberId });
    await this.options.client.query(
      `INSERT INTO ${this.subscribersTable}(room, subscriber_id, last_ack_sequence, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (room, subscriber_id)
      DO UPDATE SET last_ack_sequence = GREATEST(${this.subscribersTable}.last_ack_sequence, $3), updated_at = NOW()`,
      [room, subscriberId, sequence],
    );
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
      await this.options.client.query(
        `INSERT INTO ${this.snapshotsTable}(room, last_sequence, state, created_at)
         VALUES ($1, $2, $3::jsonb, $4)
         ON CONFLICT (room)
         DO UPDATE SET last_sequence = $2, state = $3::jsonb, created_at = $4`,
        [room, snapshot.lastSequence, JSON.stringify(snapshot.state), snapshot.createdAt],
      );
      ctx.result = snapshot;
    });
    return snapshot;
  }

  async getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    let snapshot: Snapshot<TState> | null = null;
    await this.runMiddleware({ action: 'getSnapshot', room, snapshot: { room } }, async (ctx) => {
      const result = await this.options.client.query<any>(
        `SELECT room, last_sequence, state, created_at
         FROM ${this.snapshotsTable}
         WHERE room = $1
         LIMIT 1`,
        [room],
      );
      const row = result.rows[0];
      snapshot = row
        ? {
          room: row.room,
          lastSequence: Number(row.last_sequence),
          state: typeof row.state === 'string' ? JSON.parse(row.state) : row.state,
          createdAt: new Date(row.created_at).toISOString(),
        }
        : null;
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
    if (!subscriberId) {
      return 0;
    }
    const result = await this.options.client.query<{ last_ack_sequence: number }>(
      `SELECT last_ack_sequence
       FROM ${this.subscribersTable}
       WHERE room = $1 AND subscriber_id = $2
       LIMIT 1`,
      [room, subscriberId],
    );
    return Number(result.rows[0]?.last_ack_sequence ?? 0);
  }

  async close(): Promise<void> {
    this.clearListeners();
    const listener = this.options.listenerClient;
    if (listener && this.listenStarted) {
      listener.off?.('notification', this.notificationHandler);
      listener.removeListener?.('notification', this.notificationHandler);
      await listener.query(`UNLISTEN ${this.notifyChannel}`);
      this.listenStarted = false;
    }
  }

  async health(): Promise<RealtimeHealth> {
    try {
      await this.options.client.query('SELECT 1');
      const listenerReady = !this.options.listenerClient || this.listenStarted || !this.listeners.size;
      return {
        provider: 'postgres',
        status: listenerReady ? 'healthy' : 'degraded',
        details: {
          listenerReady,
          activeRooms: this.snapshotMetrics().activeRooms,
          activeListeners: this.snapshotMetrics().activeListeners,
        },
      };
    } catch (error) {
      return {
        provider: 'postgres',
        status: 'unhealthy',
        details: {
          error: error instanceof Error ? error.message : String(error),
        },
      };
    }
  }

  private async ensureListening(): Promise<void> {
    const listener = this.options.listenerClient;
    if (!listener || this.listenStarted) {
      return;
    }
    if (this.listenPromise) {
      return this.listenPromise;
    }

    this.listenPromise = (async () => {
      listener.on?.('notification', this.notificationHandler);
      try {
        await listener.query(`LISTEN ${this.notifyChannel}`);
        this.listenStarted = true;
      } catch (error) {
        listener.off?.('notification', this.notificationHandler);
        listener.removeListener?.('notification', this.notificationHandler);
        throw error;
      } finally {
        this.listenPromise = null;
      }
    })();

    return this.listenPromise;
  }

  private async findById<TPayload>(id: string): Promise<RealtimeEnvelope<TPayload> | null> {
    const result = await this.options.client.query<any>(
      `SELECT id, room, sequence, type, emitted_at, expires_at, payload, metadata
       FROM ${this.eventsTable}
       WHERE id = $1
       LIMIT 1`,
      [id],
    );
    const row = result.rows[0];
    if (!row) {
      return null;
    }

    return rowToEnvelope<TPayload>(row);
  }

  protected async writeDLQ<TPayload>(room: string, event: RealtimeEnvelope<TPayload>, error: string, attempts: number): Promise<void> {
    await this.runMiddleware({
      action: 'dlqWrite',
      room,
      envelope: event,
      dlq: { room, originalEvent: event, attempts, reason: error },
    }, async () => {
      await this.options.client.query(
        `INSERT INTO ${this.dlqTable}(id, room, original_event, error, attempts, failed_at)
         VALUES ($1, $2, $3::jsonb, $4, $5, NOW())`,
        [`dlq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`, room, JSON.stringify(event), error, attempts],
      );
    });
  }

  protected async listDLQ<TPayload>(room: string, options: DLQListOptions): Promise<DLQEvent<TPayload>[]> {
    const params: unknown[] = [room];
    const filters = ['room = $1'];
    if (options.fromFailedAt) {
      params.push(new Date(options.fromFailedAt).toISOString());
      filters.push(`failed_at >= $${params.length}`);
    }
    params.push(options.limit ?? 100);
    const result = await this.options.client.query<any>(
      `SELECT id, room, original_event, error, attempts, failed_at
       FROM ${this.dlqTable}
       WHERE ${filters.join(' AND ')}
       ORDER BY failed_at ASC
       LIMIT $${params.length}`,
      params,
    );
    return result.rows.map((row) => ({
      id: row.id,
      room: row.room,
      originalEvent: typeof row.original_event === 'string' ? JSON.parse(row.original_event) : row.original_event,
      error: row.error,
      attempts: Number(row.attempts),
      failedAt: new Date(row.failed_at).toISOString(),
    }));
  }

  protected async clearDLQ(room: string, options: DLQListOptions): Promise<number> {
    const params: unknown[] = [room];
    const filters = ['room = $1'];
    if (options.fromFailedAt) {
      params.push(new Date(options.fromFailedAt).toISOString());
      filters.push(`failed_at >= $${params.length}`);
    }
    const result = await this.options.client.query<{ id: string }>(
      `DELETE FROM ${this.dlqTable}
       WHERE ${filters.join(' AND ')}
       RETURNING id`,
      params,
    );
    return result.rows.length;
  }
}

function rowToEnvelope<TPayload>(row: any): RealtimeEnvelope<TPayload> {
  if (!row) {
    throw new Error('PostgreSQL realtime publish did not return an event');
  }

  return {
    id: row.id,
    room: row.room,
    sequence: Number(row.sequence),
    type: row.type,
    emittedAt: new Date(row.emitted_at).toISOString(),
    expiresAt: new Date(row.expires_at).toISOString(),
    payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
    metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
  };
}

function assertPgIdentifier(value: string, field: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(value)) {
    throw new Error(`Invalid PostgreSQL identifier for ${field}: ${value}`);
  }
  return value;
}
