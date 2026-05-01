import { validateRealtimeEnvelope } from '../core/envelope';
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

export interface PostgresClientLike {
  query<T = any>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
}

export interface PostgresTransportOptions {
  client: PostgresClientLike;
  eventsTable?: string;
  subscribersTable?: string;
  notifyChannel?: string;
}

export class PostgresTransport extends TransportBase implements RealtimeTransport {
  private readonly eventsTable: string;
  private readonly subscribersTable: string;
  private readonly notifyChannel: string;

  constructor(private readonly options: PostgresTransportOptions) {
    super();
    this.eventsTable = options.eventsTable ?? 'realtime_events';
    this.subscribersTable = options.subscribersTable ?? 'realtime_subscribers';
    this.notifyChannel = options.notifyChannel ?? 'realtime_events';
  }

  async publish<TPayload>(input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    const room = input.room;
    const sequenceResult = await this.options.client.query<{ sequence: number }>(
      `INSERT INTO ${this.eventsTable}_counters(room, sequence)
       VALUES ($1, 1)
       ON CONFLICT (room) DO UPDATE SET sequence = ${this.eventsTable}_counters.sequence + 1
       RETURNING sequence`,
      [room],
    );
    const sequence = input.sequence ?? Number(sequenceResult.rows[0]?.sequence ?? 1);
    const envelope = this.createEnvelope(input, sequence, 'postgres');
    validateRealtimeEnvelope(envelope);

    await this.options.client.query(
      `INSERT INTO ${this.eventsTable}
       (id, room, sequence, type, emitted_at, expires_at, payload, metadata)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (room, sequence) DO NOTHING`,
      [
        envelope.id,
        envelope.room,
        envelope.sequence,
        envelope.type,
        envelope.emittedAt,
        envelope.expiresAt,
        JSON.stringify(envelope.payload),
        JSON.stringify(envelope.metadata ?? {}),
      ],
    );

    await this.options.client.query(`SELECT pg_notify($1, $2)`, [this.notifyChannel, JSON.stringify(envelope)]);
    this.fanout(envelope);
    return envelope;
  }

  subscribe<TPayload>(options: SubscribeOptions, handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    return this.addListener(options, handler);
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
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

    return {
      events: result.rows.map((row) => ({
        id: row.id,
        room: row.room,
        sequence: Number(row.sequence),
        type: row.type,
        emittedAt: new Date(row.emitted_at).toISOString(),
        expiresAt: new Date(row.expires_at).toISOString(),
        payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
        metadata: typeof row.metadata === 'string' ? JSON.parse(row.metadata) : row.metadata,
      })),
    };
  }

  async ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    await this.options.client.query(
      `INSERT INTO ${this.subscribersTable}(room, subscriber_id, last_ack_sequence, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (room, subscriber_id)
       DO UPDATE SET last_ack_sequence = GREATEST(${this.subscribersTable}.last_ack_sequence, $3), updated_at = NOW()`,
      [room, subscriberId, sequence],
    );
  }

  async close(): Promise<void> {
    this.clearListeners();
  }
}

