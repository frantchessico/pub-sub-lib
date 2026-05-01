export type RealtimeStorageProvider = 'postgres' | 'mongo' | 'redis' | 'firestore';

export type Unsubscribe = () => void | Promise<void>;

export interface RealtimeEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  room: string;
  sequence: number;
  emittedAt: string;
  expiresAt: string;
  payload: TPayload;
  metadata?: {
    provider?: RealtimeStorageProvider;
    producer?: string;
    correlationId?: string;
    traceId?: string;
    tenantId?: string;
    [key: string]: unknown;
  };
}

export interface PublishInput<TPayload = unknown> {
  type: string;
  room: string;
  payload: TPayload;
  id?: string;
  sequence?: number;
  emittedAt?: string | Date;
  expiresAt?: string | Date;
  ttlMs?: number;
  metadata?: RealtimeEnvelope['metadata'];
}

export interface SubscribeOptions {
  room: string;
  subscriberId?: string;
  eventTypes?: string[];
  fromSequence?: number;
  limit?: number;
}

export interface ReplayOptions {
  room: string;
  fromSequence: number;
  toSequence?: number;
  eventTypes?: string[];
  limit?: number;
}

export interface ReplayResult<TPayload = unknown> {
  events: RealtimeEnvelope<TPayload>[];
}

export type RealtimeEventHandler<TPayload = unknown> = (
  event: RealtimeEnvelope<TPayload>,
) => void | Promise<void>;

export interface RealtimeTransport {
  publish<TPayload = unknown>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>>;
  subscribe<TPayload = unknown>(
    options: SubscribeOptions,
    handler: RealtimeEventHandler<TPayload>,
  ): Unsubscribe;
  replay<TPayload = unknown>(options: ReplayOptions): Promise<ReplayResult<TPayload>>;
  ack(room: string, sequence: number, subscriberId: string): Promise<void>;
  close(): Promise<void>;
}

export interface RealtimeMetrics {
  published: number;
  received: number;
  acked: number;
  gapsDetected: number;
  errors: number;
}

export interface RoomAccessContext {
  subscriberId: string;
  token?: string;
  claims?: Record<string, unknown>;
  room: string;
  action: 'subscribe' | 'publish' | 'replay' | 'ack';
}

export type RoomAuthorizer = (context: RoomAccessContext) => boolean | Promise<boolean>;

