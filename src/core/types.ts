export type RealtimeStorageProvider = 'postgres' | 'mongo' | 'redis';

export type Unsubscribe = () => void | Promise<void>;

export interface RealtimeEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  room: string;
  sequence: number;
  emittedAt: string;
  expiresAt: string;
  payload: TPayload;
  encrypted?: boolean;
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
  encrypted?: boolean;
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
  catchUp?: boolean | SubscribeCatchUpOptions;
  retry?: RetryOptions;
  flowControl?: FlowControlOptions;
  sync?: 'events' | 'snapshot';
}

export interface SubscribeCatchUpOptions {
  batchSize?: number;
  maxBatches?: number;
}

export interface RetryOptions {
  attempts?: number;
  strategy?: 'fixed' | 'linear' | 'exponential';
  baseDelayMs?: number;
}

export interface FlowControlOptions {
  maxInFlight?: number;
  strategy?: 'pause' | 'drop' | 'buffer';
  maxBufferSize?: number;
}

export interface ReplayOptions {
  room: string;
  fromSequence: number;
  toSequence?: number;
  eventTypes?: string[];
  limit?: number;
}

export interface CatchUpOptions {
  room: string;
  subscriberId?: string;
  fromSequence?: number;
  toSequence?: number;
  eventTypes?: string[];
  batchSize?: number;
  maxBatches?: number;
}

export type StreamReplayOptions = CatchUpOptions;

export interface SnapshotInput<TState = unknown> {
  lastSequence: number;
  state: TState;
  createdAt?: string | Date;
}

export interface Snapshot<TState = unknown> {
  room: string;
  lastSequence: number;
  state: TState;
  createdAt: string;
}

export interface DLQEvent<TPayload = unknown> {
  id: string;
  room: string;
  originalEvent: RealtimeEnvelope<TPayload>;
  error: string;
  attempts: number;
  failedAt: string;
}

export interface DLQListOptions {
  limit?: number;
  fromFailedAt?: string | Date;
}

export interface DLQReplayOptions extends DLQListOptions {
  deleteOnSuccess?: boolean;
}

export interface DLQHandle {
  list<TPayload = unknown>(options?: DLQListOptions): Promise<DLQEvent<TPayload>[]>;
  replay<TPayload = unknown>(options?: DLQReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>>;
  clear(options?: DLQListOptions): Promise<number>;
}

export interface PublishEphemeralInput<TPayload = unknown> {
  room: string;
  type: string;
  payload: TPayload;
  id?: string;
  emittedAt?: string | Date;
  metadata?: RealtimeEnvelope['metadata'];
}

export interface EphemeralEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  room: string;
  emittedAt: string;
  payload: TPayload;
  metadata?: RealtimeEnvelope['metadata'];
}

export interface PresenceUser {
  userId: string;
  metadata?: Record<string, unknown>;
  lastSeenAt: string;
}

export type MiddlewareAction =
  | 'publish'
  | 'subscribe'
  | 'deliver'
  | 'replay'
  | 'catchUp'
  | 'ack'
  | 'snapshot'
  | 'getSnapshot'
  | 'ephemeralPublish'
  | 'ephemeralDeliver'
  | 'presenceEnter'
  | 'presenceLeave'
  | 'dlqWrite'
  | 'dlqReplay'
  | 'close';

export interface ReplayMiddlewareContext {
  fromSequence?: number;
  toSequence?: number;
  limit?: number;
  eventTypes?: string[];
}

export interface CatchUpMiddlewareContext {
  fromSequence?: number;
  toSequence?: number;
  batchSize: number;
  maxBatches?: number;
  currentBatch?: number;
}

export interface AckMiddlewareContext {
  room: string;
  sequence: number;
  subscriberId: string;
}

export interface SnapshotMiddlewareContext {
  room: string;
  lastSequence?: number;
  state?: unknown;
}

export interface PresenceMiddlewareContext {
  room: string;
  userId?: string;
  metadata?: Record<string, unknown>;
}

export interface EphemeralMiddlewareContext {
  room: string;
  type: string;
  payload: unknown;
}

export interface DLQMiddlewareContext {
  room: string;
  originalEvent?: RealtimeEnvelope;
  attempts?: number;
  reason?: string;
}

export interface RealtimeMiddlewareContext<TPayload = unknown> {
  action: MiddlewareAction;
  provider: RealtimeStorageProvider | 'hybrid';
  startedAt: number;
  metadata: Record<string, unknown>;
  requestId?: string;
  traceId?: string;
  tenantId?: string;
  room?: string;
  event?: PublishInput<TPayload> | RealtimeEnvelope<TPayload> | PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>;
  envelope?: RealtimeEnvelope<TPayload>;
  subscriberId?: string;
  sequence?: number;
  replay?: ReplayMiddlewareContext;
  catchUp?: CatchUpMiddlewareContext;
  ack?: AckMiddlewareContext;
  snapshot?: SnapshotMiddlewareContext;
  presence?: PresenceMiddlewareContext;
  ephemeral?: EphemeralMiddlewareContext;
  dlq?: DLQMiddlewareContext;
  result?: unknown;
  error?: unknown;
  set<K extends string>(key: K, value: unknown): void;
  get<T = unknown>(key: string): T | undefined;
}

export type RealtimeMiddleware = (
  context: RealtimeMiddlewareContext,
  next: () => Promise<void>,
) => Promise<void> | void;

export interface ReplayResult<TPayload = unknown> {
  events: RealtimeEnvelope<TPayload>[];
}

export type RealtimeEventHandler<TPayload = unknown> = (
  event: RealtimeEnvelope<TPayload>,
) => void | Promise<void>;

export interface RealtimeTransport {
  use(middleware: RealtimeMiddleware): void;
  publish<TPayload = unknown>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>>;
  publishEphemeral<TPayload = unknown>(event: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>): Promise<EphemeralEnvelope<TPayload>>;
  subscribe<TPayload = unknown>(
    options: SubscribeOptions,
    handler: RealtimeEventHandler<TPayload>,
  ): Unsubscribe;
  subscribeEphemeral<TPayload = unknown>(
    options: Pick<SubscribeOptions, 'room' | 'eventTypes' | 'flowControl'>,
    handler: (event: EphemeralEnvelope<TPayload>) => void | Promise<void>,
  ): Unsubscribe;
  replay<TPayload = unknown>(options: ReplayOptions): Promise<ReplayResult<TPayload>>;
  catchUp<TPayload = unknown>(options: CatchUpOptions): AsyncIterable<RealtimeEnvelope<TPayload>>;
  streamReplay<TPayload = unknown>(options: StreamReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>>;
  snapshot<TState = unknown>(room: string, snapshot: SnapshotInput<TState>): Promise<Snapshot<TState>>;
  getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null>;
  dlq(room: string): DLQHandle;
  ack(room: string, sequence: number, subscriberId: string): Promise<void>;
  snapshotMetrics?(): RealtimeMetrics;
  health?(): Promise<RealtimeHealth> | RealtimeHealth;
  close(): Promise<void>;
}

export interface RealtimeMetrics {
  published: number;
  received: number;
  acked: number;
  gapsDetected: number;
  errors: number;
  replayed: number;
  duplicatesDropped: number;
  activeRooms: number;
  activeListeners: number;
  averageDeliveryLagMs: number;
  retryCount: number;
  dlqSize: number;
  snapshotUsage: number;
  replayLatencyMs: number;
}

export type RealtimeHealthStatus = 'healthy' | 'degraded' | 'unhealthy';

export interface RealtimeHealth {
  status: RealtimeHealthStatus;
  provider: RealtimeStorageProvider | 'hybrid';
  details?: Record<string, unknown>;
}

export interface RoomAccessContext {
  subscriberId: string;
  token?: string;
  claims?: Record<string, unknown>;
  room: string;
  action: 'subscribe' | 'publish' | 'replay' | 'ack';
}

export type RoomAuthorizer = (context: RoomAccessContext) => boolean | Promise<boolean>;
