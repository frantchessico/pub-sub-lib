export type RealtimeScope = string;

export type RealtimeAction =
  | 'created'
  | 'updated'
  | 'deleted'
  | 'snapshot'
  | 'read'
  | 'typing'
  | 'joined'
  | 'left'
  | 'resync_required';

export type RealtimeProvider = 'postgres' | 'mongo' | 'redis' | 'socket.io';

export type RealtimeApp = 'client' | 'vendor' | 'driver' | 'admin' | 'backend';

export type Unsubscribe = () => void | Promise<void>;

export interface RealtimeEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  scope: RealtimeScope;
  room: string;
  entityId?: string;
  action: RealtimeAction;
  sequence: number;
  version?: number;
  updatedAt: string;
  emittedAt: string;
  expiresAt: string;
  payload: TPayload;
  metadata?: {
    producer?: string;
    provider?: RealtimeProvider;
    correlationId?: string;
    traceId?: string;
    tenantId?: string;
    [key: string]: unknown;
  };
}

export interface PublishRealtimeEvent<TPayload = unknown> {
  room: string;
  type: string;
  entityId?: string;
  action: RealtimeAction;
  version?: number;
  updatedAt?: string | Date;
  payload: TPayload;
  metadata?: RealtimeEnvelope['metadata'];
}

export interface PublishOptions {
  ttlMs?: number;
  eventId?: string;
  sequence?: number;
  idempotencyKey?: string;
}

export interface SubscribeOptions {
  room: string;
  subscriberId?: string;
  eventTypes?: string[];
  from?: 'cursor' | 'now' | 'beginning' | { sequence: number };
  autoAck?: boolean;
  ackMode?: 'before-callback' | 'after-callback' | 'manual';
  includeExpired?: boolean;
  limit?: number;
}

export interface ReplayOptions {
  room: string;
  subscriberId?: string;
  fromSequence: number;
  toSequence?: number;
  eventTypes?: string[];
  includeExpired?: boolean;
  limit?: number;
}

export interface ReplayResult<TPayload = unknown> {
  events: RealtimeEnvelope<TPayload>[];
  hasGap: boolean;
  lastSequence: number;
  resyncRequired: boolean;
}

export interface RealtimeLogger {
  debug?(message: string, context?: Record<string, unknown>): void;
  info?(message: string, context?: Record<string, unknown>): void;
  warn?(message: string, context?: Record<string, unknown>): void;
  error?(message: string, context?: Record<string, unknown>): void;
}

export interface SubscriptionAck {
  ack(): Promise<void>;
  nack(error?: unknown): Promise<void>;
}

export interface RealtimeRoomDocument {
  room: string;
  scope: RealtimeScope;
  resourceId?: string;
  createdAt: unknown;
  updatedAt: unknown;
  lastSequence: number;
  eventCount?: number;
}

export interface RealtimeEventDocument<TPayload = unknown> {
  id: string;
  room: string;
  scope: RealtimeScope;
  type: string;
  entityId?: string;
  action: RealtimeAction;
  sequence: number;
  version?: number;
  updatedAt: unknown;
  emittedAt: unknown;
  expiresAt: unknown;
  payload: TPayload;
  metadata?: Record<string, unknown>;
}

export interface RealtimeSubscriberDocument {
  subscriberId: string;
  room: string;
  lastAckSequence: number;
  lastSeenSequence: number;
  joinedAt: unknown;
  lastSeenAt: unknown;
  status: 'active' | 'idle' | 'closed';
  app?: RealtimeApp;
  deviceId?: string;
}
