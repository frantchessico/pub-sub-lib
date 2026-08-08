import { InvalidEnvelopeError } from './errors';
import { parseRoom } from './rooms';
import type { PublishRealtimeEvent, RealtimeEnvelope } from './transport/types';

export const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

export function createEventId(prefix = 'evt'): string {
  const random = Math.random().toString(36).slice(2, 12);
  return `${prefix}_${Date.now().toString(36)}_${random}`;
}

export function toIsoDate(value?: string | Date): string {
  if (!value) {
    return new Date().toISOString();
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    throw new InvalidEnvelopeError(`Invalid date value: ${value}`);
  }

  return parsed.toISOString();
}

export function createEnvelope<TPayload>(
  event: PublishRealtimeEvent<TPayload>,
  sequence: number,
  options?: {
    eventId?: string;
    ttlMs?: number;
    emittedAt?: Date;
  },
): RealtimeEnvelope<TPayload> {
  const parsedRoom = parseRoom(event.room);
  const emittedAt = options?.emittedAt ?? new Date();
  const ttlMs = options?.ttlMs ?? DEFAULT_TTL_MS;
  const expiresAt = new Date(emittedAt.getTime() + ttlMs);

  const envelope: RealtimeEnvelope<TPayload> = {
    id: options?.eventId ?? createEventId(),
    type: event.type,
    scope: parsedRoom.scope,
    room: event.room,
    entityId: event.entityId,
    action: event.action,
    sequence,
    version: event.version,
    updatedAt: toIsoDate(event.updatedAt),
    emittedAt: emittedAt.toISOString(),
    expiresAt: expiresAt.toISOString(),
    payload: event.payload,
    metadata: {
      ...event.metadata,
    },
  };

  validateEnvelope(envelope);
  return envelope;
}

export function validateEnvelope<TPayload>(event: RealtimeEnvelope<TPayload>): void {
  if (!event.id) {
    throw new InvalidEnvelopeError('Realtime event must have an id');
  }

  if (!event.type) {
    throw new InvalidEnvelopeError('Realtime event must have a type', { id: event.id });
  }

  parseRoom(event.room);

  if (!Number.isFinite(event.sequence) || event.sequence < 0) {
    throw new InvalidEnvelopeError('Realtime event must have a non-negative sequence', {
      id: event.id,
      sequence: event.sequence,
    });
  }

  assertIsoDate(event.updatedAt, 'updatedAt', event.id);
  assertIsoDate(event.emittedAt, 'emittedAt', event.id);
  assertIsoDate(event.expiresAt, 'expiresAt', event.id);
}

export function isExpired(event: RealtimeEnvelope, now = new Date()): boolean {
  return new Date(event.expiresAt).getTime() <= now.getTime();
}

function assertIsoDate(value: string, field: string, eventId: string): void {
  if (!value || Number.isNaN(new Date(value).getTime())) {
    throw new InvalidEnvelopeError(`Realtime event has invalid ${field}`, { id: eventId, value });
  }
}
