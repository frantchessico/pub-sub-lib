import { InvalidEnvelopeError } from '../errors';
import { parseRoom } from '../rooms';
import type { PublishInput, RealtimeEnvelope, RealtimeStorageProvider } from './types';

export const REALTIME_DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;

export function createRealtimeEventId(prefix = 'evt'): string {
  return `${prefix}_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 12)}`;
}

export function toRealtimeIsoDate(value?: string | Date, fallback = new Date()): string {
  const date = value ? new Date(value) : fallback;
  if (Number.isNaN(date.getTime())) {
    throw new InvalidEnvelopeError(`Invalid date value: ${String(value)}`);
  }
  return date.toISOString();
}

export function createRealtimeEnvelope<TPayload>(
  input: PublishInput<TPayload> | RealtimeEnvelope<TPayload>,
  sequence: number,
  provider?: RealtimeStorageProvider,
): RealtimeEnvelope<TPayload> {
  const emittedAt = toRealtimeIsoDate(input.emittedAt);
  const ttlMs = 'ttlMs' in input ? input.ttlMs : undefined;
  const expiresAt = input.expiresAt
    ? toRealtimeIsoDate(input.expiresAt)
    : new Date(new Date(emittedAt).getTime() + (ttlMs ?? REALTIME_DEFAULT_TTL_MS)).toISOString();

  const envelope: RealtimeEnvelope<TPayload> = {
    id: input.id || createRealtimeEventId(),
    type: input.type,
    room: input.room,
    sequence,
    emittedAt,
    expiresAt,
    payload: input.payload,
    metadata: {
      ...input.metadata,
      provider: provider ?? input.metadata?.provider,
    },
  };

  validateRealtimeEnvelope(envelope);
  return envelope;
}

export function validateRealtimeEnvelope<TPayload>(event: RealtimeEnvelope<TPayload>): void {
  if (!event.id) {
    throw new InvalidEnvelopeError('Realtime envelope must have an id');
  }

  if (!event.type) {
    throw new InvalidEnvelopeError('Realtime envelope must have a type', { id: event.id });
  }

  parseRoom(event.room);
  validateSequence(event.sequence);
  assertIso(event.emittedAt, 'emittedAt', event.id);
  assertIso(event.expiresAt, 'expiresAt', event.id);
}

export function validateSequence(sequence: number): void {
  if (!Number.isSafeInteger(sequence) || sequence < 0) {
    throw new InvalidEnvelopeError('Realtime sequence must be a non-negative safe integer', { sequence });
  }
}

export function isRealtimeEnvelopeExpired(event: RealtimeEnvelope, now = new Date()): boolean {
  return new Date(event.expiresAt).getTime() <= now.getTime();
}

function assertIso(value: string, field: string, eventId: string): void {
  if (!value || Number.isNaN(new Date(value).getTime())) {
    throw new InvalidEnvelopeError(`Realtime envelope has invalid ${field}`, { id: eventId, value });
  }
}
