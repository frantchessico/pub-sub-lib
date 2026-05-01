import { Timestamp } from 'firebase/firestore';
import type { RealtimeEnvelope, RealtimeEventDocument } from '../transport/types';

export function envelopeToDocument<TPayload>(event: RealtimeEnvelope<TPayload>): RealtimeEventDocument<TPayload> {
  return stripUndefinedDeep({
    ...event,
    updatedAt: Timestamp.fromDate(new Date(event.updatedAt)),
    emittedAt: Timestamp.fromDate(new Date(event.emittedAt)),
    expiresAt: Timestamp.fromDate(new Date(event.expiresAt)),
  }) as RealtimeEventDocument<TPayload>;
}

export function documentToEnvelope<TPayload>(
  data: RealtimeEventDocument<TPayload>,
): RealtimeEnvelope<TPayload> {
  return {
    id: data.id,
    room: data.room,
    scope: data.scope,
    type: data.type,
    entityId: data.entityId,
    action: data.action,
    sequence: data.sequence,
    version: data.version,
    updatedAt: timestampToIso(data.updatedAt),
    emittedAt: timestampToIso(data.emittedAt),
    expiresAt: timestampToIso(data.expiresAt),
    payload: data.payload,
    metadata: data.metadata,
  };
}

export function timestampToIso(value: unknown): string {
  if (value instanceof Timestamp) {
    return value.toDate().toISOString();
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === 'string') {
    return new Date(value).toISOString();
  }

  if (value && typeof value === 'object' && 'toDate' in value && typeof (value as { toDate?: unknown }).toDate === 'function') {
    return (value as { toDate: () => Date }).toDate().toISOString();
  }

  return new Date().toISOString();
}

export function stripUndefinedDeep<T>(value: T): T {
  if (Array.isArray(value)) {
    return value.map((item) => (item === undefined ? null : stripUndefinedDeep(item))) as T;
  }

  if (
    value instanceof Date ||
    value instanceof Timestamp ||
    value === null ||
    typeof value !== 'object' ||
    !isPlainObject(value)
  ) {
    return value;
  }

  const output: Record<string, unknown> = {};
  Object.entries(value as Record<string, unknown>).forEach(([key, nestedValue]) => {
    if (nestedValue !== undefined) {
      output[key] = stripUndefinedDeep(nestedValue);
    }
  });

  return output as T;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}
