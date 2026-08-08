import type { EphemeralEnvelope, PresenceUser, PublishEphemeralInput, PublishInput, RealtimeEnvelope, Snapshot } from '../core/types';
import { InvalidProtocolMessageError } from '../errors';

export type ClientMessage =
  | { type: 'init'; subscriberId: string; token?: string }
  | { type: 'subscribe'; room: string | string[]; eventTypes?: string[]; fromSequence?: number; sync?: 'events' | 'snapshot' }
  | { type: 'unsubscribe'; room: string }
  | { type: 'publish'; event: PublishInput | RealtimeEnvelope }
  | { type: 'publish_ephemeral'; event: PublishEphemeralInput | EphemeralEnvelope }
  | { type: 'presence_enter'; room: string; user: Omit<PresenceUser, 'lastSeenAt'> & { lastSeenAt?: string } }
  | { type: 'presence_leave'; room: string; userId?: string }
  | { type: 'ack'; room: string; sequence: number }
  | { type: 'replay_request'; room: string; fromSequence: number; toSequence?: number; eventTypes?: string[]; limit?: number };

export type ServerMessage =
  | { type: 'ready'; subscriberId: string }
  | { type: 'event'; data: RealtimeEnvelope }
  | { type: 'ephemeral'; data: EphemeralEnvelope }
  | { type: 'snapshot'; room: string; snapshot: Snapshot | null }
  | { type: 'presence'; room: string; users: PresenceUser[] }
  | { type: 'replay'; room: string; events: RealtimeEnvelope[] }
  | { type: 'subscribed'; room: string }
  | { type: 'unsubscribed'; room: string }
  | { type: 'ack'; room: string; sequence: number }
  | { type: 'error'; message: string; code?: string };

export interface WebSocketLike {
  send(data: string, callback?: (error?: Error) => void): unknown;
  ping?(): unknown;
  terminate?(): unknown;
  close?(code?: number, reason?: string): unknown;
  on?(event: string, handler: (...args: any[]) => void): unknown;
  off?(event: string, handler: (...args: any[]) => void): unknown;
  readyState?: number;
  bufferedAmount?: number;
}

export function encodeServerMessage(message: ServerMessage): string {
  return JSON.stringify(message);
}

export function decodeClientMessage(raw: string | Buffer | ArrayBuffer): ClientMessage {
  const text = typeof raw === 'string' ? raw : Buffer.from(raw as any).toString('utf8');
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new InvalidProtocolMessageError('Realtime message must be valid JSON', {
      error: error instanceof Error ? error.message : String(error),
    });
  }

  return assertClientMessage(parsed);
}

export function assertClientMessage(value: unknown): ClientMessage {
  if (!isObject(value) || typeof value.type !== 'string') {
    throw new InvalidProtocolMessageError('Realtime message must include a type');
  }

  switch (value.type) {
    case 'init':
      assertString(value.subscriberId, 'subscriberId');
      optionalString(value.token, 'token');
      return value as ClientMessage;
    case 'subscribe':
      assertRoomOrRooms(value.room, 'room');
      optionalStringArray(value.eventTypes, 'eventTypes');
      optionalNonNegativeInteger(value.fromSequence, 'fromSequence');
      optionalSync(value.sync);
      return value as ClientMessage;
    case 'unsubscribe':
      assertString(value.room, 'room');
      return value as ClientMessage;
    case 'publish':
      assertPublishEvent(value.event);
      return value as ClientMessage;
    case 'publish_ephemeral':
      assertPublishEvent(value.event);
      return value as ClientMessage;
    case 'presence_enter':
      assertString(value.room, 'room');
      assertPresenceUser(value.user);
      return value as ClientMessage;
    case 'presence_leave':
      assertString(value.room, 'room');
      optionalString(value.userId, 'userId');
      return value as ClientMessage;
    case 'ack':
      assertString(value.room, 'room');
      assertNonNegativeInteger(value.sequence, 'sequence');
      return value as ClientMessage;
    case 'replay_request':
      assertString(value.room, 'room');
      assertNonNegativeInteger(value.fromSequence, 'fromSequence');
      optionalNonNegativeInteger(value.toSequence, 'toSequence');
      optionalStringArray(value.eventTypes, 'eventTypes');
      optionalPositiveInteger(value.limit, 'limit');
      return value as ClientMessage;
    default:
      throw new InvalidProtocolMessageError(`Unknown realtime message type: ${value.type}`);
  }
}

function assertRoomOrRooms(value: unknown, field: string): void {
  if (Array.isArray(value)) {
    optionalStringArray(value, field);
    return;
  }
  assertString(value, field);
}

function assertPublishEvent(value: unknown): void {
  if (!isObject(value)) {
    throw new InvalidProtocolMessageError('publish.event must be an object');
  }
  assertString(value.room, 'event.room');
  assertString(value.type, 'event.type');
  if (!('payload' in value)) {
    throw new InvalidProtocolMessageError('publish.event must include payload');
  }
}

function assertPresenceUser(value: unknown): void {
  if (!isObject(value)) {
    throw new InvalidProtocolMessageError('presence_enter.user must be an object');
  }
  assertString(value.userId, 'user.userId');
  if (value.metadata !== undefined && !isObject(value.metadata)) {
    throw new InvalidProtocolMessageError('presence_enter.user.metadata must be an object');
  }
}

function isObject(value: unknown): value is Record<string, any> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function assertString(value: unknown, field: string): void {
  if (typeof value !== 'string' || !value) {
    throw new InvalidProtocolMessageError(`Realtime message field ${field} must be a non-empty string`);
  }
}

function optionalString(value: unknown, field: string): void {
  if (value !== undefined && typeof value !== 'string') {
    throw new InvalidProtocolMessageError(`Realtime message field ${field} must be a string`);
  }
}

function optionalStringArray(value: unknown, field: string): void {
  if (value === undefined) {
    return;
  }
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string' || !item)) {
    throw new InvalidProtocolMessageError(`Realtime message field ${field} must be a string array`);
  }
}

function assertNonNegativeInteger(value: unknown, field: string): void {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw new InvalidProtocolMessageError(`Realtime message field ${field} must be a non-negative integer`);
  }
}

function optionalNonNegativeInteger(value: unknown, field: string): void {
  if (value !== undefined) {
    assertNonNegativeInteger(value, field);
  }
}

function optionalPositiveInteger(value: unknown, field: string): void {
  if (value !== undefined && (!Number.isSafeInteger(value) || Number(value) <= 0)) {
    throw new InvalidProtocolMessageError(`Realtime message field ${field} must be a positive integer`);
  }
}

function optionalSync(value: unknown): void {
  if (value !== undefined && value !== 'events' && value !== 'snapshot') {
    throw new InvalidProtocolMessageError('Realtime message field sync must be "events" or "snapshot"');
  }
}
