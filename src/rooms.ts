import { InvalidRoomError } from './errors';
import type { RealtimeScope } from './transport/types';

const ROOM_SEPARATOR = ':';
const ENCODED_SEPARATOR = '__';
const SCOPE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

export const room = {
  user(userId: string): string {
    return buildRoom('user', userId);
  },

  vendor(vendorId: string): string {
    return buildRoom('vendor', vendorId);
  },

  driver(driverUserId: string): string {
    return buildRoom('driver', driverUserId);
  },

  admin(): 'admin:realtime' {
    return 'admin:realtime';
  },

  chat(conversationId: string): string {
    return buildRoom('chat', conversationId);
  },

  tracking(topic: string): string {
    return buildRoom('tracking', topic);
  },
};

export interface ParsedRoom {
  scope: string;
  resourceId: string;
}

export function parseRoom(value: string): ParsedRoom {
  if (typeof value !== 'string' || !value.trim()) {
    throw new InvalidRoomError(String(value));
  }

  const [scope, ...resourceParts] = value.split(ROOM_SEPARATOR);
  const resourceId = resourceParts.join(ROOM_SEPARATOR);

  if (!isRealtimeScope(scope)) {
    throw new InvalidRoomError(value);
  }

  if (!resourceId) {
    throw new InvalidRoomError(value);
  }

  return { scope, resourceId };
}

export function encodeRoom(value: string): string {
  parseRoom(value);
  return encodeURIComponent(value).replace(/%3A/gi, ENCODED_SEPARATOR);
}

export function decodeRoom(value: string): string {
  const decoded = decodeURIComponent(value.replace(new RegExp(ENCODED_SEPARATOR, 'g'), '%3A'));
  parseRoom(decoded);
  return decoded;
}

export function isRealtimeScope(value: string): value is RealtimeScope {
  return typeof value === 'string' && SCOPE_PATTERN.test(value);
}

export function buildRoom(scope: string, resourceId: string): string {
  const cleanScope = scope.trim();
  const cleanResourceId = resourceId.trim();
  if (!isRealtimeScope(cleanScope) || !cleanResourceId || cleanResourceId.includes('\n')) {
    throw new InvalidRoomError(`${scope}${ROOM_SEPARATOR}${resourceId}`);
  }

  return `${cleanScope}${ROOM_SEPARATOR}${cleanResourceId}`;
}
