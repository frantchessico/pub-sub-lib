import { InvalidRoomError } from './errors';
import type { RealtimeScope } from './transport/types';

const ROOM_SEPARATOR = ':';
const ENCODED_SEPARATOR = '__';

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
  scope: RealtimeScope;
  resourceId?: string;
}

export function parseRoom(value: string): ParsedRoom {
  const [scope, ...resourceParts] = value.split(ROOM_SEPARATOR);
  const resourceId = resourceParts.join(ROOM_SEPARATOR);

  if (!isRealtimeScope(scope)) {
    throw new InvalidRoomError(value);
  }

  if (scope === 'admin') {
    if (resourceId !== 'realtime') {
      throw new InvalidRoomError(value);
    }

    return { scope };
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
  return decodeURIComponent(value.replace(new RegExp(ENCODED_SEPARATOR, 'g'), '%3A'));
}

export function isRealtimeScope(value: string): value is RealtimeScope {
  return value === 'user' ||
    value === 'vendor' ||
    value === 'driver' ||
    value === 'admin' ||
    value === 'chat' ||
    value === 'tracking';
}

function buildRoom(scope: Exclude<RealtimeScope, 'admin'>, resourceId: string): string {
  const cleanResourceId = resourceId.trim();
  if (!cleanResourceId) {
    throw new InvalidRoomError(`${scope}:`);
  }

  return `${scope}${ROOM_SEPARATOR}${cleanResourceId}`;
}
