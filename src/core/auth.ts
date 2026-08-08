import { parseRoom } from '../rooms';
import type { RoomAccessContext, RoomAuthorizer } from './types';

export interface JwtVerifier {
  verify(token: string): Promise<Record<string, unknown>> | Record<string, unknown>;
}

export interface AuthConfig {
  jwt?: JwtVerifier | ((token: string) => Promise<Record<string, unknown>> | Record<string, unknown>);
  scopes?: string[];
  authorize?: RoomAuthorizer;
}

export async function verifyInitToken(token: string | undefined, config?: AuthConfig): Promise<Record<string, unknown> | undefined> {
  if (!config?.jwt) {
    return undefined;
  }

  if (!token) {
    throw new Error('Authentication token is required');
  }

  if (typeof config.jwt === 'function') {
    return config.jwt(token);
  }

  return config.jwt.verify(token);
}

export async function assertRoomAccess(context: RoomAccessContext, config?: AuthConfig): Promise<void> {
  const parsed = parseRoom(context.room);

  // Se a instância exige autenticação, uma ligação sem claims verificadas nunca
  // acede a uma room — mesmo que `authorize` não esteja definido.
  if (config?.jwt && !context.claims) {
    throw new Error(`Access denied for room: ${context.room}`);
  }

  if (config?.scopes?.length && !config.scopes.includes(parsed.scope)) {
    throw new Error(`Room scope is not allowed: ${parsed.scope}`);
  }

  if (config?.authorize) {
    const allowed = await config.authorize(context);
    if (!allowed) {
      throw new Error(`Access denied for room: ${context.room}`);
    }
  }
}

