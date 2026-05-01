import type { PublishInput, RealtimeEnvelope } from '../core/types';

export type ClientMessage =
  | { type: 'init'; subscriberId: string; token?: string }
  | { type: 'subscribe'; room: string; eventTypes?: string[]; fromSequence?: number }
  | { type: 'unsubscribe'; room: string }
  | { type: 'publish'; event: PublishInput | RealtimeEnvelope }
  | { type: 'ack'; room: string; sequence: number }
  | { type: 'replay_request'; room: string; fromSequence: number; toSequence?: number; eventTypes?: string[]; limit?: number };

export type ServerMessage =
  | { type: 'ready'; subscriberId: string }
  | { type: 'event'; data: RealtimeEnvelope }
  | { type: 'replay'; room: string; events: RealtimeEnvelope[] }
  | { type: 'subscribed'; room: string }
  | { type: 'unsubscribed'; room: string }
  | { type: 'ack'; room: string; sequence: number }
  | { type: 'error'; message: string; code?: string };

export interface WebSocketLike {
  send(data: string): unknown;
  close?(code?: number, reason?: string): unknown;
  on?(event: string, handler: (...args: any[]) => void): unknown;
  off?(event: string, handler: (...args: any[]) => void): unknown;
  readyState?: number;
}

export function encodeServerMessage(message: ServerMessage): string {
  return JSON.stringify(message);
}

export function decodeClientMessage(raw: string | Buffer | ArrayBuffer): ClientMessage {
  const text = typeof raw === 'string' ? raw : Buffer.from(raw as any).toString('utf8');
  return JSON.parse(text) as ClientMessage;
}

