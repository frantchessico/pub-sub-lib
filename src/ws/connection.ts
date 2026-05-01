import { assertRoomAccess, type AuthConfig, verifyInitToken } from '../core/auth';
import type { RealtimeTransport, Unsubscribe } from '../core/types';
import { parseRoom } from '../rooms';
import { decodeClientMessage, encodeServerMessage, type ClientMessage, type ServerMessage, type WebSocketLike } from './protocol';

export interface ConnectionOptions {
  id: string;
  ws: WebSocketLike;
  transport: RealtimeTransport;
  auth?: AuthConfig;
  onClose?: (connectionId: string) => void;
}

export class Connection {
  private subscriberId?: string;
  private token?: string;
  private claims?: Record<string, unknown>;
  private readonly subscriptions = new Map<string, Unsubscribe>();

  constructor(private readonly options: ConnectionOptions) {}

  bind(): void {
    this.options.ws.on?.('message', (message: string | Buffer | ArrayBuffer) => {
      void this.handleRaw(message);
    });
    this.options.ws.on?.('close', () => this.disconnect());
    this.options.ws.on?.('error', (error: unknown) => {
      this.sendError(error instanceof Error ? error.message : String(error));
    });
  }

  async handleRaw(raw: string | Buffer | ArrayBuffer): Promise<void> {
    try {
      await this.handle(decodeClientMessage(raw));
    } catch (error) {
      this.sendError(error instanceof Error ? error.message : String(error));
    }
  }

  async handle(message: ClientMessage): Promise<void> {
    if (message.type === 'init') {
      return this.init(message);
    }

    this.assertInitialized();

    switch (message.type) {
      case 'subscribe':
        return this.subscribe(message);
      case 'unsubscribe':
        return this.unsubscribe(message.room);
      case 'publish':
        return this.publish(message);
      case 'ack':
        return this.ack(message);
      case 'replay_request':
        return this.replay(message);
      default:
        return this.sendError('Unknown realtime protocol message');
    }
  }

  disconnect(): void {
    Array.from(this.subscriptions.values()).forEach((unsubscribe) => void unsubscribe());
    this.subscriptions.clear();
    this.options.onClose?.(this.options.id);
  }

  private async init(message: Extract<ClientMessage, { type: 'init' }>): Promise<void> {
    if (!message.subscriberId) {
      throw new Error('subscriberId is required');
    }
    this.subscriberId = message.subscriberId;
    this.token = message.token;
    this.claims = await verifyInitToken(message.token, this.options.auth);
    this.send({ type: 'ready', subscriberId: this.subscriberId });
  }

  private async subscribe(message: Extract<ClientMessage, { type: 'subscribe' }>): Promise<void> {
    parseRoom(message.room);
    await this.authorize(message.room, 'subscribe');
    this.unsubscribe(message.room);
    const unsubscribe = this.options.transport.subscribe(
      {
        room: message.room,
        subscriberId: this.subscriberId,
        eventTypes: message.eventTypes,
        fromSequence: message.fromSequence,
      },
      (event) => {
        this.send({ type: 'event', data: event });
      },
    );
    this.subscriptions.set(message.room, unsubscribe);
    this.send({ type: 'subscribed', room: message.room });
  }

  private unsubscribe(room: string): void {
    const unsubscribe = this.subscriptions.get(room);
    if (unsubscribe) {
      void unsubscribe();
    }
    this.subscriptions.delete(room);
    this.send({ type: 'unsubscribed', room });
  }

  private async publish(message: Extract<ClientMessage, { type: 'publish' }>): Promise<void> {
    await this.authorize(message.event.room, 'publish');
    await this.options.transport.publish(message.event);
  }

  private async ack(message: Extract<ClientMessage, { type: 'ack' }>): Promise<void> {
    await this.authorize(message.room, 'ack');
    await this.options.transport.ack(message.room, message.sequence, this.subscriberId!);
    this.send({ type: 'ack', room: message.room, sequence: message.sequence });
  }

  private async replay(message: Extract<ClientMessage, { type: 'replay_request' }>): Promise<void> {
    await this.authorize(message.room, 'replay');
    const result = await this.options.transport.replay({
      room: message.room,
      fromSequence: message.fromSequence,
      toSequence: message.toSequence,
      eventTypes: message.eventTypes,
      limit: message.limit,
    });
    this.send({ type: 'replay', room: message.room, events: result.events });
  }

  private async authorize(room: string, action: 'subscribe' | 'publish' | 'replay' | 'ack'): Promise<void> {
    await assertRoomAccess({
      subscriberId: this.subscriberId!,
      token: this.token,
      claims: this.claims,
      room,
      action,
    }, this.options.auth);
  }

  private assertInitialized(): void {
    if (!this.subscriberId) {
      throw new Error('Realtime connection must send init first');
    }
  }

  private send(message: ServerMessage): void {
    this.options.ws.send(encodeServerMessage(message));
  }

  private sendError(message: string, code?: string): void {
    this.send({ type: 'error', message, code });
  }
}

