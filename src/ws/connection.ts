import { assertRoomAccess, type AuthConfig, verifyInitToken } from '../core/auth';
import type { PresenceUser, RealtimeTransport, Unsubscribe } from '../core/types';
import { parseRoom } from '../rooms';
import { decodeClientMessage, encodeServerMessage, type ClientMessage, type ServerMessage, type WebSocketLike } from './protocol';

export interface ConnectionLimits {
  initTimeoutMs?: number;
  maxPayloadBytes?: number;
  maxSubscriptions?: number;
  rateLimitWindowMs?: number;
  maxMessagesPerWindow?: number;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  maxSocketBufferedBytes?: number;
  allowClientPublish?: boolean;
}

export interface ConnectionOptions {
  id: string;
  ws: WebSocketLike;
  transport: RealtimeTransport;
  auth?: AuthConfig;
  limits?: ConnectionLimits;
  onClose?: (connectionId: string) => void;
  onPresenceEnter?: (room: string, user: PresenceUser) => void;
  onPresenceLeave?: (room: string, userId: string) => void;
  getPresence?: (room: string) => PresenceUser[];
}

export class Connection {
  private subscriberId?: string;
  private token?: string;
  private claims?: Record<string, unknown>;
  private readonly subscriptions = new Map<string, Unsubscribe>();
  private readonly limits: Required<ConnectionLimits>;
  private initTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private heartbeatTimeout: ReturnType<typeof setTimeout> | null = null;
  private windowStartedAt = Date.now();
  private messagesInWindow = 0;
  private closed = false;
  private processing: Promise<void> = Promise.resolve();

  constructor(private readonly options: ConnectionOptions) {
    this.limits = {
      initTimeoutMs: options.limits?.initTimeoutMs ?? 10000,
      maxPayloadBytes: options.limits?.maxPayloadBytes ?? 64 * 1024,
      maxSubscriptions: options.limits?.maxSubscriptions ?? 128,
      rateLimitWindowMs: options.limits?.rateLimitWindowMs ?? 1000,
      maxMessagesPerWindow: options.limits?.maxMessagesPerWindow ?? 100,
      heartbeatIntervalMs: options.limits?.heartbeatIntervalMs ?? 30000,
      heartbeatTimeoutMs: options.limits?.heartbeatTimeoutMs ?? 10000,
      maxSocketBufferedBytes: options.limits?.maxSocketBufferedBytes ?? 1024 * 1024,
      allowClientPublish: options.limits?.allowClientPublish ?? true,
    };
  }

  bind(): void {
    this.initTimer = setTimeout(() => {
      if (!this.subscriberId) {
        this.close(4408, 'Realtime init timeout');
      }
    }, this.limits.initTimeoutMs);
    unrefTimer(this.initTimer);

    this.options.ws.on?.('message', (message: string | Buffer | ArrayBuffer) => {
      void this.handleRaw(message);
    });
    this.options.ws.on?.('close', () => this.disconnect());
    this.options.ws.on?.('error', (error: unknown) => {
      this.sendError(error instanceof Error ? error.message : String(error));
    });
    this.options.ws.on?.('pong', () => this.markAlive());
    this.startHeartbeat();
  }

  /**
   * As mensagens são processadas em série, uma de cada vez.
   *
   * O `init` é assíncrono (verifica o token) e um cliente pode enviar `subscribe`
   * imediatamente a seguir, sem esperar pelo `ready`. Sem esta fila, essas
   * mensagens seriam processadas enquanto o `init` ainda está pendente e
   * falhariam em `assertInitialized`.
   */
  async handleRaw(raw: string | Buffer | ArrayBuffer): Promise<void> {
    const processNext = () => this.processRaw(raw);
    this.processing = this.processing.then(processNext, processNext);
    return this.processing;
  }

  private async processRaw(raw: string | Buffer | ArrayBuffer): Promise<void> {
    try {
      if (this.closed) {
        return;
      }
      if (!this.consumeRateLimit()) {
        return this.close(4408, 'Realtime rate limit exceeded');
      }
      if (rawByteLength(raw) > this.limits.maxPayloadBytes) {
        return this.close(4409, 'Realtime payload too large');
      }
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
      case 'publish_ephemeral':
        return this.publishEphemeral(message);
      case 'presence_enter':
        return this.presenceEnter(message);
      case 'presence_leave':
        return this.presenceLeave(message);
      case 'ack':
        return this.ack(message);
      case 'replay_request':
        return this.replay(message);
      default:
        return this.sendError('Unknown realtime protocol message');
    }
  }

  disconnect(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.clearTimers();
    Array.from(this.subscriptions.values()).forEach((unsubscribe) => void unsubscribe());
    this.subscriptions.clear();
    this.options.onClose?.(this.options.id);
  }

  hasSubscription(room: string): boolean {
    return this.subscriptions.has(room);
  }

  sendPresence(room: string, users: PresenceUser[]): void {
    this.send({ type: 'presence', room, users });
  }

  private async init(message: Extract<ClientMessage, { type: 'init' }>): Promise<void> {
    if (!message.subscriberId) {
      throw new Error('subscriberId is required');
    }

    // A verificação corre ANTES de qualquer estado ser assumido. Se `subscriberId`
    // fosse atribuído primeiro, um init com token inválido deixaria a ligação num
    // estado "inicializado sem claims" — `assertInitialized` passaria e o cliente
    // continuaria a enviar mensagens autenticadas apenas pela política de rooms.
    let claims: Record<string, unknown> | undefined;
    try {
      claims = await verifyInitToken(message.token, this.options.auth);
    } catch (error) {
      // Falha de autenticação encerra a ligação em vez de a deixar aberta.
      this.sendError(error instanceof Error ? error.message : String(error));
      this.close(4401, 'Realtime authentication failed');
      return;
    }

    this.subscriberId = message.subscriberId;
    this.token = message.token;
    this.claims = claims;
    if (this.initTimer) {
      clearTimeout(this.initTimer);
      this.initTimer = null;
    }
    this.send({ type: 'ready', subscriberId: this.subscriberId });
  }

  private async subscribe(message: Extract<ClientMessage, { type: 'subscribe' }>): Promise<void> {
    const rooms = Array.isArray(message.room) ? message.room : [message.room];
    for (const room of rooms) {
      await this.subscribeRoom(room, message);
    }
  }

  private async subscribeRoom(room: string, message: Extract<ClientMessage, { type: 'subscribe' }>): Promise<void> {
    parseRoom(room);
    await this.authorize(room, 'subscribe');
    if (!this.subscriptions.has(room) && this.subscriptions.size >= this.limits.maxSubscriptions) {
      throw new Error('Realtime subscription limit exceeded');
    }
    this.unsubscribe(room, false);
    let fromSequence = message.fromSequence;
    if (message.sync === 'snapshot') {
      const snapshot = await this.options.transport.getSnapshot(room);
      this.send({ type: 'snapshot', room, snapshot });
      fromSequence = fromSequence ?? snapshot?.lastSequence;
    }
    const unsubscribe = this.options.transport.subscribe(
      {
        room,
        subscriberId: this.subscriberId,
        eventTypes: message.eventTypes,
        fromSequence,
        sync: message.sync,
      },
      (event) => {
        this.send({ type: 'event', data: event });
      },
    );
    const unsubscribeEphemeral = this.options.transport.subscribeEphemeral
      ? this.options.transport.subscribeEphemeral(
        { room, eventTypes: message.eventTypes },
        (event) => {
          this.applyPresenceEvent(event.room, event.type, event.payload);
          this.send({ type: 'ephemeral', data: event });
        },
      )
      : () => undefined;
    this.subscriptions.set(room, async () => {
      await unsubscribe();
      await unsubscribeEphemeral();
    });
    this.send({ type: 'subscribed', room });
    const users = this.options.getPresence?.(room) ?? [];
    if (users.length > 0) {
      this.sendPresence(room, users);
    }
  }

  private unsubscribe(room: string, notify = true): void {
    const unsubscribe = this.subscriptions.get(room);
    if (unsubscribe) {
      void unsubscribe();
    }
    this.subscriptions.delete(room);
    if (notify) {
      this.send({ type: 'unsubscribed', room });
    }
  }

  private async publish(message: Extract<ClientMessage, { type: 'publish' }>): Promise<void> {
    if (!this.limits.allowClientPublish) {
      throw new Error('Realtime client publish is disabled');
    }
    await this.authorize(message.event.room, 'publish');
    await this.options.transport.publish(message.event);
  }

  private async publishEphemeral(message: Extract<ClientMessage, { type: 'publish_ephemeral' }>): Promise<void> {
    await this.authorize(message.event.room, 'publish');
    await this.options.transport.publishEphemeral(message.event);
  }

  private async presenceEnter(message: Extract<ClientMessage, { type: 'presence_enter' }>): Promise<void> {
    await this.authorize(message.room, 'publish');
    const user = {
      ...message.user,
      lastSeenAt: message.user.lastSeenAt ?? new Date().toISOString(),
    };
    this.options.onPresenceEnter?.(message.room, user);
    await this.options.transport.publishEphemeral({
      room: message.room,
      type: 'presence.enter',
      payload: {
        user,
      },
      metadata: { producer: this.subscriberId },
    });
  }

  private async presenceLeave(message: Extract<ClientMessage, { type: 'presence_leave' }>): Promise<void> {
    await this.authorize(message.room, 'publish');
    const userId = message.userId ?? this.subscriberId!;
    this.options.onPresenceLeave?.(message.room, userId);
    await this.options.transport.publishEphemeral({
      room: message.room,
      type: 'presence.leave',
      payload: {
        userId,
      },
      metadata: { producer: this.subscriberId },
    });
  }

  private applyPresenceEvent(room: string, type: string, payload: unknown): void {
    const value = payload as { user?: PresenceUser; userId?: string };
    if (type === 'presence.enter' && value.user?.userId) {
      this.options.onPresenceEnter?.(room, value.user);
    }
    if (type === 'presence.leave' && value.userId) {
      this.options.onPresenceLeave?.(room, value.userId);
    }
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
    if (this.closed) {
      return;
    }
    if ((this.options.ws.bufferedAmount ?? 0) > this.limits.maxSocketBufferedBytes) {
      this.options.ws.close?.(1013, 'Realtime socket backpressure limit exceeded');
      this.disconnect();
      return;
    }
    try {
      this.options.ws.send(encodeServerMessage(message), (error?: Error) => {
        if (error) {
          this.close(1011, 'Realtime socket send failed');
        }
      });
    } catch {
      this.close(1011, 'Realtime socket send failed');
    }
  }

  private sendError(message: string, code?: string): void {
    this.send({ type: 'error', message, code });
  }

  private close(code: number, reason: string): void {
    if (this.closed) {
      return;
    }
    this.sendError(reason, String(code));
    this.options.ws.close?.(code, reason);
    this.disconnect();
  }

  private consumeRateLimit(): boolean {
    const now = Date.now();
    if (now - this.windowStartedAt >= this.limits.rateLimitWindowMs) {
      this.windowStartedAt = now;
      this.messagesInWindow = 0;
    }
    this.messagesInWindow += 1;
    return this.messagesInWindow <= this.limits.maxMessagesPerWindow;
  }

  private startHeartbeat(): void {
    if (!this.options.ws.ping || this.limits.heartbeatIntervalMs <= 0) {
      return;
    }

    this.heartbeatTimer = setInterval(() => {
      if (this.closed) {
        return;
      }
      this.options.ws.ping?.();
      if (this.heartbeatTimeout) {
        clearTimeout(this.heartbeatTimeout);
      }
      this.heartbeatTimeout = setTimeout(() => {
        this.options.ws.terminate?.();
        this.disconnect();
      }, this.limits.heartbeatTimeoutMs);
      unrefTimer(this.heartbeatTimeout);
    }, this.limits.heartbeatIntervalMs);
    unrefTimer(this.heartbeatTimer);
  }

  private markAlive(): void {
    if (this.heartbeatTimeout) {
      clearTimeout(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
    }
  }

  private clearTimers(): void {
    if (this.initTimer) {
      clearTimeout(this.initTimer);
      this.initTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.heartbeatTimeout) {
      clearTimeout(this.heartbeatTimeout);
      this.heartbeatTimeout = null;
    }
  }
}

function rawByteLength(raw: string | Buffer | ArrayBuffer): number {
  if (typeof raw === 'string') {
    return Buffer.byteLength(raw);
  }
  if (raw instanceof ArrayBuffer) {
    return raw.byteLength;
  }
  return raw.length;
}

function unrefTimer(timer: ReturnType<typeof setTimeout> | ReturnType<typeof setInterval> | null): void {
  (timer as any)?.unref?.();
}
