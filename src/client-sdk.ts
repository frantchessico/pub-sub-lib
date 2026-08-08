import type {
  EphemeralEnvelope,
  FlowControlOptions,
  PresenceUser,
  PublishEphemeralInput,
  PublishInput,
  RealtimeEnvelope,
  Snapshot,
  Unsubscribe,
} from './core/types';
import { parseRoom } from './rooms';
import type { ClientMessage, ServerMessage } from './ws/protocol';

type WebSocketCtor = new (url: string) => {
  send(data: string): void;
  close(): void;
  addEventListener?(event: string, handler: (event: any) => void): void;
  removeEventListener?(event: string, handler: (event: any) => void): void;
  onopen?: () => void;
  onclose?: () => void;
  onerror?: (event: any) => void;
  onmessage?: (event: { data: string }) => void;
  readyState?: number;
};

export interface RealtimeClientOptions {
  url: string;
  subscriberId: string;
  authToken?: string;
  reconnect?: boolean;
  WebSocket?: WebSocketCtor;
  reconnectDelayMs?: number;
  maxBufferedEventsPerRoom?: number;
  replayThrottleMs?: number;
  maxQueuedMessages?: number;
  onError?: (error: Error) => void;
  onStatusChange?: (status: RealtimeConnectionStatus) => void;
}

export type RealtimeConnectionStatus = "connecting" | "connected" | "reconnecting" | "disconnected";

export interface ClientSubscribeOptions {
  room: string;
  eventTypes?: string[];
  fromSequence?: number;
  sync?: 'events' | 'snapshot';
  flowControl?: FlowControlOptions;
  onSnapshot?: (snapshot: Snapshot | null) => void | Promise<void>;
}

type ClientHandler<TPayload = unknown> = (event: RealtimeEnvelope<TPayload>) => void | Promise<void>;
type EphemeralHandler<TPayload = unknown> = (event: EphemeralEnvelope<TPayload>) => void | Promise<void>;
type PresenceHandler = (users: PresenceUser[]) => void | Promise<void>;

type RoomState = {
  lastSequence: number;
  initialFromSequence?: number;
  buffer: Map<number, RealtimeEnvelope>;
  handlers: Set<ClientHandler<any>>;
  ephemeralHandlers: Set<EphemeralHandler<any>>;
  presenceHandlers: Set<PresenceHandler>;
  presenceUsers: Map<string, PresenceUser>;
  eventTypes?: string[];
  sync?: 'events' | 'snapshot';
  onSnapshot?: (snapshot: Snapshot | null) => void | Promise<void>;
  lastReplayRequestedAt: number;
  replayInFlight: boolean;
};

export class RealtimeClient {
  private socket: InstanceType<WebSocketCtor> | null = null;
  private readonly rooms = new Map<string, RoomState>();
  private readonly outboundQueue: ClientMessage[] = [];
  private outboundQueueOffset = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(private readonly options: RealtimeClientOptions) {}

  connect(): void {
    this.closed = false;
    this.emitStatus("connecting");
    const WebSocketImpl = this.options.WebSocket ?? getGlobalWebSocket();
    if (!WebSocketImpl) {
      throw new Error('WebSocket implementation is required');
    }

    this.socket = new WebSocketImpl(this.options.url);
    this.bindSocket(this.socket);
  }

  subscribe<TPayload = unknown>(
    room: string | string[],
    handler: ClientHandler<TPayload>,
    options: Omit<ClientSubscribeOptions, 'room'> = {},
  ): Unsubscribe {
    if (Array.isArray(room)) {
      const unsubscribes = room.map((singleRoom) => this.subscribe(singleRoom, handler, options));
      return async () => {
        await Promise.all(unsubscribes.map((unsubscribe) => unsubscribe()));
      };
    }
    parseRoom(room);
    const state = this.rooms.get(room) ?? this.createRoomState(options);
    state.handlers.add(handler as ClientHandler<any>);
    state.eventTypes = options.eventTypes ?? state.eventTypes;
    state.sync = options.sync ?? state.sync;
    state.onSnapshot = options.onSnapshot ?? state.onSnapshot;
    this.rooms.set(room, state);
    this.send({
      type: 'subscribe',
      room,
      eventTypes: state.eventTypes,
      fromSequence: this.resolveSubscribeFromSequence(state),
      sync: state.sync,
    });

    return () => {
      const current = this.rooms.get(room);
      current?.handlers.delete(handler as ClientHandler<any>);
      if (current && current.handlers.size === 0) {
        this.rooms.delete(room);
        this.send({ type: 'unsubscribe', room });
      }
    };
  }

  publish<TPayload = unknown>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): void {
    this.send({ type: 'publish', event });
  }

  publishEphemeral<TPayload = unknown>(event: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>): void {
    this.send({ type: 'publish_ephemeral', event });
  }

  subscribeEphemeral<TPayload = unknown>(
    room: string,
    handler: EphemeralHandler<TPayload>,
    options: Pick<ClientSubscribeOptions, 'eventTypes'> = {},
  ): Unsubscribe {
    parseRoom(room);
    const state = this.rooms.get(room) ?? this.createRoomState({ eventTypes: options.eventTypes });
    state.ephemeralHandlers.add(handler as EphemeralHandler<any>);
    state.eventTypes = options.eventTypes ?? state.eventTypes;
    this.rooms.set(room, state);
    this.send({ type: 'subscribe', room, eventTypes: state.eventTypes, fromSequence: state.lastSequence });

    return () => {
      const current = this.rooms.get(room);
      current?.ephemeralHandlers.delete(handler as EphemeralHandler<any>);
    };
  }

  presence(room: string): {
    enter(user: Omit<PresenceUser, 'lastSeenAt'> & { lastSeenAt?: string }): void;
    leave(userId?: string): void;
    subscribe(handler: PresenceHandler): Unsubscribe;
  } {
    parseRoom(room);
    return {
      enter: (user) => this.send({ type: 'presence_enter', room, user }),
      leave: (userId) => this.send({ type: 'presence_leave', room, userId }),
      subscribe: (handler) => {
        const state = this.rooms.get(room) ?? this.createRoomState({});
        state.presenceHandlers.add(handler);
        this.rooms.set(room, state);
        this.send({ type: 'subscribe', room, fromSequence: state.lastSequence });
        return () => {
          const current = this.rooms.get(room);
          current?.presenceHandlers.delete(handler);
        };
      },
    };
  }

  close(): void {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.socket?.close();
    this.socket = null;
    this.emitStatus("disconnected");
  }

  private emitStatus(status: RealtimeConnectionStatus) {
    this.options.onStatusChange?.(status);
  }

  private bindSocket(socket: InstanceType<WebSocketCtor>): void {
    const onOpen = () => {
      this.emitStatus("connected");
      this.send({ type: 'init', subscriberId: this.options.subscriberId, token: this.options.authToken });
      this.rooms.forEach((state, room) => {
        this.send({ type: 'subscribe', room, eventTypes: state.eventTypes, fromSequence: this.resolveSubscribeFromSequence(state), sync: state.sync });
      });
      this.flushOutboundQueue();
    };

    const onClose = () => {
      if (this.closed || this.options.reconnect === false) {
        this.emitStatus("disconnected");
        return;
      }

      this.emitStatus("reconnecting");
      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          this.connect();
        }, this.options.reconnectDelayMs ?? 1000);
      }
    };

    const onMessage = (event: { data: string }) => {
      try {
        this.handleServerMessage(JSON.parse(event.data) as ServerMessage);
      } catch (error) {
        this.reportError(error);
      }
    };

    if (socket.addEventListener) {
      socket.addEventListener('open', onOpen);
      socket.addEventListener('close', onClose);
      socket.addEventListener('message', onMessage);
    } else {
      socket.onopen = onOpen;
      socket.onclose = onClose;
      socket.onmessage = onMessage;
    }
  }

  private handleServerMessage(message: ServerMessage): void {
    if (message.type === 'event') {
      this.receive(message.data);
      return;
    }

    if (message.type === 'replay') {
      const state = this.rooms.get(message.room);
      if (state) {
        state.replayInFlight = false;
      }
      message.events.forEach((event) => this.receive(event));
      return;
    }

    if (message.type === 'ephemeral') {
      this.receiveEphemeral(message.data);
      return;
    }

    if (message.type === 'snapshot') {
      const state = this.rooms.get(message.room);
      if (state) {
        state.lastSequence = Math.max(state.lastSequence, message.snapshot?.lastSequence ?? 0);
        void Promise.resolve(state.onSnapshot?.(message.snapshot)).catch((error) => this.reportError(error));
      }
      return;
    }

    if (message.type === 'presence') {
      const state = this.rooms.get(message.room);
      if (state) {
        void Promise.all(Array.from(state.presenceHandlers).map((handler) => Promise.resolve(handler(message.users))))
          .catch((error) => this.reportError(error));
      }
    }
  }

  private receive(event: RealtimeEnvelope): void {
    const state = this.rooms.get(event.room);
    if (!state) {
      return;
    }

    if (event.sequence <= state.lastSequence) {
      return;
    }

    if (event.sequence !== state.lastSequence + 1) {
      if (state.buffer.size >= (this.options.maxBufferedEventsPerRoom ?? 1000)) {
        this.reportError(new Error(`Realtime client buffer limit exceeded for room ${event.room}`));
        state.buffer.clear();
        this.requestReplay(event.room, state, true);
        return;
      }

      state.buffer.set(event.sequence, event);
      this.requestReplay(event.room, state);
      return;
    }

    this.flush(event.room, event);
  }

  private flush(room: string, firstEvent: RealtimeEnvelope): void {
    const state = this.rooms.get(room);
    if (!state) {
      return;
    }

    let next: RealtimeEnvelope | undefined = firstEvent;
    while (next) {
      state.lastSequence = next.sequence;
      const delivered = next;
      void Promise.all(Array.from(state.handlers).map((handler) => Promise.resolve(handler(delivered))))
        .then(() => this.send({ type: 'ack', room, sequence: delivered.sequence }))
        .catch((error) => this.reportError(error));
      state.buffer.delete(next.sequence);
      next = state.buffer.get(state.lastSequence + 1);
    }
  }

  private receiveEphemeral(event: EphemeralEnvelope): void {
    const state = this.rooms.get(event.room);
    if (!state) {
      return;
    }
    this.applyPresenceEvent(state, event);
    void Promise.all(Array.from(state.ephemeralHandlers).map((handler) => Promise.resolve(handler(event))))
      .catch((error) => this.reportError(error));
  }

  private applyPresenceEvent(state: RoomState, event: EphemeralEnvelope): void {
    const payload = event.payload as any;
    if (event.type === 'presence.enter' && payload?.user?.userId) {
      state.presenceUsers.set(payload.user.userId, payload.user);
    }
    if (event.type === 'presence.leave' && payload?.userId) {
      state.presenceUsers.delete(payload.userId);
    }
    if (event.type === 'presence.enter' || event.type === 'presence.leave') {
      const users = Array.from(state.presenceUsers.values());
      void Promise.all(Array.from(state.presenceHandlers).map((handler) => Promise.resolve(handler(users))))
        .catch((error) => this.reportError(error));
    }
  }

  private send(message: ClientMessage): void {
    if (!this.socket || this.socket.readyState !== 1) {
      if (this.outboundQueue.length < (this.options.maxQueuedMessages ?? 1000)) {
        this.outboundQueue.push(message);
      } else {
        this.reportError(new Error('Realtime client outbound queue limit exceeded'));
      }
      return;
    }
    this.socket.send(JSON.stringify(message));
  }

  private flushOutboundQueue(): void {
    while (this.outboundQueueOffset < this.outboundQueue.length && this.socket?.readyState === 1) {
      const message = this.outboundQueue[this.outboundQueueOffset++];
      if (message) {
        this.socket.send(JSON.stringify(message));
      }
    }
    if (this.outboundQueueOffset >= this.outboundQueue.length) {
      this.outboundQueue.length = 0;
      this.outboundQueueOffset = 0;
    }
  }

  private requestReplay(room: string, state: RoomState, force = false): void {
    const now = Date.now();
    const throttleMs = this.options.replayThrottleMs ?? 500;
    if (!force && (state.replayInFlight || now - state.lastReplayRequestedAt < throttleMs)) {
      return;
    }

    state.replayInFlight = true;
    state.lastReplayRequestedAt = now;
    this.send({ type: 'replay_request', room, fromSequence: state.lastSequence });
  }

  private reportError(error: unknown): void {
    const normalized = error instanceof Error ? error : new Error(String(error));
    this.options.onError?.(normalized);
  }

  private createRoomState(options: Omit<ClientSubscribeOptions, 'room'>): RoomState {
    return {
      lastSequence: options.fromSequence ?? 0,
      initialFromSequence: options.fromSequence,
      buffer: new Map(),
      handlers: new Set(),
      ephemeralHandlers: new Set(),
      presenceHandlers: new Set(),
      presenceUsers: new Map(),
      eventTypes: options.eventTypes,
      sync: options.sync,
      onSnapshot: options.onSnapshot,
      lastReplayRequestedAt: 0,
      replayInFlight: false,
    };
  }

  private resolveSubscribeFromSequence(state: RoomState): number | undefined {
    if (state.initialFromSequence !== undefined) {
      return state.initialFromSequence;
    }
    return state.lastSequence > 0 ? state.lastSequence : undefined;
  }
}

export function createRealtimeClient(options: RealtimeClientOptions): RealtimeClient {
  const client = new RealtimeClient({ reconnect: true, ...options });
  return client;
}

function getGlobalWebSocket(): WebSocketCtor | undefined {
  return (globalThis as any).WebSocket;
}
