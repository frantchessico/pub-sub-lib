import type { PublishInput, RealtimeEnvelope, Unsubscribe } from './core/types';
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
}

export interface ClientSubscribeOptions {
  room: string;
  eventTypes?: string[];
  fromSequence?: number;
}

type ClientHandler<TPayload = unknown> = (event: RealtimeEnvelope<TPayload>) => void | Promise<void>;

type RoomState = {
  lastSequence: number;
  buffer: Map<number, RealtimeEnvelope>;
  handlers: Set<ClientHandler<any>>;
  eventTypes?: string[];
};

export class RealtimeClient {
  private socket: InstanceType<WebSocketCtor> | null = null;
  private readonly rooms = new Map<string, RoomState>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(private readonly options: RealtimeClientOptions) {}

  connect(): void {
    this.closed = false;
    const WebSocketImpl = this.options.WebSocket ?? getGlobalWebSocket();
    if (!WebSocketImpl) {
      throw new Error('WebSocket implementation is required');
    }

    this.socket = new WebSocketImpl(this.options.url);
    this.bindSocket(this.socket);
  }

  subscribe<TPayload = unknown>(
    room: string,
    handler: ClientHandler<TPayload>,
    options: Omit<ClientSubscribeOptions, 'room'> = {},
  ): Unsubscribe {
    parseRoom(room);
    const state = this.rooms.get(room) ?? { lastSequence: options.fromSequence ?? 0, buffer: new Map(), handlers: new Set(), eventTypes: options.eventTypes };
    state.handlers.add(handler as ClientHandler<any>);
    state.eventTypes = options.eventTypes ?? state.eventTypes;
    this.rooms.set(room, state);
    this.send({ type: 'subscribe', room, eventTypes: state.eventTypes, fromSequence: state.lastSequence });

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

  close(): void {
    this.closed = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.socket?.close();
    this.socket = null;
  }

  private bindSocket(socket: InstanceType<WebSocketCtor>): void {
    const onOpen = () => {
      this.send({ type: 'init', subscriberId: this.options.subscriberId, token: this.options.authToken });
      this.rooms.forEach((state, room) => {
        this.send({ type: 'subscribe', room, eventTypes: state.eventTypes, fromSequence: state.lastSequence });
      });
    };

    const onClose = () => {
      if (this.closed || this.options.reconnect === false) {
        return;
      }

      if (!this.reconnectTimer) {
        this.reconnectTimer = setTimeout(() => {
          this.reconnectTimer = null;
          this.connect();
        }, this.options.reconnectDelayMs ?? 1000);
      }
    };

    const onMessage = (event: { data: string }) => {
      this.handleServerMessage(JSON.parse(event.data) as ServerMessage);
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
      message.events.forEach((event) => this.receive(event));
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
      state.buffer.set(event.sequence, event);
      this.send({ type: 'replay_request', room: event.room, fromSequence: state.lastSequence });
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
      state.handlers.forEach((handler) => void handler(next!));
      this.send({ type: 'ack', room, sequence: next.sequence });
      state.buffer.delete(next.sequence);
      next = state.buffer.get(state.lastSequence + 1);
    }
  }

  private send(message: ClientMessage): void {
    if (!this.socket || this.socket.readyState !== 1) {
      return;
    }
    this.socket.send(JSON.stringify(message));
  }
}

export function createRealtimeClient(options: RealtimeClientOptions): RealtimeClient {
  const client = new RealtimeClient({ reconnect: true, ...options });
  return client;
}

function getGlobalWebSocket(): WebSocketCtor | undefined {
  return (globalThis as any).WebSocket;
}

