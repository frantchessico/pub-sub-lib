import type { EphemeralEnvelope, PublishEphemeralInput, PublishInput, RealtimeEnvelope, Unsubscribe } from './core/types';
import {
  RealtimeClient,
  type ClientSubscribeOptions,
  type RealtimeClientOptions,
  type RealtimeConnectionStatus,
} from './client-sdk';
import { SseRealtimeClient, type SseClientStatus } from './sse-client';

export type RealtimeTransportKind = 'websocket' | 'sse' | 'none';

export interface ResilientRealtimeClientOptions extends RealtimeClientOptions {
  sseUrl?: string;
  wsConnectTimeoutMs?: number;
  sseActivationDelayMs?: number;
  preferTransport?: 'websocket' | 'sse';
  onTransportChange?: (transport: RealtimeTransportKind) => void;
}

type ClientHandler<TPayload = unknown> = (event: RealtimeEnvelope<TPayload>) => void | Promise<void>;

type SubscriptionRecord = {
  room: string;
  handler: ClientHandler<any>;
  options: Omit<ClientSubscribeOptions, 'room'>;
  wsUnsubscribe?: Unsubscribe;
  sseUnsubscribe?: Unsubscribe;
};

export class ResilientRealtimeClient {
  private readonly wsClient: RealtimeClient;
  private sseClient: SseRealtimeClient | null = null;
  private readonly subscriptions = new Map<string, SubscriptionRecord>();
  private readonly dedupeByRoom = new Map<string, number>();
  private sseActivationTimer: ReturnType<typeof setTimeout> | null = null;
  private activeTransport: RealtimeTransportKind = 'none';
  private closed = false;
  private wsEverConnected = false;

  constructor(private readonly options: ResilientRealtimeClientOptions) {
    this.wsClient = new RealtimeClient({
      reconnect: true,
      ...options,
      onStatusChange: (status) => this.handleWsStatus(status),
    });
  }

  connect(): void {
    this.closed = false;
    this.setTransport('none');
    this.wsClient.connect();
    this.scheduleSseActivation();
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

    const key = `${room}:${this.subscriptions.size}:${Date.now()}`;
    const wrappedHandler = (event: RealtimeEnvelope<TPayload>) => {
      const lastSequence = this.dedupeByRoom.get(room) ?? 0;
      if (event.sequence <= lastSequence) {
        return;
      }
      this.dedupeByRoom.set(room, event.sequence);
      return handler(event);
    };

    const record: SubscriptionRecord = {
      room,
      handler: wrappedHandler as ClientHandler<any>,
      options,
    };
    this.subscriptions.set(key, record);
    this.attachSubscription(record);

    return async () => {
      const current = this.subscriptions.get(key);
      if (!current) {
        return;
      }
      await Promise.all([
        current.wsUnsubscribe ? Promise.resolve(current.wsUnsubscribe()) : undefined,
        current.sseUnsubscribe ? Promise.resolve(current.sseUnsubscribe()) : undefined,
      ]);
      this.subscriptions.delete(key);
    };
  }

  publish<TPayload = unknown>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): void {
    this.wsClient.publish(event);
  }

  publishEphemeral<TPayload = unknown>(event: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>): void {
    this.wsClient.publishEphemeral(event);
  }

  subscribeEphemeral<TPayload = unknown>(
    room: string,
    handler: (event: EphemeralEnvelope<TPayload>) => void | Promise<void>,
    options: Pick<ClientSubscribeOptions, 'eventTypes'> = {},
  ): Unsubscribe {
    return this.wsClient.subscribeEphemeral(room, handler, options);
  }

  presence(room: string) {
    return this.wsClient.presence(room);
  }

  close(): void {
    this.closed = true;
    this.clearTimers();
    this.sseClient?.close();
    this.sseClient = null;
    this.wsClient.close();
    this.setTransport('none');
  }

  getTransport() {
    return this.activeTransport;
  }

  private attachSubscription(record: SubscriptionRecord) {
    if (!record.wsUnsubscribe) {
      record.wsUnsubscribe = this.wsClient.subscribe(record.room, record.handler, record.options);
    }

    if (this.shouldUseSse()) {
      this.ensureSseClient();
      if (!record.sseUnsubscribe && this.sseClient) {
        record.sseUnsubscribe = this.sseClient.subscribe(record.room, record.handler, {
          eventTypes: record.options.eventTypes,
          fromSequence: record.options.fromSequence,
        });
      }
    }
  }

  private ensureSseClient() {
    if (!this.options.sseUrl || this.sseClient) {
      return;
    }

    this.sseClient = new SseRealtimeClient({
      url: this.options.sseUrl,
      subscriberId: this.options.subscriberId,
      authToken: this.options.authToken,
      onStatusChange: (status) => this.handleSseStatus(status),
      onError: (error) => this.options.onError?.(error),
    });
    this.sseClient.connect();
  }

  private handleWsStatus(status: RealtimeConnectionStatus) {
    this.options.onStatusChange?.(status);

    if (status === 'connected') {
      this.wsEverConnected = true;
      this.clearSseActivationTimer();
      if (this.sseClient) {
        for (const record of this.subscriptions.values()) {
          record.sseUnsubscribe = undefined;
        }
        this.sseClient.close();
        this.sseClient = null;
      }
      this.setTransport('websocket');
      return;
    }

    if (status === 'connecting' || status === 'reconnecting') {
      this.setTransport(this.sseClient ? 'sse' : 'none');
      this.scheduleSseActivation();
      return;
    }

    if (status === 'disconnected') {
      this.scheduleSseActivation();
    }
  }

  private handleSseStatus(status: SseClientStatus) {
    if (status === 'connected') {
      this.setTransport('sse');
    }
  }

  private scheduleSseActivation() {
    if (this.closed || !this.options.sseUrl) {
      return;
    }

    this.clearSseActivationTimer();
    const delayMs = this.wsEverConnected
      ? this.options.sseActivationDelayMs ?? 2_000
      : this.options.wsConnectTimeoutMs ?? 8_000;

    this.sseActivationTimer = setTimeout(() => {
      this.sseActivationTimer = null;
      if (this.closed || this.activeTransport === 'websocket') {
        return;
      }
      this.activateSseFallback();
    }, delayMs);
    (this.sseActivationTimer as any).unref?.();
  }

  private activateSseFallback() {
    if (!this.options.sseUrl || this.closed) {
      return;
    }

    this.ensureSseClient();
    if (!this.sseClient) {
      return;
    }

    for (const record of this.subscriptions.values()) {
      if (!record.sseUnsubscribe) {
        record.sseUnsubscribe = this.sseClient.subscribe(record.room, record.handler, {
          eventTypes: record.options.eventTypes,
          fromSequence: record.options.fromSequence,
        });
      }
    }

    this.setTransport('sse');
  }

  private shouldUseSse() {
    return Boolean(this.options.sseUrl && this.sseClient && this.activeTransport === 'sse');
  }

  private setTransport(transport: RealtimeTransportKind) {
    if (this.activeTransport === transport) {
      return;
    }
    this.activeTransport = transport;
    this.options.onTransportChange?.(transport);
  }

  private clearSseActivationTimer() {
    if (this.sseActivationTimer) {
      clearTimeout(this.sseActivationTimer);
      this.sseActivationTimer = null;
    }
  }

  private clearTimers() {
    this.clearSseActivationTimer();
  }
}

export function createResilientRealtimeClient(options: ResilientRealtimeClientOptions): ResilientRealtimeClient {
  return new ResilientRealtimeClient(options);
}
