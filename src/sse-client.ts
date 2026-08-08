import type { RealtimeEnvelope, Unsubscribe } from './core/types';
import { parseRoom } from './rooms';
import {
  deserializeSinceMap,
  formatSseCursor,
  readSseStream,
  serializeSinceMap,
  type SseMessage,
} from './sse-stream';

type FetchLike = typeof fetch;

export type SseClientStatus = 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closed' | 'error';

export interface SseRealtimeClientOptions {
  url: string;
  subscriberId: string;
  authToken?: string;
  fetchImpl?: FetchLike;
  reconnect?: boolean;
  reconnectDelayMs?: number;
  heartbeatTimeoutMs?: number;
  onError?: (error: Error) => void;
  onStatusChange?: (status: SseClientStatus) => void;
}

export interface SseSubscribeOptions {
  room: string;
  eventTypes?: string[];
  fromSequence?: number;
}

type SseHandler<TPayload = unknown> = (event: RealtimeEnvelope<TPayload>) => void | Promise<void>;

type RoomState = {
  lastSequence: number;
  handlers: Set<SseHandler<any>>;
  eventTypes?: string[];
};

export class SseRealtimeClient {
  private readonly rooms = new Map<string, RoomState>();
  private readonly sinceByRoom = new Map<string, number>();
  private abortController: AbortController | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;
  private connectGeneration = 0;
  private status: SseClientStatus = 'idle';

  constructor(private readonly options: SseRealtimeClientOptions) {}

  connect(): void {
    this.closed = false;
    this.scheduleConnect(0);
  }

  subscribe<TPayload = unknown>(
    room: string | string[],
    handler: SseHandler<TPayload>,
    options: Omit<SseSubscribeOptions, 'room'> = {},
  ): Unsubscribe {
    if (Array.isArray(room)) {
      const unsubscribes = room.map((singleRoom) => this.subscribe(singleRoom, handler, options));
      return async () => {
        await Promise.all(unsubscribes.map((unsubscribe) => unsubscribe()));
      };
    }

    parseRoom(room);
    const state = this.rooms.get(room) ?? this.createRoomState(options.fromSequence);
    state.handlers.add(handler as SseHandler<any>);
    state.eventTypes = mergeEventTypes(state.eventTypes, options.eventTypes);
    if (options.fromSequence !== undefined) {
      state.lastSequence = Math.max(state.lastSequence, options.fromSequence);
      this.sinceByRoom.set(room, Math.max(this.sinceByRoom.get(room) ?? 0, options.fromSequence));
    }
    this.rooms.set(room, state);
    this.reconnectWithUpdatedRooms();
    return () => {
      const current = this.rooms.get(room);
      current?.handlers.delete(handler as SseHandler<any>);
      if (current && current.handlers.size === 0) {
        this.rooms.delete(room);
        this.reconnectWithUpdatedRooms();
      }
    };
  }

  close(): void {
    this.closed = true;
    this.clearTimers();
    this.abortController?.abort();
    this.abortController = null;
    this.setStatus('closed');
  }

  getStatus() {
    return this.status;
  }

  private createRoomState(fromSequence?: number): RoomState {
    return {
      lastSequence: fromSequence ?? 0,
      handlers: new Set(),
      eventTypes: undefined,
    };
  }

  private reconnectWithUpdatedRooms() {
    if (this.closed) {
      return;
    }

    this.abortController?.abort();
    this.scheduleConnect(this.options.reconnectDelayMs ?? 500);
  }

  private scheduleConnect(delayMs: number) {
    if (this.closed || this.rooms.size === 0) {
      return;
    }

    this.clearReconnectTimer();
    this.setStatus(delayMs > 0 ? 'reconnecting' : 'connecting');
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.openStream();
    }, delayMs);
  }

  private async openStream() {
    if (this.closed || this.rooms.size === 0) {
      return;
    }

    const generation = ++this.connectGeneration;
    const rooms = Array.from(this.rooms.keys());
    const eventTypes = mergeEventTypes(...Array.from(this.rooms.values()).map((state) => state.eventTypes));
    const params = new URLSearchParams();
    params.set('rooms', rooms.join(','));
    if (eventTypes?.length) {
      params.set('eventTypes', eventTypes.join(','));
    }

    const since = serializeSinceMap(this.buildSinceMap());
    if (since) {
      params.set('since', since);
    }

    const fetchImpl = this.options.fetchImpl ?? getGlobalFetch();
    if (!fetchImpl) {
      this.reportError(new Error('fetch implementation is required for SSE realtime'));
      this.setStatus('error');
      return;
    }

    this.abortController = new AbortController();
    this.setStatus('connecting');

    try {
      const response = await fetchImpl(`${this.options.url}?${params.toString()}`, {
        method: 'GET',
        headers: {
          Accept: 'text/event-stream',
          ...(this.options.authToken ? { Authorization: `Bearer ${this.options.authToken}` } : {}),
        },
        signal: this.abortController.signal,
      });

      if (!response.ok || !response.body) {
        throw new Error(`SSE connection failed with status ${response.status}`);
      }

      if (generation !== this.connectGeneration || this.closed) {
        return;
      }

      this.setStatus('connected');
      this.resetHeartbeatTimer();

      for await (const message of readSseStream(response.body)) {
        if (generation !== this.connectGeneration || this.closed) {
          break;
        }
        await this.handleMessage(message);
      }

      if (!this.closed && generation === this.connectGeneration) {
        this.scheduleConnect(this.options.reconnectDelayMs ?? 1000);
      }
    } catch (error) {
      if (this.closed || generation !== this.connectGeneration) {
        return;
      }

      if (isAbortError(error)) {
        return;
      }

      this.reportError(error);
      this.setStatus('error');
      if (this.options.reconnect !== false) {
        this.scheduleConnect(this.options.reconnectDelayMs ?? 1500);
      }
    }
  }

  private async handleMessage(message: SseMessage) {
    this.resetHeartbeatTimer();

    if (message.event === 'heartbeat' || message.event === 'ready') {
      if (message.event === 'ready' && message.data) {
        try {
          const payload = JSON.parse(message.data) as { since?: string };
          if (payload.since) {
            for (const [room, sequence] of deserializeSinceMap(payload.since)) {
              this.sinceByRoom.set(room, Math.max(this.sinceByRoom.get(room) ?? 0, sequence));
            }
          }
        } catch {
          // ignore malformed ready payloads
        }
      }
      return;
    }

    if (message.event === 'error') {
      this.reportError(new Error(message.data || 'SSE stream error'));
      return;
    }

    let parsed: { room?: string; event?: RealtimeEnvelope };
    try {
      parsed = JSON.parse(message.data || '{}') as { room?: string; event?: RealtimeEnvelope };
    } catch (error) {
      this.reportError(error);
      return;
    }

    const envelope = parsed.event;
    const room = parsed.room || envelope?.room;
    if (!room || !envelope) {
      return;
    }

    if (message.id) {
      const cursorRoom = room;
      const sequence = envelope.sequence;
      if (Number.isFinite(sequence)) {
        this.sinceByRoom.set(cursorRoom, Math.max(this.sinceByRoom.get(cursorRoom) ?? 0, sequence));
      }
    }

    this.deliver(room, envelope);
  }

  private deliver(room: string, envelope: RealtimeEnvelope) {
    const state = this.rooms.get(room);
    if (!state) {
      return;
    }

    if (state.eventTypes?.length && !state.eventTypes.includes(envelope.type)) {
      return;
    }

    if (envelope.sequence <= state.lastSequence) {
      return;
    }

    state.lastSequence = envelope.sequence;
    void Promise.all(Array.from(state.handlers).map((handler) => Promise.resolve(handler(envelope)))).catch(
      (error) => this.reportError(error),
    );
  }

  private buildSinceMap() {
    const merged = new Map<string, number>();
    for (const [room, state] of this.rooms.entries()) {
      merged.set(room, Math.max(state.lastSequence, this.sinceByRoom.get(room) ?? 0));
    }
    for (const [room, sequence] of this.sinceByRoom.entries()) {
      merged.set(room, Math.max(merged.get(room) ?? 0, sequence));
    }
    return merged;
  }

  private resetHeartbeatTimer() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }

    const timeoutMs = this.options.heartbeatTimeoutMs ?? 60_000;
    this.heartbeatTimer = setTimeout(() => {
      this.reportError(new Error('SSE heartbeat timeout'));
      this.abortController?.abort();
      if (!this.closed && this.options.reconnect !== false) {
        this.scheduleConnect(this.options.reconnectDelayMs ?? 1000);
      }
    }, timeoutMs);
    (this.heartbeatTimer as any).unref?.();
  }

  private clearTimers() {
    this.clearReconnectTimer();
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private clearReconnectTimer() {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private setStatus(status: SseClientStatus) {
    if (this.status === status) {
      return;
    }
    this.status = status;
    this.options.onStatusChange?.(status);
  }

  private reportError(error: unknown) {
    const normalized = error instanceof Error ? error : new Error(String(error));
    this.options.onError?.(normalized);
  }
}

export function createSseRealtimeClient(options: SseRealtimeClientOptions): SseRealtimeClient {
  return new SseRealtimeClient({ reconnect: true, ...options });
}

function mergeEventTypes(...groups: Array<string[] | undefined>) {
  const merged = new Set<string>();
  for (const group of groups) {
    for (const eventType of group || []) {
      merged.add(eventType);
    }
  }
  return merged.size ? Array.from(merged) : undefined;
}

function getGlobalFetch(): FetchLike | undefined {
  return (globalThis as any).fetch as FetchLike | undefined;
}

function isAbortError(error: unknown) {
  return error instanceof Error && error.name === 'AbortError';
}
