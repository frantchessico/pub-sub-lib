import type {
  CatchUpOptions,
  DLQHandle,
  EphemeralEnvelope,
  PublishEphemeralInput,
  PublishInput,
  RealtimeEnvelope,
  RealtimeEventHandler,
  RealtimeHealth,
  RealtimeMiddleware,
  RealtimeMetrics,
  RealtimeTransport,
  ReplayOptions,
  ReplayResult,
  Snapshot,
  SnapshotInput,
  StreamReplayOptions,
  SubscribeOptions,
  Unsubscribe,
} from './core/types';
import { createRealtimeClient, type RealtimeClient, type RealtimeClientOptions } from './client-sdk';

export interface RealtimeCloudOptions {
  endpoint: string;
  secretKey: string;
  fetch?: typeof fetch;
}

export interface RealtimeCloudClientOptions extends Omit<RealtimeClientOptions, 'url'> {
  appId: string;
  apiKey: string;
  endpoint?: string;
}

export function createRealtimeCloudClient(options: RealtimeCloudClientOptions): RealtimeClient {
  const endpoint = options.endpoint ?? 'wss://cloud.zero-pub-sub.savanapoint.com';
  return createRealtimeClient({
    ...options,
    url: `${endpoint.replace(/\/$/, '')}/apps/${encodeURIComponent(options.appId)}`,
    authToken: options.apiKey,
  });
}

export function connectRealtimeCloud(options: RealtimeCloudOptions): RealtimeTransport {
  return new CloudRealtimeTransport(options);
}

class CloudRealtimeTransport implements RealtimeTransport {
  private readonly fetchImpl: typeof fetch;

  constructor(private readonly options: RealtimeCloudOptions) {
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    if (!this.fetchImpl) {
      throw new Error('fetch is required for connectRealtimeCloud');
    }
  }

  use(_middleware: RealtimeMiddleware): void {}

  publish<TPayload = unknown>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>): Promise<RealtimeEnvelope<TPayload>> {
    return this.request('/events', 'POST', event);
  }

  publishEphemeral<TPayload = unknown>(event: PublishEphemeralInput<TPayload> | EphemeralEnvelope<TPayload>): Promise<EphemeralEnvelope<TPayload>> {
    return this.request('/ephemeral', 'POST', event);
  }

  subscribe<TPayload = unknown>(_options: SubscribeOptions, _handler: RealtimeEventHandler<TPayload>): Unsubscribe {
    throw new Error('Server-side cloud subscribe is not supported. Use createRealtimeCloudClient for live subscriptions.');
  }

  subscribeEphemeral<TPayload = unknown>(): Unsubscribe {
    throw new Error('Server-side cloud ephemeral subscribe is not supported. Use createRealtimeCloudClient for live subscriptions.');
  }

  replay<TPayload = unknown>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    return this.request('/replay', 'POST', options);
  }

  async *catchUp<TPayload = unknown>(options: CatchUpOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    let cursor = options.fromSequence ?? 0;
    const batchSize = options.batchSize ?? 500;
    while (true) {
      const { events } = await this.replay<TPayload>({ ...options, fromSequence: cursor, limit: batchSize });
      if (!events.length) {
        return;
      }
      for (const event of events) {
        cursor = event.sequence;
        yield event;
      }
      if (events.length < batchSize) {
        return;
      }
    }
  }

  streamReplay<TPayload = unknown>(options: StreamReplayOptions): AsyncIterable<RealtimeEnvelope<TPayload>> {
    return this.catchUp<TPayload>(options);
  }

  snapshot<TState = unknown>(room: string, snapshot: SnapshotInput<TState>): Promise<Snapshot<TState>> {
    return this.request(`/rooms/${encodeURIComponent(room)}/snapshot`, 'PUT', snapshot);
  }

  getSnapshot<TState = unknown>(room: string): Promise<Snapshot<TState> | null> {
    return this.request(`/rooms/${encodeURIComponent(room)}/snapshot`, 'GET');
  }

  dlq(room: string): DLQHandle {
    const transport = this;
    return {
      list: () => this.request(`/rooms/${encodeURIComponent(room)}/dlq`, 'GET'),
      replay: async function* <TPayload = unknown>() {
        const events = await transport.request<RealtimeEnvelope<TPayload>[]>(`/rooms/${encodeURIComponent(room)}/dlq/replay`, 'POST');
        for (const event of events) {
          yield event;
        }
      },
      clear: () => this.request(`/rooms/${encodeURIComponent(room)}/dlq`, 'DELETE'),
    };
  }

  ack(room: string, sequence: number, subscriberId: string): Promise<void> {
    return this.request('/ack', 'POST', { room, sequence, subscriberId });
  }

  snapshotMetrics(): RealtimeMetrics {
    return {
      published: 0,
      received: 0,
      acked: 0,
      gapsDetected: 0,
      errors: 0,
      replayed: 0,
      duplicatesDropped: 0,
      activeRooms: 0,
      activeListeners: 0,
      averageDeliveryLagMs: 0,
      retryCount: 0,
      dlqSize: 0,
      snapshotUsage: 0,
      replayLatencyMs: 0,
    };
  }

  health(): Promise<RealtimeHealth> {
    return this.request('/health', 'GET');
  }

  async close(): Promise<void> {}

  private async request<T>(path: string, method: string, body?: unknown): Promise<T> {
    const response = await this.fetchImpl(`${this.options.endpoint.replace(/\/$/, '')}${path}`, {
      method,
      headers: {
        authorization: `Bearer ${this.options.secretKey}`,
        'content-type': 'application/json',
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!response.ok) {
      throw new Error(`Zero Pub/Sub Cloud request failed: ${response.status}`);
    }
    if (response.status === 204) {
      return undefined as T;
    }
    return response.json() as Promise<T>;
  }
}
