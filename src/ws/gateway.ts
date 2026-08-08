import type { AuthConfig } from '../core/auth';
import type { PresenceUser, RealtimeTransport } from '../core/types';
import { connectRealtime, type ConnectRealtimeOptions } from '../factory';
import { Connection, type ConnectionLimits } from './connection';
import type { WebSocketLike } from './protocol';

export interface RealtimeGatewayOptions {
  transport: RealtimeTransport;
  auth?: AuthConfig;
  limits?: ConnectionLimits & {
    maxConnections?: number;
  };
}

export class RealtimeGateway {
  private readonly connections = new Map<string, Connection>();
  private readonly presence = new Map<string, Map<string, PresenceUser>>();
  private sequence = 0;

  constructor(private readonly options: RealtimeGatewayOptions) {}

  handleConnection(ws: WebSocketLike): Connection | null {
    if (this.options.limits?.maxConnections && this.connections.size >= this.options.limits.maxConnections) {
      ws.close?.(1013, 'Realtime gateway overloaded');
      return null;
    }

    const id = `conn_${Date.now().toString(36)}_${++this.sequence}`;
    const connection = new Connection({
      id,
      ws,
      transport: this.options.transport,
      auth: this.options.auth,
      limits: this.options.limits,
      onClose: (connectionId) => this.connections.delete(connectionId),
      onPresenceEnter: (room, user) => this.enterPresence(room, user),
      onPresenceLeave: (room, userId) => this.leavePresence(room, userId),
      getPresence: (room) => this.listPresence(room),
    });
    this.connections.set(id, connection);
    connection.bind();
    return connection;
  }

  connectionCount(): number {
    return this.connections.size;
  }

  async health() {
    const transport = await Promise.resolve(this.options.transport.health?.());
    return {
      status: transport?.status ?? 'healthy',
      connections: this.connections.size,
      transport,
      metrics: this.options.transport.snapshotMetrics?.(),
    };
  }

  async close(): Promise<void> {
    this.connections.forEach((connection) => connection.disconnect());
    this.connections.clear();
    await this.options.transport.close();
  }

  listPresence(room: string): PresenceUser[] {
    return Array.from(this.presence.get(room)?.values() ?? []);
  }

  private enterPresence(room: string, user: PresenceUser): void {
    const users = this.presence.get(room) ?? new Map<string, PresenceUser>();
    const existing = users.get(user.userId);
    if (existing && JSON.stringify(existing) === JSON.stringify(user)) {
      return;
    }
    users.set(user.userId, user);
    this.presence.set(room, users);
    this.broadcastPresence(room);
  }

  private leavePresence(room: string, userId: string): void {
    const users = this.presence.get(room);
    if (!users) {
      return;
    }
    if (!users.has(userId)) {
      return;
    }
    users.delete(userId);
    if (users.size === 0) {
      this.presence.delete(room);
    }
    this.broadcastPresence(room);
  }

  private broadcastPresence(room: string): void {
    const users = this.listPresence(room);
    this.connections.forEach((connection) => {
      if (connection.hasSubscription(room)) {
        connection.sendPresence(room, users);
      }
    });
  }
}

export interface GatewayServerOptions extends RealtimeGatewayOptions {
  port?: number;
  server?: unknown;
  WebSocketServer?: new (options: Record<string, unknown>) => { on(event: string, handler: (ws: WebSocketLike) => void): unknown; close?(cb?: () => void): unknown };
}

export function createWebSocketGateway(options: GatewayServerOptions) {
  const gateway = new RealtimeGateway(options);
  const WebSocketServerCtor = options.WebSocketServer ?? optionalWsServer();
  if (!WebSocketServerCtor) {
    return { gateway, server: null };
  }

  const server = new WebSocketServerCtor(options.server ? { server: options.server } : { port: options.port ?? 8080 });
  server.on('connection', (ws: WebSocketLike) => gateway.handleConnection(ws));
  return { gateway, server };
}

export type ServeRealtimeOptions = ConnectRealtimeOptions & {
  websocket?: Omit<GatewayServerOptions, 'transport'>;
};

export async function serveRealtime(options: ServeRealtimeOptions): Promise<{
  transport: RealtimeTransport;
  gateway: RealtimeGateway;
  server: ReturnType<typeof createWebSocketGateway>['server'];
  close(): Promise<void>;
}> {
  const transport = await connectRealtime(options);
  const { gateway, server } = createWebSocketGateway({
    ...options.websocket,
    transport,
  });

  return {
    transport,
    gateway,
    server,
    async close() {
      await new Promise<void>((resolve) => {
        if (server?.close) {
          server.close(() => resolve());
          return;
        }
        resolve();
      });
      await gateway.close();
    },
  };
}

function optionalWsServer(): GatewayServerOptions['WebSocketServer'] | undefined {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require('ws').WebSocketServer;
  } catch {
    return undefined;
  }
}
