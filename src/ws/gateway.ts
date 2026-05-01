import type { AuthConfig } from '../core/auth';
import type { RealtimeTransport } from '../core/types';
import { Connection } from './connection';
import type { WebSocketLike } from './protocol';

export interface RealtimeGatewayOptions {
  transport: RealtimeTransport;
  auth?: AuthConfig;
}

export class RealtimeGateway {
  private readonly connections = new Map<string, Connection>();
  private sequence = 0;

  constructor(private readonly options: RealtimeGatewayOptions) {}

  handleConnection(ws: WebSocketLike): Connection {
    const id = `conn_${Date.now().toString(36)}_${++this.sequence}`;
    const connection = new Connection({
      id,
      ws,
      transport: this.options.transport,
      auth: this.options.auth,
      onClose: (connectionId) => this.connections.delete(connectionId),
    });
    this.connections.set(id, connection);
    connection.bind();
    return connection;
  }

  connectionCount(): number {
    return this.connections.size;
  }

  async close(): Promise<void> {
    this.connections.forEach((connection) => connection.disconnect());
    this.connections.clear();
    await this.options.transport.close();
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

function optionalWsServer(): GatewayServerOptions['WebSocketServer'] | undefined {
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    return require('ws').WebSocketServer;
  } catch {
    return undefined;
  }
}

