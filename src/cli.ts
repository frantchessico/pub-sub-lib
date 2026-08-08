#!/usr/bin/env node
import { serveRealtime, type ServeRealtimeOptions } from './ws/gateway';

type Args = Record<string, string | boolean>;

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  const command = String(args._ ?? args.command ?? '');
  if (command !== 'gateway') {
    printHelp();
    process.exitCode = command ? 1 : 0;
    return;
  }

  const provider = String(args.provider ?? process.env.ZERO_PUBSUB_PROVIDER ?? '');
  const connection = String(args.connection ?? process.env.ZERO_PUBSUB_CONNECTION ?? '');
  const port = Number(args.port ?? process.env.PORT ?? 8080);
  if (!provider || !['mongo', 'postgres', 'redis'].includes(provider)) {
    throw new Error('--provider must be one of: mongo, postgres, redis');
  }
  if (!connection) {
    throw new Error('--connection or ZERO_PUBSUB_CONNECTION is required');
  }

  const app = await serveRealtime({
    provider: provider as ServeRealtimeOptions['provider'],
    connection,
    websocket: {
      port,
      limits: {
        allowClientPublish: args['allow-client-publish'] === true || args.allowClientPublish === true,
      },
    },
  } as ServeRealtimeOptions);

  console.log(`[zero-pub-sub] gateway listening on ws://0.0.0.0:${port}`);
  process.on('SIGINT', () => void shutdown(app));
  process.on('SIGTERM', () => void shutdown(app));
}

async function shutdown(app: Awaited<ReturnType<typeof serveRealtime>>): Promise<void> {
  console.log('[zero-pub-sub] shutting down gateway');
  await app.close();
  process.exit(0);
}

function parseArgs(values: string[]): Args {
  const args: Args = {};
  const positional: string[] = [];
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith('--')) {
      positional.push(value);
      continue;
    }
    const key = value.slice(2);
    const next = values[index + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }
    args[key] = next;
    index += 1;
  }
  args._ = positional[0] ?? '';
  return args;
}

function printHelp(): void {
  console.log(`Usage:
  zero-pub-sub gateway --provider mongo --connection "$MONGO_URL" --port 8080

Options:
  --provider              mongo | postgres | redis
  --connection            Provider connection URI
  --port                  WebSocket port (default: 8080)
  --allow-client-publish  Allow clients to publish through the gateway
`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
