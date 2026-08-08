import type { RealtimeMiddleware, RealtimeTransport } from './core/types';
import { HybridTransport } from './transports/hybrid';
import { MongoTransport, type MongoTransportOptions } from './transports/mongo';
import { PostgresTransport, type PostgresTransportOptions } from './transports/postgres';
import { RedisTransport, type RedisTransportOptions } from './transports/redis';
import type { TransportBaseOptions } from './transports/base';

export type RealtimeEngineProvider = 'postgres' | 'mongo' | 'redis';

export type CreateRealtimeOptions =
  | ({ provider: 'postgres'; connection: PostgresTransportOptions | PostgresTransportOptions['client'] } & MiddlewareOption)
  | ({ provider: 'mongo'; connection: MongoTransportOptions } & MiddlewareOption)
  | ({ provider: 'redis'; connection: RedisTransportOptions | RedisTransportOptions['client'] } & MiddlewareOption)
  | ({ provider: 'hybrid'; storage: CreateRealtimeOptions; realtime: CreateRealtimeOptions } & MiddlewareOption);

export interface MiddlewareOption {
  middleware?: RealtimeMiddleware[];
}

export interface PostgresUriConnectionOptions {
  uri: string;
  eventsTable?: string;
  subscribersTable?: string;
  notifyChannel?: string;
  ssl?: boolean | Record<string, unknown>;
  autoMigrate?: boolean;
  closeClients?: boolean;
  resilience?: TransportBaseOptions;
}

export interface MongoUriConnectionOptions {
  uri: string;
  database?: string;
  eventsCollection?: string;
  countersCollection?: string;
  subscribersCollection?: string;
  snapshotsCollection?: string;
  dlqCollection?: string;
  collectionPrefix?: string;
  autoCreateIndexes?: boolean;
  closeClient?: boolean;
  resilience?: TransportBaseOptions;
}

export interface RedisUriConnectionOptions {
  uri: string;
  streamPrefix?: string;
  channelPrefix?: string;
  subscriberPrefix?: string;
  sequencePrefix?: string;
  idempotencyPrefix?: string;
  sequenceIndexPrefix?: string;
  snapshotPrefix?: string;
  dlqPrefix?: string;
  persistStreams?: boolean;
  closeClients?: boolean;
  resilience?: TransportBaseOptions;
}

export type ConnectRealtimeOptions =
  | ({ provider: 'postgres'; connection: string | PostgresUriConnectionOptions | PostgresTransportOptions | PostgresTransportOptions['client'] } & MiddlewareOption)
  | ({ provider: 'mongo'; connection: string | MongoUriConnectionOptions | MongoTransportOptions } & MiddlewareOption)
  | ({ provider: 'redis'; connection: string | RedisUriConnectionOptions | RedisTransportOptions | RedisTransportOptions['client'] } & MiddlewareOption)
  | ({ provider: 'hybrid'; storage: ConnectRealtimeOptions; realtime: ConnectRealtimeOptions } & MiddlewareOption);

type PostgresConnectConnection = Extract<ConnectRealtimeOptions, { provider: 'postgres' }>['connection'];
type MongoConnectConnection = Extract<ConnectRealtimeOptions, { provider: 'mongo' }>['connection'];
type RedisConnectConnection = Extract<ConnectRealtimeOptions, { provider: 'redis' }>['connection'];

export function createRealtime(options: CreateRealtimeOptions): RealtimeTransport {
  let transport: RealtimeTransport;
  if (options.provider === 'postgres') {
    transport = new PostgresTransport(isPostgresOptions(options.connection)
      ? options.connection
      : { client: options.connection });
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  if (options.provider === 'mongo') {
    transport = new MongoTransport(options.connection);
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  if (options.provider === 'redis') {
    transport = new RedisTransport(isRedisOptions(options.connection)
      ? options.connection
      : { client: options.connection });
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  transport = new HybridTransport({
    storage: createRealtime(options.storage),
    realtime: createRealtime(options.realtime),
  });
  applyMiddleware(transport, options.middleware);
  return transport;
}

export async function connectRealtime(options: ConnectRealtimeOptions): Promise<RealtimeTransport> {
  let transport: RealtimeTransport;
  if (options.provider === 'postgres') {
    transport = await connectPostgres(options.connection);
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  if (options.provider === 'mongo') {
    transport = await connectMongo(options.connection);
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  if (options.provider === 'redis') {
    transport = await connectRedis(options.connection);
    applyMiddleware(transport, options.middleware);
    return transport;
  }

  transport = new HybridTransport({
    storage: await connectRealtime(options.storage),
    realtime: await connectRealtime(asRealtimeFanout(options.realtime)),
  });
  applyMiddleware(transport, options.middleware);
  return transport;
}

function isPostgresOptions(value: PostgresTransportOptions | PostgresTransportOptions['client']): value is PostgresTransportOptions {
  return Boolean((value as PostgresTransportOptions).client);
}

function isRedisOptions(value: RedisTransportOptions | RedisTransportOptions['client']): value is RedisTransportOptions {
  return Boolean((value as RedisTransportOptions).client);
}

async function connectPostgres(connection: PostgresConnectConnection): Promise<RealtimeTransport> {
  if (typeof connection !== 'string' && isPostgresOptions(connection as any)) {
    return new PostgresTransport(connection as PostgresTransportOptions);
  }

  if (typeof connection !== 'string' && isPostgresClient(connection)) {
    return new PostgresTransport({ client: connection });
  }

  const options = normalizePostgresUriOptions(connection as string | PostgresUriConnectionOptions);
  const pg = loadOptionalDriver<any>('pg');
  const client = new pg.Pool({
    connectionString: options.uri,
    ssl: options.ssl,
  });
  const listenerClient = new pg.Client({
    connectionString: options.uri,
    ssl: options.ssl,
  });
  try {
    await listenerClient.connect();

    if (options.autoMigrate ?? defaultAutoMigrate()) {
      await ensurePostgresSchema(client, options.eventsTable, options.subscribersTable);
    }
  } catch (error) {
    await Promise.allSettled([
      client.end?.(),
      listenerClient.end?.(),
    ]);
    throw error;
  }

  const transport = new PostgresTransport({
    client,
    listenerClient,
    eventsTable: options.eventsTable,
    subscribersTable: options.subscribersTable,
    notifyChannel: options.notifyChannel,
    resilience: options.resilience,
  });

  return withOwnedClose(transport, async () => {
    if (options.closeClients ?? true) {
      await Promise.all([
        client.end?.(),
        listenerClient.end?.(),
      ]);
    }
  });
}

function defaultAutoMigrate(): boolean {
  return process.env.NODE_ENV !== 'production';
}

async function connectMongo(connection: MongoConnectConnection): Promise<RealtimeTransport> {
  if (typeof connection !== 'string' && isMongoOptions(connection as any)) {
    return new MongoTransport(connection as MongoTransportOptions);
  }

  const options = normalizeMongoUriOptions(connection as string | MongoUriConnectionOptions);
  const mongodb = loadOptionalDriver<any>('mongodb');
  const client = new mongodb.MongoClient(options.uri);
  let events: any;
  let counters: any;
  let subscribers: any;
  let snapshots: any;
  let dlq: any;
  try {
    await client.connect();

    const database = options.database ?? inferMongoDatabaseName(options.uri) ?? 'zero_realtime';
    const db = client.db(database);
    const prefix = options.collectionPrefix ?? 'realtime';
    events = db.collection(options.eventsCollection ?? `${prefix}_events`);
    counters = db.collection(options.countersCollection ?? `${prefix}_counters`);
    subscribers = db.collection(options.subscribersCollection ?? `${prefix}_subscribers`);
    snapshots = db.collection(options.snapshotsCollection ?? `${prefix}_snapshots`);
    dlq = db.collection(options.dlqCollection ?? `${prefix}_dlq`);

    if (options.autoCreateIndexes ?? true) {
      await Promise.all([
        events.createIndex?.({ room: 1, sequence: 1 }, { unique: true }),
        events.createIndex?.({ id: 1 }, { unique: true }),
        events.createIndex?.({ expiresAt: 1 }),
        subscribers.createIndex?.({ room: 1, subscriberId: 1 }, { unique: true }),
        snapshots.createIndex?.({ room: 1 }, { unique: true }),
        dlq.createIndex?.({ room: 1, failedAt: 1 }),
      ]);
    }
  } catch (error) {
    await client.close?.();
    throw error;
  }

  const transport = new MongoTransport({
    events,
    counters,
    subscribers,
    snapshots,
    dlq,
    resilience: options.resilience,
  });

  return withOwnedClose(transport, async () => {
    if (options.closeClient ?? true) {
      await client.close?.();
    }
  });
}

async function connectRedis(connection: RedisConnectConnection): Promise<RealtimeTransport> {
  if (typeof connection !== 'string' && isRedisOptions(connection as any)) {
    return new RedisTransport(connection as RedisTransportOptions);
  }

  if (typeof connection !== 'string' && isRedisClient(connection)) {
    return new RedisTransport({ client: connection });
  }

  const options = normalizeRedisUriOptions(connection as string | RedisUriConnectionOptions);
  const redis = loadOptionalDriver<any>('redis');
  const client = redis.createClient({ url: options.uri });
  const subscriberClient = client.duplicate();
  try {
    await Promise.all([client.connect(), subscriberClient.connect()]);
  } catch (error) {
    await Promise.allSettled([
      client.quit?.(),
      subscriberClient.quit?.(),
    ]);
    throw error;
  }

  return new RedisTransport({
    client,
    subscriberClient,
    streamPrefix: options.streamPrefix,
    channelPrefix: options.channelPrefix,
    subscriberPrefix: options.subscriberPrefix,
    sequencePrefix: options.sequencePrefix,
    idempotencyPrefix: options.idempotencyPrefix,
    sequenceIndexPrefix: options.sequenceIndexPrefix,
    snapshotPrefix: options.snapshotPrefix,
    dlqPrefix: options.dlqPrefix,
    persistStreams: options.persistStreams,
    closeClients: options.closeClients ?? true,
    resilience: options.resilience,
  });
}

function asRealtimeFanout(options: ConnectRealtimeOptions): ConnectRealtimeOptions {
  if (options.provider !== 'redis') {
    return options;
  }

  if (typeof options.connection === 'string') {
    return {
      provider: 'redis',
      connection: {
        uri: options.connection,
        persistStreams: false,
      },
    };
  }

  if (isRedisOptions(options.connection as any)) {
    return {
      provider: 'redis',
      connection: {
        ...(options.connection as RedisTransportOptions),
        persistStreams: (options.connection as RedisTransportOptions).persistStreams ?? false,
      },
    };
  }

  if (!isRedisClient(options.connection)) {
    return {
      provider: 'redis',
      connection: {
        ...(options.connection as RedisUriConnectionOptions),
        persistStreams: (options.connection as RedisUriConnectionOptions).persistStreams ?? false,
      },
    };
  }

  return options;
}

function normalizePostgresUriOptions(connection: string | PostgresUriConnectionOptions): PostgresUriConnectionOptions {
  return typeof connection === 'string' ? { uri: connection } : connection;
}

function normalizeMongoUriOptions(connection: string | MongoUriConnectionOptions): MongoUriConnectionOptions {
  return typeof connection === 'string' ? { uri: connection } : connection;
}

function normalizeRedisUriOptions(connection: string | RedisUriConnectionOptions): RedisUriConnectionOptions {
  return typeof connection === 'string' ? { uri: connection } : connection;
}

function isPostgresClient(value: unknown): value is PostgresTransportOptions['client'] {
  return Boolean(value && typeof (value as PostgresTransportOptions['client']).query === 'function');
}

function isMongoOptions(value: unknown): value is MongoTransportOptions {
  return Boolean(
    value
      && (value as MongoTransportOptions).events
      && (value as MongoTransportOptions).counters
      && (value as MongoTransportOptions).subscribers,
  );
}

function isRedisClient(value: unknown): value is RedisTransportOptions['client'] {
  return Boolean(value && (
    typeof (value as RedisTransportOptions['client']).publish === 'function'
    || typeof (value as RedisTransportOptions['client']).xAdd === 'function'
    || typeof (value as RedisTransportOptions['client']).incr === 'function'
  ));
}

function applyMiddleware(transport: RealtimeTransport, middleware: RealtimeMiddleware[] = []): void {
  middleware.forEach((item) => transport.use(item));
}

function loadOptionalDriver<T>(packageName: string): T {
  try {
    return require(packageName) as T;
  } catch (error) {
    const code = (error as { code?: string }).code;
    if (code !== 'MODULE_NOT_FOUND') {
      throw error;
    }
    throw new Error(
      `Missing bundled dependency "${packageName}". Reinstall @savanapoint/zero-pub-sub to restore its database drivers. If you are using a local link/file install, run yarn install or npm install inside the library before linking it.`,
    );
  }
}

async function ensurePostgresSchema(
  client: PostgresTransportOptions['client'],
  eventsTable = 'realtime_events',
  subscribersTable = 'realtime_subscribers',
): Promise<void> {
  assertSafePgIdentifier(eventsTable, 'eventsTable');
  assertSafePgIdentifier(subscribersTable, 'subscribersTable');
  const countersTable = `${eventsTable}_counters`;
  const snapshotsTable = `${eventsTable}_snapshots`;
  const dlqTable = `${eventsTable}_dlq`;

  await client.query(`
    CREATE TABLE IF NOT EXISTS ${eventsTable} (
      id TEXT PRIMARY KEY,
      room TEXT NOT NULL,
      sequence BIGINT NOT NULL,
      type TEXT NOT NULL,
      emitted_at TIMESTAMPTZ NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      payload JSONB NOT NULL,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      UNIQUE (room, sequence)
    )
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ${eventsTable}_room_sequence_idx ON ${eventsTable} (room, sequence)`);
  await client.query(`CREATE INDEX IF NOT EXISTS ${eventsTable}_expires_at_idx ON ${eventsTable} (expires_at)`);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${countersTable} (
      room TEXT PRIMARY KEY,
      sequence BIGINT NOT NULL DEFAULT 0
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${subscribersTable} (
      room TEXT NOT NULL,
      subscriber_id TEXT NOT NULL,
      last_ack_sequence BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (room, subscriber_id)
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${snapshotsTable} (
      room TEXT PRIMARY KEY,
      last_sequence BIGINT NOT NULL,
      state JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${dlqTable} (
      id TEXT PRIMARY KEY,
      room TEXT NOT NULL,
      original_event JSONB NOT NULL,
      error TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await client.query(`CREATE INDEX IF NOT EXISTS ${dlqTable}_room_failed_at_idx ON ${dlqTable} (room, failed_at)`);
}

function assertSafePgIdentifier(value: string, field: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(value)) {
    throw new Error(`Invalid PostgreSQL identifier for ${field}: ${value}`);
  }
}

function inferMongoDatabaseName(uri: string): string | undefined {
  try {
    const parsed = new URL(uri);
    const name = parsed.pathname.replace(/^\//, '').split('/')[0];
    return name || undefined;
  } catch {
    return undefined;
  }
}

function withOwnedClose<T extends RealtimeTransport>(transport: T, closeOwnedResources: () => Promise<void>): RealtimeTransport {
  return {
    publish: transport.publish.bind(transport),
    publishEphemeral: transport.publishEphemeral.bind(transport),
    subscribe: transport.subscribe.bind(transport),
    subscribeEphemeral: transport.subscribeEphemeral.bind(transport),
    replay: transport.replay.bind(transport),
    catchUp: transport.catchUp.bind(transport),
    streamReplay: transport.streamReplay.bind(transport),
    snapshot: transport.snapshot.bind(transport),
    getSnapshot: transport.getSnapshot.bind(transport),
    dlq: transport.dlq.bind(transport),
    ack: transport.ack.bind(transport),
    use: transport.use.bind(transport),
    snapshotMetrics: transport.snapshotMetrics?.bind(transport),
    health: transport.health?.bind(transport),
    async close() {
      await transport.close();
      await closeOwnedResources();
    },
  };
}
