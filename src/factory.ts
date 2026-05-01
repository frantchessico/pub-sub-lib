import type { FirestoreFallbackTransportOptions } from './transport/types';
import type { RealtimeTransport } from './core/types';
import { FirestoreTransport } from './transports/firestore';
import { HybridTransport } from './transports/hybrid';
import { MongoTransport, type MongoTransportOptions } from './transports/mongo';
import { PostgresTransport, type PostgresTransportOptions } from './transports/postgres';
import { RedisTransport, type RedisTransportOptions } from './transports/redis';

export type RealtimeEngineProvider = 'postgres' | 'mongo' | 'redis' | 'firestore';

export type CreateRealtimeOptions =
  | ({ provider: 'postgres'; connection: PostgresTransportOptions | PostgresTransportOptions['client'] })
  | ({ provider: 'mongo'; connection: MongoTransportOptions })
  | ({ provider: 'redis'; connection: RedisTransportOptions | RedisTransportOptions['client'] })
  | ({ provider: 'firestore'; connection: FirestoreFallbackTransportOptions })
  | ({ provider: 'hybrid'; storage: CreateRealtimeOptions; realtime: CreateRealtimeOptions });

export function createRealtime(options: CreateRealtimeOptions): RealtimeTransport {
  if (options.provider === 'postgres') {
    return new PostgresTransport(isPostgresOptions(options.connection)
      ? options.connection
      : { client: options.connection });
  }

  if (options.provider === 'mongo') {
    return new MongoTransport(options.connection);
  }

  if (options.provider === 'redis') {
    return new RedisTransport(isRedisOptions(options.connection)
      ? options.connection
      : { client: options.connection });
  }

  if (options.provider === 'firestore') {
    return new FirestoreTransport(options.connection);
  }

  return new HybridTransport({
    storage: createRealtime(options.storage),
    realtime: createRealtime(options.realtime),
  });
}

function isPostgresOptions(value: PostgresTransportOptions | PostgresTransportOptions['client']): value is PostgresTransportOptions {
  return Boolean((value as PostgresTransportOptions).client);
}

function isRedisOptions(value: RedisTransportOptions | RedisTransportOptions['client']): value is RedisTransportOptions {
  return Boolean((value as RedisTransportOptions).client);
}
