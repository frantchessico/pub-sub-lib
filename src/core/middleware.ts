import type {
  RealtimeMiddleware,
  RealtimeMiddlewareContext,
  RealtimeStorageProvider,
} from './types';

export type MiddlewareContextInput = Omit<
  RealtimeMiddlewareContext,
  'provider' | 'startedAt' | 'metadata' | 'set' | 'get'
> & {
  provider?: RealtimeStorageProvider | 'hybrid';
  startedAt?: number;
  metadata?: Record<string, unknown>;
};

export function createMiddlewareContext(
  provider: RealtimeStorageProvider | 'hybrid',
  input: MiddlewareContextInput,
): RealtimeMiddlewareContext {
  const store = new Map<string, unknown>();
  const metadata = input.metadata ?? {};
  return {
    ...input,
    provider: input.provider ?? provider,
    startedAt: input.startedAt ?? Date.now(),
    metadata,
    requestId: input.requestId ?? getString(metadata.requestId),
    traceId: input.traceId ?? getString(metadata.traceId),
    tenantId: input.tenantId ?? getString(metadata.tenantId),
    set(key, value) {
      store.set(key, value);
    },
    get(key) {
      return store.get(key) as never;
    },
  };
}

export async function runMiddlewares(
  middlewares: RealtimeMiddleware[],
  ctx: RealtimeMiddlewareContext,
  core: () => Promise<void> | void,
): Promise<void> {
  let index = -1;

  async function dispatch(i: number): Promise<void> {
    if (i <= index) {
      throw new Error('next() called multiple times');
    }

    index = i;
    const middleware = middlewares[i];
    if (!middleware) {
      await core();
      return;
    }

    await middleware(ctx, () => dispatch(i + 1));
  }

  try {
    await dispatch(0);
  } catch (error) {
    ctx.error = error;
    throw error;
  }
}

function getString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined;
}
