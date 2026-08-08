import type { RealtimeMiddleware } from './types';

export function loggerMiddleware(logger: Pick<Console, 'log' | 'error'> = console): RealtimeMiddleware {
  return async (ctx, next) => {
    const start = Date.now();
    logger.log(`[realtime] ${ctx.action} started`);
    try {
      await next();
      logger.log(`[realtime] ${ctx.action} completed in ${Date.now() - start}ms`);
    } catch (error) {
      logger.error(`[realtime] ${ctx.action} failed`, error);
      throw error;
    }
  };
}

export function metricsMiddleware(metrics: {
  increment(name: string, tags?: Record<string, string>): void;
  timing(name: string, value: number, tags?: Record<string, string>): void;
}): RealtimeMiddleware {
  return async (ctx, next) => {
    const start = Date.now();
    try {
      await next();
      metrics.increment('realtime.operation.success', { action: ctx.action });
    } catch (error) {
      metrics.increment('realtime.operation.error', { action: ctx.action });
      throw error;
    } finally {
      metrics.timing('realtime.operation.duration', Date.now() - start, { action: ctx.action });
    }
  };
}

export function tenantMiddleware(): RealtimeMiddleware {
  return async (ctx, next) => {
    if (ctx.action === 'publish' && !ctx.event?.metadata?.tenantId) {
      throw new Error('Missing tenantId');
    }
    await next();
  };
}

export function payloadSizeMiddleware(maxBytes: number): RealtimeMiddleware {
  return async (ctx, next) => {
    if (ctx.action === 'publish') {
      const size = Buffer.byteLength(JSON.stringify(ctx.event?.payload ?? null));
      if (size > maxBytes) {
        throw new Error(`Payload too large: ${size} bytes`);
      }
    }
    await next();
  };
}

export function maskSensitiveDataMiddleware(fields = ['password', 'cardNumber', 'cvv']): RealtimeMiddleware {
  return async (ctx, next) => {
    if (ctx.action === 'deliver' && ctx.envelope?.payload && typeof ctx.envelope.payload === 'object') {
      const payload = ctx.envelope.payload as Record<string, unknown>;
      fields.forEach((field) => {
        delete payload[field];
      });
    }
    await next();
  };
}
