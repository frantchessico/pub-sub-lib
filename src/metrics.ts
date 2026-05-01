import type { RealtimeFallbackMetrics } from './transport/types';

export function createInitialMetrics(): RealtimeFallbackMetrics {
  return {
    published: 0,
    received: 0,
    acked: 0,
    droppedDuplicate: 0,
    droppedExpired: 0,
    gapsDetected: 0,
    resyncRequired: 0,
    publishErrors: 0,
    subscribeErrors: 0,
    averageDeliveryLagMs: 0,
  };
}

export function updateAverage(currentAverage: number, currentCount: number, nextValue: number): number {
  if (currentCount <= 1) {
    return nextValue;
  }

  return ((currentAverage * (currentCount - 1)) + nextValue) / currentCount;
}
