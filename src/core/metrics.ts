import type { RealtimeMetrics } from './types';

export function createRealtimeMetrics(): RealtimeMetrics {
  return {
    published: 0,
    received: 0,
    acked: 0,
    gapsDetected: 0,
    errors: 0,
  };
}

export class MetricsCounter {
  private readonly state = createRealtimeMetrics();

  snapshot(): RealtimeMetrics {
    return { ...this.state };
  }

  increment(metric: keyof RealtimeMetrics, value = 1): void {
    this.state[metric] += value;
  }
}

