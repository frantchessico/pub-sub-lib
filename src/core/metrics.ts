import type { RealtimeMetrics } from './types';

export function createRealtimeMetrics(): RealtimeMetrics {
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

export class MetricsCounter {
  private readonly state = createRealtimeMetrics();

  snapshot(): RealtimeMetrics {
    return { ...this.state };
  }

  increment(metric: keyof RealtimeMetrics, value = 1): void {
    this.state[metric] += value;
  }

  set(metric: keyof RealtimeMetrics, value: number): void {
    this.state[metric] = value;
  }

  recordDeliveryLag(lagMs: number): void {
    const received = Math.max(this.state.received, 1);
    this.state.averageDeliveryLagMs = (
      (this.state.averageDeliveryLagMs * (received - 1)) + Math.max(0, lagMs)
    ) / received;
  }
}
