import type { PublishInput, RealtimeEnvelope, RealtimeTransport } from './types';

export class RealtimeRouter {
  constructor(private readonly transport: RealtimeTransport) {}

  publish<TPayload>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>) {
    return this.transport.publish(event);
  }

  route<TPayload>(event: PublishInput<TPayload> | RealtimeEnvelope<TPayload>) {
    return this.publish(event);
  }
}

