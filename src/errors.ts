export class PubSubError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly context?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'PubSubError';
  }
}

export class InvalidRoomError extends PubSubError {
  constructor(room: string) {
    super(`Invalid realtime room: ${room}`, 'invalid_room', { room });
    this.name = 'InvalidRoomError';
  }
}

export class InvalidEnvelopeError extends PubSubError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, 'invalid_envelope', context);
    this.name = 'InvalidEnvelopeError';
  }
}

export class InvalidProtocolMessageError extends PubSubError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, 'invalid_protocol_message', context);
    this.name = 'InvalidProtocolMessageError';
  }
}

export class BackpressureError extends PubSubError {
  constructor(message: string, context?: Record<string, unknown>) {
    super(message, 'backpressure', context);
    this.name = 'BackpressureError';
  }
}
