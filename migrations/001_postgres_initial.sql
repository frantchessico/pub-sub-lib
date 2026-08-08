CREATE TABLE IF NOT EXISTS realtime_events_counters (
  room TEXT PRIMARY KEY,
  sequence BIGINT NOT NULL CHECK (sequence >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS realtime_events (
  id TEXT NOT NULL,
  room TEXT NOT NULL,
  sequence BIGINT NOT NULL CHECK (sequence >= 0),
  type TEXT NOT NULL,
  emitted_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room, sequence),
  UNIQUE (id)
);

CREATE INDEX IF NOT EXISTS realtime_events_room_type_sequence_idx
  ON realtime_events (room, type, sequence);

CREATE INDEX IF NOT EXISTS realtime_events_expires_at_idx
  ON realtime_events (expires_at);

CREATE TABLE IF NOT EXISTS realtime_subscribers (
  room TEXT NOT NULL,
  subscriber_id TEXT NOT NULL,
  last_ack_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_ack_sequence >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (room, subscriber_id)
);

CREATE TABLE IF NOT EXISTS realtime_events_snapshots (
  room TEXT PRIMARY KEY,
  last_sequence BIGINT NOT NULL CHECK (last_sequence >= 0),
  state JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS realtime_events_dlq (
  id TEXT PRIMARY KEY,
  room TEXT NOT NULL,
  original_event JSONB NOT NULL,
  error TEXT NOT NULL,
  attempts INTEGER NOT NULL CHECK (attempts >= 0),
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS realtime_events_dlq_room_failed_at_idx
  ON realtime_events_dlq (room, failed_at);
