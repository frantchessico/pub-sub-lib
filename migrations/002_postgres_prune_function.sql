CREATE OR REPLACE FUNCTION realtime_prune_expired_events(batch_size INTEGER DEFAULT 10000)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  deleted_count INTEGER;
BEGIN
  WITH expired AS (
    SELECT room, sequence
    FROM realtime_events
    WHERE expires_at <= NOW()
    ORDER BY expires_at ASC
    LIMIT batch_size
  )
  DELETE FROM realtime_events e
  USING expired
  WHERE e.room = expired.room
    AND e.sequence = expired.sequence;

  GET DIAGNOSTICS deleted_count = ROW_COUNT;
  RETURN deleted_count;
END;
$$;

