const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');
const test = require('node:test');
const { PostgresTransport } = require('../dist/transports/postgres');

class FakePostgresClient {
  constructor() {
    this.sequenceByRoom = new Map();
    this.events = [];
    this.queries = [];
  }

  async query(sql, params = []) {
    this.queries.push({ sql, params });

    if (sql.includes('WITH existing AS')) {
      const existing = this.events.find((event) => event[0] === params[0]);
      if (existing) {
        return { rows: [this.rowFromEvent(existing)] };
      }

      const sequence = params[2] ?? ((this.sequenceByRoom.get(params[1]) ?? 0) + 1);
      this.sequenceByRoom.set(params[1], sequence);
      const event = [
        params[0],
        params[1],
        sequence,
        params[3],
        params[4],
        params[5],
        params[6],
        params[7],
      ];
      this.events.push(event);
      return { rows: [this.rowFromEvent(event)] };
    }

    if (sql.includes('RETURNING sequence')) {
      const room = params[0];
      const sequence = (this.sequenceByRoom.get(room) ?? 0) + 1;
      this.sequenceByRoom.set(room, sequence);
      return { rows: [{ sequence }] };
    }

    if (sql.includes('WHERE id = $1')) {
      const row = this.events.find((event) => event[0] === params[0]);
      if (!row) {
        return { rows: [] };
      }
      return {
        rows: [{
          id: row[0],
          room: row[1],
          sequence: row[2],
          type: row[3],
          emitted_at: row[4],
          expires_at: row[5],
          payload: row[6],
          metadata: row[7],
        }],
      };
    }

    if (sql.includes('INSERT INTO realtime_events')) {
      this.events.push(params);
      return { rows: [] };
    }

    if (sql.includes('SELECT id, room, sequence')) {
      const [room, fromSequence] = params;
      return {
        rows: this.events
          .filter((event) => event[1] === room && event[2] > fromSequence)
          .map((event) => ({
            id: event[0],
            room: event[1],
            sequence: event[2],
            type: event[3],
            emitted_at: event[4],
            expires_at: event[5],
            payload: event[6],
            metadata: event[7],
          })),
      };
    }

    return { rows: [] };
  }

  rowFromEvent(event) {
    return {
      id: event[0],
      room: event[1],
      sequence: event[2],
      type: event[3],
      emitted_at: event[4],
      expires_at: event[5],
      payload: event[6],
      metadata: event[7],
    };
  }
}

class FakeListenerClient extends EventEmitter {
  constructor() {
    super();
    this.queries = [];
  }

  async query(sql) {
    this.queries.push(sql);
    return { rows: [] };
  }
}

test('PostgresTransport starts LISTEN for multi-instance fanout and dedupes notifications', async () => {
  const client = new FakePostgresClient();
  const listenerClient = new FakeListenerClient();
  const transport = new PostgresTransport({ client, listenerClient });
  const received = [];

  transport.subscribe({ room: 'order:123' }, (event) => received.push(event));
  await new Promise((resolve) => setImmediate(resolve));

  assert(listenerClient.queries.includes('LISTEN realtime_events'));

  const event = await transport.publish({ room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });
  listenerClient.emit('notification', { channel: 'realtime_events', payload: JSON.stringify(event) });
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(event.sequence, 1);
  assert.equal(received.length, 1);
  assert.equal(received[0].id, event.id);
});

test('PostgresTransport rejects unsafe identifiers', () => {
  assert.throws(
    () => new PostgresTransport({ client: new FakePostgresClient(), eventsTable: 'realtime_events;DROP' }),
    /Invalid PostgreSQL identifier/,
  );
});

test('PostgresTransport returns existing event for repeated idempotent publish', async () => {
  const client = new FakePostgresClient();
  const transport = new PostgresTransport({ client });

  const first = await transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });
  const second = await transport.publish({ id: 'evt_fixed', room: 'order:123', type: 'status.changed', payload: { status: 'ready' } });

  assert.equal(first.id, second.id);
  assert.equal(first.sequence, second.sequence);
  assert.equal(client.events.length, 1);
});

test('PostgresTransport exposes health status', async () => {
  const transport = new PostgresTransport({ client: new FakePostgresClient() });
  const health = await transport.health();

  assert.equal(health.provider, 'postgres');
  assert.equal(health.status, 'healthy');
});
