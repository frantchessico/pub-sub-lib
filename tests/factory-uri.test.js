const assert = require('node:assert/strict');
const Module = require('node:module');
const test = require('node:test');
const { connectRealtime } = require('../dist/factory');

async function withMockedDrivers(drivers, run) {
  const originalLoad = Module._load;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (Object.prototype.hasOwnProperty.call(drivers, request)) {
      return drivers[request];
    }
    return originalLoad.apply(this, [request, parent, isMain]);
  };

  try {
    await run();
  } finally {
    Module._load = originalLoad;
  }
}

test('connectRealtime creates Mongo transport from URI with default collections and indexes', async () => {
  const clients = [];
  const collections = new Map();

  class FakeMongoClient {
    constructor(uri) {
      this.uri = uri;
      this.connected = false;
      this.closed = false;
      clients.push(this);
    }

    async connect() {
      this.connected = true;
    }

    db(name) {
      this.database = name;
      return {
        collection(collectionName) {
          const collection = new FakeCollection(collectionName);
          collections.set(collectionName, collection);
          return collection;
        },
      };
    }

    async close() {
      this.closed = true;
    }
  }

  class FakeCollection {
    constructor(name) {
      this.name = name;
      this.indexes = [];
    }

    async createIndex(fields, options) {
      this.indexes.push({ fields, options });
    }

    watch() {
      return { on() {}, close() {} };
    }

    async insertOne() {}

    find() {
      return { sort: () => ({ limit: () => ({ async toArray() { return []; } }) }) };
    }

    async updateOne() {}

    async findOneAndUpdate() {
      return { value: { sequence: 1 } };
    }
  }

  await withMockedDrivers({ mongodb: { MongoClient: FakeMongoClient } }, async () => {
    const transport = await connectRealtime({
      provider: 'mongo',
      connection: 'mongodb://localhost:27017/app_realtime',
    });

    assert.equal(clients[0].connected, true);
    assert.equal(clients[0].database, 'app_realtime');
    assert(collections.has('realtime_events'));
    assert(collections.has('realtime_counters'));
    assert(collections.has('realtime_subscribers'));
    assert.equal(collections.get('realtime_events').indexes.length, 3);

    await transport.close();
    assert.equal(clients[0].closed, true);
  });
});

test('connectRealtime creates Postgres transport from URI and migrates schema', async () => {
  const pools = [];
  const clients = [];

  class FakePool {
    constructor(options) {
      this.options = options;
      this.queries = [];
      pools.push(this);
    }

    async query(sql, params = []) {
      this.queries.push({ sql, params });
      return { rows: [] };
    }

    async end() {
      this.closed = true;
    }
  }

  class FakeClient extends FakePool {
    constructor(options) {
      super(options);
      clients.push(this);
    }

    async connect() {
      this.connected = true;
    }
  }

  await withMockedDrivers({ pg: { Pool: FakePool, Client: FakeClient } }, async () => {
    const transport = await connectRealtime({
      provider: 'postgres',
      connection: 'postgres://user:pass@localhost:5432/app',
    });

    assert.equal(pools[0].options.connectionString, 'postgres://user:pass@localhost:5432/app');
    assert.equal(clients[0].connected, true);
    assert(pools[0].queries.some((query) => query.sql.includes('CREATE TABLE IF NOT EXISTS realtime_events')));

    await transport.close();
    assert.equal(pools[0].closed, true);
    assert.equal(clients[0].closed, true);
  });
});

test('connectRealtime creates Redis transport from URI with duplicated subscriber client', async () => {
  const clients = [];

  function createFakeRedisClient(url, parent) {
    const client = {
      url,
      parent,
      async connect() {
        this.connected = true;
      },
      duplicate() {
        const duplicate = createFakeRedisClient(url, this);
        clients.push(duplicate);
        return duplicate;
      },
      async incr() {
        return 1;
      },
      async xAdd() {},
      async publish() {
        return 1;
      },
      async subscribe() {},
      async unsubscribe() {},
      async hSet() {},
      async quit() {
        this.closed = true;
      },
    };
    return client;
  }

  await withMockedDrivers({
    redis: {
      createClient(options) {
        const client = createFakeRedisClient(options.url);
        clients.push(client);
        return client;
      },
    },
  }, async () => {
    const transport = await connectRealtime({
      provider: 'redis',
      connection: 'redis://localhost:6379',
    });

    assert.equal(clients.length, 2);
    assert.equal(clients[0].connected, true);
    assert.equal(clients[1].connected, true);
    assert.equal(clients[1].parent, clients[0]);

    await transport.close();
    assert.equal(clients[0].closed, true);
    assert.equal(clients[1].closed, true);
  });
});

test('connectRealtime closes Postgres clients when migration fails', async () => {
  const pools = [];
  const clients = [];

  class FakePool {
    constructor() {
      pools.push(this);
    }

    async query(sql) {
      if (sql.includes('CREATE TABLE')) {
        throw new Error('migration failed');
      }
      return { rows: [] };
    }

    async end() {
      this.closed = true;
    }
  }

  class FakeClient extends FakePool {
    constructor() {
      super();
      clients.push(this);
    }

    async connect() {
      this.connected = true;
    }
  }

  await withMockedDrivers({ pg: { Pool: FakePool, Client: FakeClient } }, async () => {
    await assert.rejects(
      () => connectRealtime({ provider: 'postgres', connection: 'postgres://localhost/app' }),
      /migration failed/,
    );

    assert.equal(pools[0].closed, true);
    assert.equal(clients[0].closed, true);
  });
});

test('connectRealtime closes Redis clients when subscriber connection fails', async () => {
  const clients = [];

  function makeClient(shouldFail = false) {
    const client = {
      async connect() {
        if (shouldFail) {
          throw new Error('connect failed');
        }
      },
      duplicate() {
        const duplicate = makeClient(true);
        clients.push(duplicate);
        return duplicate;
      },
      async quit() {
        this.closed = true;
      },
    };
    return client;
  }

  await withMockedDrivers({
    redis: {
      createClient() {
        const client = makeClient();
        clients.push(client);
        return client;
      },
    },
  }, async () => {
    await assert.rejects(
      () => connectRealtime({ provider: 'redis', connection: 'redis://localhost:6379' }),
      /connect failed/,
    );

    assert.equal(clients[0].closed, true);
    assert.equal(clients[1].closed, true);
  });
});
