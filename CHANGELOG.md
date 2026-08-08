# Changelog

## 0.5.1

### Fixed

- **Gateway: ligação sem claims verificadas deixa de aceder a rooms.** Numa instância
  configurada com `jwt`, se `authorize` não estivesse definido a autorização de room
  passava sem exigir claims — a configuração mais permissiva tornava-se o caminho de
  menor resistência. Agora a ausência de `context.claims` recusa o acesso (`core/auth`).
- **Handshake: mensagens processadas em série.** O `init` é assíncrono porque verifica
  o token, e um cliente pode enviar `subscribe` logo a seguir sem esperar pelo `ready`.
  Sem fila, essas mensagens eram processadas com o `init` ainda pendente e falhavam em
  `assertInitialized` (`ws/connection`).

### Notes

- Ambas as correcções são do lado servidor/gateway. Os clientes consomem
  `@savanapoint/zero-pub-sub/client` e não executam este código, pelo que um consumidor
  em 0.5.0 não fica exposto — mas o gateway tem de correr 0.5.1.
- `zero-backend` consome a lib por `file:../pub-sub-lib` e já corre esta versão. Publicar
  no registry antes de qualquer consumidor externo passar a servir realtime.

## 0.5.0

### Added

- SSE stream parser (`sse-stream`) with composite resume cursors `room@sequence`.
- `createSseRealtimeClient` — fetch-based SSE client with Bearer auth (browser + React Native).
- `createResilientRealtimeClient` — WebSocket primary with automatic SSE fallback and dedupe by sequence.
- PostgreSQL migrations shipped in package: `migrations/001_postgres_initial.sql`, `002_postgres_prune_function.sql`.

### Notes

- SSE is a read-only fallback transport; publish/ephemeral remain WebSocket-only.
- Consumers should point SSE at backend `GET /api/realtime/sse` (see Zero INT-004B).

## 0.4.0

### Added

- CLI gateway: `npx zero-pub-sub gateway --provider mongo --connection "$MONGO_URL" --port 8080`.
- `serveRealtime()` one-call embedded gateway API.
- Cloud-mode APIs: `connectRealtimeCloud()` and `createRealtimeCloudClient()`.
- Distributed presence behavior through gateway presence state plus propagated ephemeral presence events.
- Wildcard room delivery for `scope:*` patterns with explicit room-scope semantics.
- Native Redis snapshots and DLQ storage.
- Native PostgreSQL snapshots and DLQ storage, including auto-migration tables.
- Middleware layer with Koa-style `before -> core -> after` execution.
- Official middleware helpers for logging, metrics, tenant enforcement, payload limits and sensitive data masking.
- 1M-event load-test profile through `REALTIME_LOAD_EVENTS_PER_ROOM=1000000 npm run load:test`.
- Expanded recipes for chat, notifications, banking, workers and multi-tenant apps.

### Changed

- Database drivers are bundled in the npm package for URI-based DX.
- Gateway presence now sends current room presence to late subscribers.
- Package prepared for public publishing with a clean changelog and CLI bin.

### Notes

- Wildcard subscriptions are scoped patterns such as `chat:*`; unrestricted global wildcards are intentionally not supported.
- Hosted cloud mode is an SDK contract. It requires a compatible Zero Pub/Sub Cloud endpoint.
