# Plano de Implementacao: @savanapoint/zero-pub-sub como Fallback do WebSocket

## 1. Objetivo

Evoluir a biblioteca `@savanapoint/zero-pub-sub` para servir como fallback de realtime do ecossistema Zero quando o canal principal Socket.IO estiver indisponivel, instavel ou bloqueado pela rede do utilizador.

O fallback deve preservar o contrato funcional do realtime atual:

- entregar eventos de `user:*`, `vendor:*`, `driver:*`, `admin:*`, `chat:*` e `tracking:*`
- permitir bootstrap, delta incremental e recuperacao controlada
- evitar polling agressivo e refetch completo por evento
- suportar deduplicacao, ordenacao eventual e resync quando houver perda de eventos
- ser seguro para uso por cliente, vendor, driver e admin

Esta lib nao deve substituir o WebSocket como canal primario. Ela deve funcionar como transporte secundario acionado pela camada shared de realtime.

## 2. Estado Atual da Lib

Hoje a lib possui uma classe `PubSub` com duas operacoes principais:

- `publish(channel, message, subscribers)`
- `subscribe(channel, subscriberIds, onMessage)`

O modelo atual grava mensagens em:

```text
channels/{channel}/messages/{messageId}
```

Cada documento possui:

```ts
{
  message: string;
  timestamp: Timestamp;
  read: boolean;
  subscriber: string;
}
```

Esse modelo e suficiente para um MVP simples, mas nao atende as necessidades do ecossistema Zero como fallback realtime, porque:

- o payload e apenas string, sem envelope padronizado
- nao ha `eventId`, `sequence`, `version`, `updatedAt` ou `expiresAt`
- `read: true` e global por documento e nao representa cursor por consumidor
- `subscribe` nao retorna `unsubscribe`
- nao ha controle de backlog, TTL ou resync
- nao ha separacao clara entre publicador backend e assinante frontend
- nao ha API compativel com rooms do Socket.IO
- nao ha mecanismo de deduplicacao, ack ou observabilidade

## 3. Principios de Arquitetura

### 3.1 Socket.IO continua primario

O fluxo normal deve continuar sendo:

1. app conecta via Socket.IO
2. app faz `join` nas rooms autorizadas
3. backend publica evento via gateway
4. frontend aplica patch incremental no store local

O Firestore fallback entra apenas quando:

- Socket.IO nao conecta apos timeout configurado
- conexao cai repetidamente
- app retoma do background e precisa recuperar eventos perdidos
- ambiente/rede bloqueia WebSocket
- backend esta sem socket adapter operacional, mas ainda consegue persistir fallback

### 3.2 Mesma semantica em todos os transportes

O app nao deve tratar evento Firestore como evento diferente. A camada shared deve receber o mesmo envelope independentemente do transporte.

```ts
type RealtimeProvider = 'socket.io' | 'firestore-fallback';
```

O provider pode mudar, mas o contrato do evento nao.

### 3.3 Backend publica, frontend consome

Em producao, o frontend nao deve publicar eventos de dominio diretamente no Firestore. O backend e a fonte de verdade.

Permitido:

- backend publica eventos no Socket.IO
- backend publica copia dos eventos no Firestore fallback
- frontend le rooms autorizadas
- frontend confirma cursor/ack do proprio subscriber

Nao permitido por padrao:

- app cliente publicar `user:order:update`
- app vendor publicar `vendor:dashboard:update`
- app escrever em rooms arbitrarias

### 3.4 Fallback nao e historico permanente

Firestore deve guardar apenas uma janela curta de eventos para recuperacao.

Recomendacao inicial:

- TTL padrao: 24 horas
- backlog maximo por room: configuravel
- eventos antigos devem ser limpos por TTL nativo do Firestore ou job agendado

## 4. Envelope Padrao

A lib deve expor e validar um envelope unico.

```ts
export type RealtimeScope = 'user' | 'vendor' | 'driver' | 'admin' | 'chat' | 'tracking';

export type RealtimeAction =
  | 'created'
  | 'updated'
  | 'deleted'
  | 'snapshot'
  | 'read'
  | 'typing'
  | 'joined'
  | 'left'
  | 'resync_required';

export interface RealtimeEnvelope<TPayload = unknown> {
  id: string;
  type: string;
  scope: RealtimeScope;
  room: string;
  entityId?: string;
  action: RealtimeAction;
  sequence: number;
  version?: number;
  updatedAt: string;
  emittedAt: string;
  expiresAt: string;
  payload: TPayload;
  metadata?: {
    producer?: string;
    provider?: 'socket.io' | 'firestore-fallback';
    correlationId?: string;
    traceId?: string;
    tenantId?: string;
  };
}
```

Campos obrigatorios:

- `id`: identificador global unico do evento
- `type`: nome do evento, por exemplo `user:order:update`
- `scope`: dominio de autorizacao e roteamento
- `room`: room equivalente ao Socket.IO
- `action`: operacao semantica
- `sequence`: numero monotonicamente crescente por room
- `updatedAt`: timestamp da entidade/dominio
- `emittedAt`: timestamp de emissao do evento
- `expiresAt`: limite de retencao
- `payload`: dados suficientes para patch local

Campos recomendados:

- `entityId`: id da entidade afetada
- `version`: versao da entidade, quando existir
- `metadata.correlationId`: correlacionar mutation HTTP com evento realtime
- `metadata.traceId`: observabilidade distribuida

## 5. Rooms Suportadas

A lib deve tratar as mesmas rooms do gateway Socket.IO.

```text
user:{userId}
vendor:{vendorId}
driver:{driverUserId}
admin:realtime
chat:{conversationId}
tracking:{topic}
```

Helpers obrigatorios:

```ts
room.user(userId: string): string
room.vendor(vendorId: string): string
room.driver(driverUserId: string): string
room.admin(): 'admin:realtime'
room.chat(conversationId: string): string
room.tracking(topic: string): string
```

Validador obrigatorio:

```ts
parseRoom(room: string): {
  scope: RealtimeScope;
  resourceId?: string;
}
```

## 6. Schema Firestore Alvo

### 6.1 Colecoes

Estrutura recomendada:

```text
realtimeRooms/{encodedRoom}
  metadata
  events/{eventId}
  subscribers/{subscriberId}
```

`encodedRoom` deve ser seguro para path Firestore. Como rooms possuem `:`, a lib deve usar encoding estavel:

```ts
encodeRoom('user:123') => 'user__123'
decodeRoom('user__123') => 'user:123'
```

### 6.2 Documento da Room

```ts
interface RealtimeRoomDocument {
  room: string;
  scope: RealtimeScope;
  resourceId?: string;
  createdAt: Timestamp;
  updatedAt: Timestamp;
  lastSequence: number;
  eventCount?: number;
}
```

Path:

```text
realtimeRooms/{encodedRoom}
```

### 6.3 Documento de Evento

```ts
interface RealtimeEventDocument<TPayload = unknown> {
  id: string;
  room: string;
  scope: RealtimeScope;
  type: string;
  entityId?: string;
  action: RealtimeAction;
  sequence: number;
  version?: number;
  updatedAt: Timestamp;
  emittedAt: Timestamp;
  expiresAt: Timestamp;
  payload: TPayload;
  metadata?: Record<string, unknown>;
}
```

Path:

```text
realtimeRooms/{encodedRoom}/events/{eventId}
```

Indices previstos:

- `events`: `sequence ASC`
- `events`: `expiresAt ASC`
- `events`: `type ASC, sequence ASC`

### 6.4 Documento de Subscriber

```ts
interface RealtimeSubscriberDocument {
  subscriberId: string;
  room: string;
  lastAckSequence: number;
  lastSeenSequence: number;
  joinedAt: Timestamp;
  lastSeenAt: Timestamp;
  status: 'active' | 'idle' | 'closed';
  app?: 'client' | 'vendor' | 'driver' | 'admin' | 'backend';
  deviceId?: string;
}
```

Path:

```text
realtimeRooms/{encodedRoom}/subscribers/{subscriberId}
```

O subscriber pode ser:

- user id
- session id
- device id
- composicao `userId:deviceId`

Para apps moveis, a recomendacao e usar `userId:installationId`, evitando que uma sessao marque eventos como lidos por outra.

## 7. API Publica Alvo

### 7.1 Construcao

```ts
import { FirestoreFallbackTransport } from '@savanapoint/zero-pub-sub';

const fallback = new FirestoreFallbackTransport({
  firebaseConfig,
  subscriberId,
  app: 'client',
  defaultTtlMs: 24 * 60 * 60 * 1000,
  maxBacklogEvents: 500,
  logger,
});
```

Tambem deve aceitar instancias ja inicializadas:

```ts
new FirestoreFallbackTransport({
  app,
  firestore,
  subscriberId,
});
```

Isso evita inicializar multiplos Firebase Apps em aplicacoes que ja usam Firebase.

### 7.2 Publish

Uso esperado no backend:

```ts
await fallback.publish({
  room: 'user:123',
  type: 'user:order:update',
  entityId: orderId,
  action: 'updated',
  version: order.version,
  updatedAt: order.updatedAt,
  payload: {
    orderId,
    status: order.status,
    summary,
  },
});
```

Assinatura:

```ts
publish<TPayload>(
  event: PublishRealtimeEvent<TPayload>,
  options?: PublishOptions
): Promise<RealtimeEnvelope<TPayload>>
```

`PublishRealtimeEvent` nao recebe `id`, `sequence`, `emittedAt` ou `expiresAt`; a lib gera esses campos.

```ts
interface PublishRealtimeEvent<TPayload = unknown> {
  room: string;
  type: string;
  entityId?: string;
  action: RealtimeAction;
  version?: number;
  updatedAt?: string | Date;
  payload: TPayload;
  metadata?: RealtimeEnvelope['metadata'];
}
```

Opcoes:

```ts
interface PublishOptions {
  ttlMs?: number;
  eventId?: string;
  sequence?: number;
  idempotencyKey?: string;
}
```

### 7.3 Subscribe

Uso esperado no frontend:

```ts
const unsubscribe = fallback.subscribe(
  {
    room: 'vendor:abc',
    eventTypes: ['vendor:orders:update', 'vendor:dashboard:update'],
    from: 'cursor',
  },
  (event) => {
    vendorOpsStore.apply(event);
  },
  (error) => {
    realtimeDiagnostics.recordFallbackError(error);
  }
);
```

Assinatura:

```ts
subscribe<TPayload>(
  options: SubscribeOptions,
  onEvent: (event: RealtimeEnvelope<TPayload>) => void | Promise<void>,
  onError?: (error: Error) => void
): Unsubscribe
```

```ts
type Unsubscribe = () => Promise<void> | void;

interface SubscribeOptions {
  room: string;
  subscriberId?: string;
  eventTypes?: string[];
  from?: 'cursor' | 'now' | 'beginning' | { sequence: number };
  autoAck?: boolean;
  ackMode?: 'before-callback' | 'after-callback' | 'manual';
  includeExpired?: boolean;
  limit?: number;
}
```

Padrao recomendado:

- `from: 'cursor'`
- `autoAck: true`
- `ackMode: 'after-callback'`

### 7.4 Ack Manual

Para fluxos sensiveis:

```ts
const subscription = fallback.subscribeWithAck(options, async (event, ack, nack) => {
  try {
    await store.apply(event);
    await ack();
  } catch (error) {
    await nack(error);
  }
});
```

API:

```ts
subscribeWithAck<TPayload>(
  options: SubscribeOptions,
  onEvent: (
    event: RealtimeEnvelope<TPayload>,
    ack: () => Promise<void>,
    nack: (error?: unknown) => Promise<void>
  ) => void | Promise<void>,
  onError?: (error: Error) => void
): Unsubscribe
```

### 7.5 Recuperacao de Backlog

```ts
const result = await fallback.replay({
  room: 'user:123',
  fromSequence: 120,
  toSequence: 150,
});
```

```ts
interface ReplayResult<TPayload = unknown> {
  events: RealtimeEnvelope<TPayload>[];
  hasGap: boolean;
  lastSequence: number;
  resyncRequired: boolean;
}
```

### 7.6 Status

```ts
fallback.onStatusChange((status) => {
  console.log(status.state);
});
```

```ts
type FallbackConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'recovering'
  | 'degraded'
  | 'closed'
  | 'error';

interface FallbackStatus {
  provider: 'firestore-fallback';
  state: FallbackConnectionState;
  activeRooms: string[];
  subscriberId: string;
  lastEventAt?: string;
  lastError?: string;
}
```

## 8. Integracao com o Backend

### 8.1 Dual Publish

O backend deve publicar no Socket.IO e no fallback a partir do mesmo ponto de dominio.

Exemplo conceitual:

```ts
trackingGateway.publishUserEvent(userId, 'user:order:update', payload);

await realtimeFallback.publish({
  room: `user:${userId}`,
  type: 'user:order:update',
  entityId: orderId,
  action: 'updated',
  version: order.version,
  updatedAt: order.updatedAt,
  payload,
});
```

Para reduzir duplicacao, criar um publisher unificado no backend:

```ts
class RealtimePublisher {
  async publish(event: PublishRealtimeEvent) {
    socketTransport.publish(event);

    if (this.fallbackEnabled) {
      await firestoreFallback.publish(event);
    }
  }
}
```

### 8.2 Configuracao

Variaveis recomendadas:

```env
REALTIME_FALLBACK_ENABLED=true
REALTIME_FALLBACK_PROVIDER=firestore
REALTIME_FALLBACK_TTL_MS=86400000
REALTIME_FALLBACK_MAX_BACKLOG=500
REALTIME_FALLBACK_DUAL_PUBLISH=true
REALTIME_FALLBACK_PUBLISH_TIMEOUT_MS=1500
```

Regra importante:

- falha ao publicar fallback nao deve derrubar mutation principal
- falha deve ser logada com `correlationId`
- para eventos criticos, backend pode enfileirar retry interno

### 8.3 Eventos Prioritarios

Primeira fase:

- `user:order:update`
- `user:delivery:update`
- `user:notification`
- `vendor:orders:update`
- `vendor:dashboard:update`
- `vendor:bookings:update`
- `chat:message`
- `chat:read`
- `tracking:update`
- `tracking:snapshot`

Segunda fase:

- `admin:orders:update`
- `admin:deliveries:update`
- `admin:services:update`
- `admin:chat:update`
- `driver:offers:update`
- `driver:delivery:update`

## 9. Integracao com Frontend

### 9.1 Camada Shared de Realtime

Cada app deve consumir uma camada comum:

```ts
interface RealtimeClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  join(room: string): Promise<void>;
  leave(room: string): Promise<void>;
  on<TPayload>(type: string, handler: (event: RealtimeEnvelope<TPayload>) => void): Unsubscribe;
  status(): RealtimeClientStatus;
  resync(reason: ResyncReason): Promise<void>;
}
```

O fallback deve ser apenas um transport interno:

```ts
socketTransport -> primary
firestoreFallbackTransport -> secondary
```

### 9.2 Politica de Ativacao

Ativar fallback quando:

- socket nao conecta em `SOCKET_CONNECT_TIMEOUT_MS`
- socket passa por mais de `N` reconnects em janela curta
- app volta do background e ultimo evento tem idade maior que TTL local
- servidor informa `missedEvents: true`
- cliente detecta gap de `sequence`

Desativar fallback quando:

- socket volta a ficar saudavel
- backlog foi reconciliado
- nao ha rooms ativas

### 9.3 Deduplicacao Entre Socket e Fallback

O store compartilhado deve manter cache LRU de eventos aplicados:

```ts
appliedEventIds: LruSet<string>
lastSequenceByRoom: Map<string, number>
lastVersionByEntity: Map<string, number>
```

Regras:

- se `event.id` ja foi aplicado, ignorar
- se `event.sequence <= lastSequenceByRoom[room]`, ignorar ou avaliar replay
- se `event.version < localVersion`, ignorar
- se houver gap, disparar `resync('sequence_gap')`

### 9.4 Aplicacao no Store

Eventos devem ser ricos o suficiente para patch local.

Exemplo desejado:

```ts
{
  type: 'vendor:orders:update',
  entityId: 'order_123',
  action: 'updated',
  payload: {
    order: {
      id: 'order_123',
      status: 'ready',
      total: 450,
      customerName: 'Maria',
      updatedAt: '2026-04-30T20:10:00.000Z'
    },
    metricsPatch: {
      activeOrders: 7
    }
  }
}
```

Evitar payloads que apenas dizem "algo mudou", porque isso transforma realtime em gatilho de fetch.

## 10. Seguranca

### 10.1 Autorizacao

Regra base:

- cliente le apenas `user:{ownUserId}`, `chat:{conversationId autorizado}` e `tracking:{topic autorizado}`
- vendor le apenas `vendor:{ownVendorId}` e chats do proprio vendor
- driver le apenas `driver:{ownDriverUserId}` e deliveries atribuidas
- admin le `admin:realtime`, conforme role
- backend escreve em qualquer room autorizada pelo ambiente servidor

### 10.2 Firestore Rules

As rules devem impedir escrita direta por apps em `events`.

Modelo conceitual:

```text
allow read: if canReadRoom(request.auth, room);
allow create, update, delete: if isBackendServiceAccount();
allow update subscriber cursor: if isOwnSubscriber(request.auth, subscriberId);
```

Se o frontend precisar gravar ack/cursor, isso deve ser limitado ao proprio documento:

```text
realtimeRooms/{room}/subscribers/{subscriberId}
```

### 10.3 Service Account no Backend

Para publicacao segura, o backend deve usar Firebase Admin SDK ou credenciais de service account.

Recomendacao:

- pacote da lib deve separar build client e build admin
- `@savanapoint/zero-pub-sub/client`
- `@savanapoint/zero-pub-sub/admin`

O modulo admin pode usar `firebase-admin`.
O modulo client deve usar SDK web/mobile.

## 11. Performance e Custos

Firestore fallback tem custo por leitura/escrita. Por isso:

- nao usar fallback enquanto socket esta saudavel
- limitar rooms assinadas
- filtrar por `sequence > lastAckSequence`
- usar TTL
- compactar payload quando necessario
- evitar eventos de alta frequencia como typing em fallback, salvo quando indispensavel

Eventos que podem ser excluidos ou reduzidos no fallback:

- `chat:typing`
- presenca efemera
- updates de localizacao de alta frequencia

Para tracking de motorista, fallback deve priorizar milestones e snapshot, nao cada ponto GPS.

## 12. Observabilidade

A lib deve expor hooks de diagnostico:

```ts
interface RealtimeFallbackMetrics {
  published: number;
  received: number;
  acked: number;
  droppedDuplicate: number;
  droppedExpired: number;
  gapsDetected: number;
  resyncRequired: number;
  publishErrors: number;
  subscribeErrors: number;
  averageDeliveryLagMs: number;
}
```

Eventos de diagnostico:

- `fallback:enabled`
- `fallback:disabled`
- `fallback:publish:error`
- `fallback:subscribe:error`
- `fallback:gap`
- `fallback:resync_required`
- `fallback:backlog_replayed`

Logs devem incluir:

- `room`
- `eventId`
- `sequence`
- `subscriberId`
- `correlationId`
- `provider`

## 13. Tratamento de Erros

### 13.1 Publicacao

Falhas de publish no fallback:

- nao devem falhar a operacao principal de dominio
- devem retornar erro ao publisher se chamadas explicitamente aguardadas
- devem ser logadas e medidas
- podem ter retry com backoff em backend

### 13.2 Assinatura

Falhas de subscribe:

- devem mudar status para `degraded` ou `error`
- devem notificar callback `onError`
- devem tentar reconectar com backoff configuravel
- nao devem apagar cursor local

### 13.3 Gap de Eventos

Quando detectar gap:

```ts
if (event.sequence > lastSequence + 1) {
  emit('fallback:gap');
  const replay = await fallback.replay({ fromSequence: lastSequence + 1 });

  if (replay.resyncRequired) {
    await realtimeClient.resync('sequence_gap');
  }
}
```

## 14. Testes Necessarios

### 14.1 Unitarios

- encode/decode de rooms
- validacao de envelope
- geracao de `eventId`
- calculo de `expiresAt`
- deduplicacao por `id`
- comparacao por `sequence`
- comparacao por `version`
- parse de Timestamp Firestore para ISO string

### 14.2 Integracao com Emulator

Usar Firebase Emulator para:

- publicar evento
- assinar room
- confirmar ack
- replay a partir de sequence
- detectar gap
- filtrar eventTypes
- garantir que unsubscribe para listener
- garantir que TTL/expired seja ignorado

### 14.3 Contrato com Backend

Testes contra publisher unificado:

- publica Socket.IO e Firestore fallback
- fallback desativado nao escreve Firestore
- falha Firestore nao derruba mutation
- envelope publicado tem campos obrigatorios

### 14.4 Contrato com Frontend

Testes em client shared:

- socket saudavel nao ativa fallback
- timeout do socket ativa fallback
- retorno do socket desativa fallback apos reconcile
- evento duplicado socket + fallback aplica uma vez
- gap dispara resync

## 15. Mudancas no Pacote

### 15.1 Estrutura Recomendada

```text
src/
  index.ts
  client.ts
  admin.ts
  transport/
    FirestoreFallbackTransport.ts
    types.ts
  firestore/
    paths.ts
    converters.ts
    sequence.ts
  rooms.ts
  envelope.ts
  errors.ts
  metrics.ts
  testing/
    memoryTransport.ts
```

### 15.2 Exports

```ts
export * from './transport/types';
export * from './rooms';
export * from './envelope';
export { FirestoreFallbackTransport } from './transport/FirestoreFallbackTransport';
```

Exports separados:

```json
{
  "exports": {
    ".": "./dist/index.js",
    "./client": "./dist/client.js",
    "./admin": "./dist/admin.js"
  }
}
```

### 15.3 Dependencias

Revisar dependencias:

- remover `dotenv` do pacote client
- remover `sucrase` se nao houver uso real
- considerar `firebase` como peerDependency para client
- considerar `firebase-admin` como peerDependency opcional para admin

Exemplo:

```json
{
  "peerDependencies": {
    "firebase": "^10 || ^11"
  },
  "optionalPeerDependencies": {
    "firebase-admin": "^12"
  }
}
```

## 16. Plano de Implementacao

### Fase 0. Preparacao

- corrigir README atual
- corrigir exemplo de `subscribe`
- remover ou isolar `src/config.ts`
- retornar `unsubscribe` no `subscribe`
- instalar testes basicos

Done:

- build TypeScript passa
- README nao documenta `.catch()` em `subscribe` void
- sample compila contra API real

### Fase 1. Envelope e Rooms

- criar tipos `RealtimeEnvelope`, `PublishRealtimeEvent`, `SubscribeOptions`
- criar helpers de room
- criar validadores
- criar serializacao Firestore <-> envelope

Done:

- todos os eventos publicados possuem envelope completo
- testes unitarios cobrem room/envelope

### Fase 2. Firestore Schema Novo

- implementar paths `realtimeRooms/{room}/events`
- implementar documento de room com `lastSequence`
- implementar geracao atomica de sequence por transacao
- implementar escrita de evento
- implementar `expiresAt`

Done:

- publish cria/atualiza room
- publish incrementa sequence corretamente
- eventos aparecem ordenados por sequence

### Fase 3. Subscribe com Cursor

- criar documento de subscriber
- ler `lastAckSequence`
- assinar eventos `sequence > lastAckSequence`
- filtrar expired
- retornar `unsubscribe`
- implementar ack apos callback

Done:

- uma mensagem nao e marcada como lida globalmente
- cada subscriber mantem cursor proprio
- unsubscribe encerra listener

### Fase 4. Replay e Gap Detection

- implementar `replay`
- detectar gap de sequence
- emitir `resyncRequired`
- limitar replay por `maxBacklogEvents`

Done:

- cliente recupera backlog curto
- gap grande gera sinal de resync

### Fase 5. Backend Dual Publish

- criar adaptador no `zero-backend`
- publicar eventos prioritarios tambem no fallback
- adicionar env vars
- logar falhas sem derrubar fluxo principal

Done:

- `user:order:update` e `vendor:orders:update` chegam via fallback
- backend nao quebra quando Firestore falha

### Fase 6. Frontend Transport Adapter

- criar adaptador em `zero-Interface`, `zero-vendor`, `driver` e `zero-admin`
- ativar fallback por timeout/reconnect
- deduplicar socket + fallback
- desativar fallback quando socket recuperar

Done:

- app continua recebendo updates sem Socket.IO
- retorno do socket nao duplica eventos
- UI mostra estado discreto de fallback/reconnecting

### Fase 7. Observabilidade e Limpeza

- expor metricas
- adicionar logs estruturados
- configurar TTL Firestore
- criar alertas para uso excessivo de fallback

Done:

- dashboards mostram publish/receive/errors/gaps
- eventos expiram automaticamente
- custos ficam observaveis

## 17. Critérios de Aceite

A lib estara pronta para uso como fallback quando:

- entrega eventos com o mesmo envelope do Socket.IO
- suporta rooms padronizadas do ecossistema
- usa cursor por subscriber
- retorna unsubscribe
- tem ack apos callback
- deduplica eventos por id
- detecta gap por sequence
- suporta replay curto
- sinaliza `resyncRequired`
- possui TTL/retencao
- tem regras de seguranca claras
- passa testes unitarios e emulator tests
- backend consegue dual publish sem quebrar mutations
- frontend consegue alternar entre socket e fallback sem duplicar estado

## 18. Riscos e Mitigacoes

### Custo Firestore alto

Mitigacao:

- fallback apenas quando necessario
- TTL curto
- evitar eventos efemeros
- limitar rooms e backlog

### Duplicacao de eventos

Mitigacao:

- `eventId` obrigatorio
- cache LRU no frontend
- `sequence` por room

### Evento perdido

Mitigacao:

- cursor por subscriber
- replay por sequence
- resync controlado

### Escrita indevida por frontend

Mitigacao:

- backend como unico publisher
- Firestore rules bloqueando escrita em `events`
- ack restrito ao proprio subscriber

### Payload incompleto gerando refetch

Mitigacao:

- contrato de eventos ricos
- testes de contrato por evento prioritario
- fallback segue a mesma regra realtime-first: patch local antes de refetch

## 19. Decisao Recomendada

Transformar `@savanapoint/zero-pub-sub` em uma lib de transporte fallback, nao em uma camada de negocio.

Nome conceitual:

```text
FirestoreFallbackTransport
```

Responsabilidade da lib:

- persistir eventos realtime em Firestore
- entregar eventos por room/subscriber
- controlar cursor, ack, replay, TTL e dedupe basico
- expor status e metricas

Responsabilidade do ecossistema Zero:

- definir eventos de dominio
- publicar eventos ricos no backend
- aplicar patches nos stores frontend
- decidir quando ativar/desativar fallback
- executar resync HTTP quando necessario

