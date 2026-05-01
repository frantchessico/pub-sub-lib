import { initializeApp, getApps, type FirebaseApp } from 'firebase/app';
import {
  getDocs,
  getDoc,
  getFirestore,
  increment,
  limit as firestoreLimit,
  onSnapshot,
  orderBy,
  query,
  runTransaction,
  serverTimestamp,
  setDoc,
  updateDoc,
  where,
  type Firestore,
  type QueryConstraint,
} from 'firebase/firestore';
import { createEnvelope, DEFAULT_TTL_MS, isExpired, validateEnvelope } from '../envelope';
import { envelopeToDocument, documentToEnvelope, stripUndefinedDeep } from '../firestore/converters';
import { FirestoreRealtimePaths } from '../firestore/paths';
import { createInitialMetrics, updateAverage } from '../metrics';
import { parseRoom } from '../rooms';
import type {
  FallbackStatus,
  FirestoreFallbackTransportOptions,
  PublishOptions,
  PublishRealtimeEvent,
  RealtimeEnvelope,
  RealtimeFallbackMetrics,
  RealtimeLogger,
  ReplayOptions,
  ReplayResult,
  SubscribeOptions,
  Unsubscribe,
} from './types';

const DEFAULT_MAX_BACKLOG_EVENTS = 500;

export class FirestoreFallbackTransport {
  private readonly firestore: Firestore;
  private readonly paths: FirestoreRealtimePaths;
  private readonly subscriberId: string;
  private readonly appName?: FirestoreFallbackTransportOptions['app'];
  private readonly defaultTtlMs: number;
  private readonly maxBacklogEvents: number;
  private readonly logger?: RealtimeLogger;
  private readonly metricsState = createInitialMetrics();
  private readonly processedSubscriptionEvents = new Set<string>();
  private readonly activeSubscriptions = new Map<string, Unsubscribe>();
  private readonly subscriptionRooms = new Map<string, string>();
  private readonly cursorUpdateChains = new Map<string, Promise<void>>();
  private readonly statusListeners = new Set<(status: FallbackStatus) => void>();
  private statusState: FallbackStatus;

  constructor(options: FirestoreFallbackTransportOptions) {
    this.subscriberId = options.subscriberId;
    this.appName = options.app;
    this.defaultTtlMs = options.defaultTtlMs ?? DEFAULT_TTL_MS;
    this.maxBacklogEvents = options.maxBacklogEvents ?? DEFAULT_MAX_BACKLOG_EVENTS;
    this.logger = options.logger;

    this.firestore = options.firestore ?? getFirestoreFromOptions(options);
    this.paths = new FirestoreRealtimePaths(this.firestore, options.collectionName);
    this.statusState = {
      provider: 'firestore-fallback',
      state: 'idle',
      activeRooms: [],
      subscriberId: this.subscriberId,
    };
  }

  async publish<TPayload>(
    event: PublishRealtimeEvent<TPayload>,
    options?: PublishOptions,
  ): Promise<RealtimeEnvelope<TPayload>> {
    try {
      const parsedRoom = parseRoom(event.room);
      let envelope: RealtimeEnvelope<TPayload> | null = null;

      await runTransaction(this.firestore, async (transaction) => {
        const roomRef = this.paths.roomDoc(event.room);
        const eventId = options?.eventId ?? options?.idempotencyKey;
        const roomSnap = await transaction.get(roomRef);
        const currentSequence = roomSnap.exists() ? Number(roomSnap.data().lastSequence ?? 0) : 0;
        const sequence = options?.sequence ?? currentSequence + 1;

        envelope = createEnvelope(event, sequence, {
          eventId,
          ttlMs: options?.ttlMs ?? this.defaultTtlMs,
        });

        const eventRef = this.paths.eventDoc(event.room, envelope.id);
        const roomData = stripUndefinedDeep({
          room: event.room,
          scope: parsedRoom.scope,
          resourceId: parsedRoom.resourceId,
          createdAt: roomSnap.exists() ? roomSnap.data().createdAt : serverTimestamp(),
          updatedAt: serverTimestamp(),
          lastSequence: Math.max(sequence, currentSequence),
          eventCount: increment(1),
        });

        transaction.set(roomRef, roomData, { merge: true });
        transaction.set(eventRef, envelopeToDocument(envelope));
      });

      const publishedEnvelope = envelope as RealtimeEnvelope<TPayload> | null;
      if (!publishedEnvelope) {
        throw new Error('Failed to create realtime envelope');
      }

      this.metricsState.published += 1;
      this.logger?.debug?.('Published fallback realtime event', {
        room: publishedEnvelope.room,
        eventId: publishedEnvelope.id,
        type: publishedEnvelope.type,
        sequence: publishedEnvelope.sequence,
      });

      return publishedEnvelope;
    } catch (error) {
      this.metricsState.publishErrors += 1;
      this.setStatus('error', error);
      this.logger?.error?.('Failed to publish fallback realtime event', {
        room: event.room,
        type: event.type,
        error,
      });
      throw error;
    }
  }

  subscribe<TPayload>(
    options: SubscribeOptions,
    onEvent: (event: RealtimeEnvelope<TPayload>) => void | Promise<void>,
    onError?: (error: Error) => void,
  ): Unsubscribe {
    return this.createSubscription<TPayload>(
      options,
      async (event, ack, nack) => {
        try {
          await onEvent(event);
          if ((options.autoAck ?? true) && (options.ackMode ?? 'after-callback') === 'after-callback') {
            await ack();
          }
        } catch (error) {
          await nack(error);
          throw error;
        }
      },
      onError,
      false,
    );
  }

  subscribeWithAck<TPayload>(
    options: SubscribeOptions,
    onEvent: (
      event: RealtimeEnvelope<TPayload>,
      ack: () => Promise<void>,
      nack: (error?: unknown) => Promise<void>,
    ) => void | Promise<void>,
    onError?: (error: Error) => void,
  ): Unsubscribe {
    return this.createSubscription<TPayload>(options, onEvent, onError, true);
  }

  async replay<TPayload>(options: ReplayOptions): Promise<ReplayResult<TPayload>> {
    this.setStatus('recovering');
    const requestedLimit = Math.min(options.limit ?? this.maxBacklogEvents, this.maxBacklogEvents);
    const constraints: QueryConstraint[] = [
      where('sequence', '>=', options.fromSequence),
      orderBy('sequence', 'asc'),
      firestoreLimit(requestedLimit),
    ];

    if (options.toSequence !== undefined) {
      constraints.unshift(where('sequence', '<=', options.toSequence));
    }

    const eventsRef = this.paths.eventsCollection(options.room);
    const snapshot = await getDocs(query(eventsRef, ...constraints));
    const events: RealtimeEnvelope<TPayload>[] = [];
    let expectedSequence = options.fromSequence;
    let hasGap = false;

    snapshot.forEach((docSnap) => {
      const event = documentToEnvelope<TPayload>(docSnap.data() as any);
      if (!options.includeExpired && isExpired(event)) {
        this.metricsState.droppedExpired += 1;
        return;
      }

      if (options.eventTypes?.length && !options.eventTypes.includes(event.type)) {
        return;
      }

      if (event.sequence !== expectedSequence) {
        hasGap = true;
      }

      expectedSequence = event.sequence + 1;
      events.push(event);
    });

    const lastEvent = events.length > 0 ? events[events.length - 1] : undefined;
    const lastSequence = lastEvent?.sequence ?? options.fromSequence - 1;
    const resyncRequired = hasGap || snapshot.size >= this.maxBacklogEvents;

    if (hasGap) {
      this.metricsState.gapsDetected += 1;
    }

    if (resyncRequired) {
      this.metricsState.resyncRequired += 1;
    }

    this.setStatus(this.activeSubscriptions.size > 0 ? 'connected' : 'idle');

    return {
      events,
      hasGap,
      lastSequence,
      resyncRequired,
    };
  }

  async ack(room: string, sequence: number, subscriberId = this.subscriberId): Promise<void> {
    await this.updateSubscriberCursor(room, subscriberId, sequence, true);
    this.metricsState.acked += 1;
  }

  metrics(): RealtimeFallbackMetrics {
    return { ...this.metricsState };
  }

  status(): FallbackStatus {
    return { ...this.statusState, activeRooms: [...this.statusState.activeRooms] };
  }

  onStatusChange(listener: (status: FallbackStatus) => void): Unsubscribe {
    this.statusListeners.add(listener);
    listener(this.status());
    return () => {
      this.statusListeners.delete(listener);
    };
  }

  async close(): Promise<void> {
    const unsubscriptions = [...this.activeSubscriptions.values()];
    this.activeSubscriptions.clear();
    await Promise.all(unsubscriptions.map((unsubscribe) => Promise.resolve(unsubscribe())));
    this.setStatus('closed');
  }

  private createSubscription<TPayload>(
    options: SubscribeOptions,
    onEvent: (
      event: RealtimeEnvelope<TPayload>,
      ack: () => Promise<void>,
      nack: (error?: unknown) => Promise<void>,
    ) => void | Promise<void>,
    onError?: (error: Error) => void,
    manualAck = false,
  ): Unsubscribe {
    const subscriberId = options.subscriberId ?? this.subscriberId;
    const key = `${options.room}:${subscriberId}:${Math.random().toString(36).slice(2)}`;
    let firestoreUnsubscribe: Unsubscribe | null = null;
    let closed = false;

    this.setStatus('connecting');

    void this.startSubscription<TPayload>(
      key,
      subscriberId,
      options,
      async (event) => {
        const ack = async () => this.ack(event.room, event.sequence, subscriberId);
        const nack = async (error?: unknown) => {
          this.logger?.warn?.('Fallback realtime event was not acknowledged', {
            room: event.room,
            eventId: event.id,
            sequence: event.sequence,
            error,
          });
        };

        const ackMode = options.ackMode ?? 'after-callback';
        if (!manualAck && (options.autoAck ?? true) && ackMode === 'before-callback') {
          await ack();
        }

        await onEvent(event, ack, nack);
      },
      (unsubscribe) => {
        if (closed) {
          void Promise.resolve(unsubscribe());
          return;
        }

        firestoreUnsubscribe = unsubscribe;
        this.activeSubscriptions.set(key, unsubscribe);
        this.subscriptionRooms.set(key, options.room);
      },
      onError,
    );

    return async () => {
      closed = true;
      this.activeSubscriptions.delete(key);
      this.subscriptionRooms.delete(key);
      if (firestoreUnsubscribe) {
        await firestoreUnsubscribe();
      }

      await this.markSubscriberClosed(options.room, subscriberId);
      this.refreshActiveRoomsStatus();
    };
  }

  private async startSubscription<TPayload>(
    key: string,
    subscriberId: string,
    options: SubscribeOptions,
    onEvent: (event: RealtimeEnvelope<TPayload>) => Promise<void>,
    onReady: (unsubscribe: Unsubscribe) => void,
    onError?: (error: Error) => void,
  ): Promise<void> {
    try {
      parseRoom(options.room);
      const fromSequence = await this.resolveStartSequence(options, subscriberId);
      await this.ensureSubscriber(options.room, subscriberId, fromSequence);

      const requestedLimit = Math.min(options.limit ?? this.maxBacklogEvents, this.maxBacklogEvents);
      const eventsRef = this.paths.eventsCollection(options.room);
      const q = query(
        eventsRef,
        where('sequence', '>', fromSequence),
        orderBy('sequence', 'asc'),
        firestoreLimit(requestedLimit),
      );

      let lastSeenSequence = fromSequence;
      const unsubscribe = onSnapshot(
        q,
        (snapshot) => {
          void (async () => {
            for (const change of snapshot.docChanges()) {
              if (change.type !== 'added') {
                continue;
              }

              const event = documentToEnvelope<TPayload>(change.doc.data() as any);
              if (options.eventTypes?.length && !options.eventTypes.includes(event.type)) {
                continue;
              }

              if (!options.includeExpired && isExpired(event)) {
                this.metricsState.droppedExpired += 1;
                continue;
              }

              const subscriptionEventKey = `${key}:${event.id}`;
              if (this.processedSubscriptionEvents.has(subscriptionEventKey)) {
                this.metricsState.droppedDuplicate += 1;
                continue;
              }

              validateEnvelope(event);

              if (event.sequence > lastSeenSequence + 1) {
                this.metricsState.gapsDetected += 1;
                this.logger?.warn?.('Fallback realtime sequence gap detected', {
                  room: event.room,
                  expected: lastSeenSequence + 1,
                  received: event.sequence,
                  subscription: key,
                });
              }

              this.processedSubscriptionEvents.add(subscriptionEventKey);
              lastSeenSequence = Math.max(lastSeenSequence, event.sequence);
              await this.updateSubscriberCursor(event.room, subscriberId, event.sequence, false);
              this.recordReceivedEvent(event);
              await onEvent(event);
            }
          })().catch((error) => {
            this.handleSubscribeError(error, onError);
          });
        },
        (error) => {
          this.handleSubscribeError(error, onError);
        },
      );

      onReady(unsubscribe);
      this.setStatus('connected');
      this.refreshActiveRoomsStatus();
    } catch (error) {
      this.handleSubscribeError(error, onError);
    }
  }

  private async resolveStartSequence(options: SubscribeOptions, subscriberId: string): Promise<number> {
    if (typeof options.from === 'object') {
      return options.from.sequence;
    }

    if (options.from === 'beginning') {
      return 0;
    }

    const subscriberRef = this.paths.subscriberDoc(options.room, subscriberId);
    const subscriberSnap = await getDoc(subscriberRef);

    if (options.from === 'now') {
      const roomSnap = await getDoc(this.paths.roomDoc(options.room));
      return roomSnap.exists() ? Number(roomSnap.data().lastSequence ?? 0) : 0;
    }

    if (subscriberSnap.exists()) {
      return Number(subscriberSnap.data().lastAckSequence ?? 0);
    }

    return 0;
  }

  private async ensureSubscriber(room: string, subscriberId: string, fromSequence: number): Promise<void> {
    await setDoc(
      this.paths.subscriberDoc(room, subscriberId),
        stripUndefinedDeep({
          subscriberId,
          room,
          lastAckSequence: fromSequence,
          lastSeenSequence: fromSequence,
          lastSeenAt: serverTimestamp(),
          status: 'active',
          app: this.appName,
        }),
      { merge: true },
    );
  }

  private async markSubscriberClosed(room: string, subscriberId: string): Promise<void> {
    try {
      await updateDoc(this.paths.subscriberDoc(room, subscriberId), {
        status: 'closed',
        lastSeenAt: serverTimestamp(),
      });
    } catch (error) {
      this.logger?.warn?.('Failed to mark fallback subscriber closed', { room, subscriberId, error });
    }
  }

  private async updateSubscriberCursor(
    room: string,
    subscriberId: string,
    sequence: number,
    ack: boolean,
  ): Promise<void> {
    const updateKey = `${room}:${subscriberId}`;
    const previousUpdate = this.cursorUpdateChains.get(updateKey) ?? Promise.resolve();
    const nextUpdate = previousUpdate
      .catch(() => undefined)
      .then(() => this.writeSubscriberCursor(room, subscriberId, sequence, ack));

    this.cursorUpdateChains.set(updateKey, nextUpdate);

    try {
      await nextUpdate;
    } finally {
      if (this.cursorUpdateChains.get(updateKey) === nextUpdate) {
        this.cursorUpdateChains.delete(updateKey);
      }
    }
  }

  private async writeSubscriberCursor(
    room: string,
    subscriberId: string,
    sequence: number,
    ack: boolean,
  ): Promise<void> {
    const subscriberRef = this.paths.subscriberDoc(room, subscriberId);
    const cursorUpdate = stripUndefinedDeep({
      subscriberId,
      room,
      lastSeenSequence: sequence,
      lastAckSequence: ack ? sequence : undefined,
      lastSeenAt: serverTimestamp(),
      status: 'active',
      app: this.appName,
    });

    await setDoc(subscriberRef, cursorUpdate, { merge: true });
  }

  private recordReceivedEvent(event: RealtimeEnvelope): void {
    this.metricsState.received += 1;
    const lag = Date.now() - new Date(event.emittedAt).getTime();
    this.metricsState.averageDeliveryLagMs = updateAverage(
      this.metricsState.averageDeliveryLagMs,
      this.metricsState.received,
      Math.max(0, lag),
    );
    this.statusState.lastEventAt = new Date().toISOString();
  }

  private handleSubscribeError(error: unknown, onError?: (error: Error) => void): void {
    const normalizedError = error instanceof Error ? error : new Error(String(error));
    this.metricsState.subscribeErrors += 1;
    this.setStatus('error', normalizedError);
    this.logger?.error?.('Fallback realtime subscription error', { error: normalizedError });
    onError?.(normalizedError);
  }

  private setStatus(state: FallbackStatus['state'], error?: unknown): void {
    this.statusState = {
      ...this.statusState,
      state,
      lastError: error ? String(error instanceof Error ? error.message : error) : this.statusState.lastError,
    };
    this.emitStatus();
  }

  private refreshActiveRoomsStatus(): void {
    const activeRooms = [...new Set(this.subscriptionRooms.values())];
    this.statusState = {
      ...this.statusState,
      activeRooms,
      state: activeRooms.length > 0 ? 'connected' : this.statusState.state,
    };
    this.emitStatus();
  }

  private emitStatus(): void {
    const status = this.status();
    this.statusListeners.forEach((listener) => listener(status));
  }
}

function getFirestoreFromOptions(options: FirestoreFallbackTransportOptions): Firestore {
  if (options.firebaseApp) {
    return getFirestore(options.firebaseApp);
  }

  if (!options.firebaseConfig) {
    throw new Error('FirestoreFallbackTransport requires firestore, firebaseApp, or firebaseConfig');
  }

  const app: FirebaseApp = getApps().length > 0 ? getApps()[0] : initializeApp(options.firebaseConfig);
  return getFirestore(app);
}
