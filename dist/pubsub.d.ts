import type { FirebaseOptions } from 'firebase/app';
import type { Unsubscribe } from './transport/types';
/**
 * Backward-compatible facade for the original PubSub API.
 *
 * New integrations should prefer FirestoreFallbackTransport directly because it
 * exposes envelopes, rooms, cursors, replay, ack, metrics and connection status.
 */
declare class PubSub {
    private readonly transport;
    constructor(firebaseConfig: FirebaseOptions, subscriberId?: string);
    publish(channel: string, message: string, subscribers: string[]): Promise<void>;
    subscribe(channel: string, subscriberIds: string[], onMessage: (message: string) => void | Promise<void>): Unsubscribe;
}
export default PubSub;
