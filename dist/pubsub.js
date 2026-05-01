"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const FirestoreFallbackTransport_1 = require("./transport/FirestoreFallbackTransport");
/**
 * Backward-compatible facade for the original PubSub API.
 *
 * New integrations should prefer FirestoreFallbackTransport directly because it
 * exposes envelopes, rooms, cursors, replay, ack, metrics and connection status.
 */
class PubSub {
    constructor(firebaseConfig, subscriberId = 'legacy-subscriber') {
        this.transport = new FirestoreFallbackTransport_1.FirestoreFallbackTransport({
            firebaseConfig,
            subscriberId,
            app: 'client',
        });
    }
    publish(channel, message, subscribers) {
        return __awaiter(this, void 0, void 0, function* () {
            yield Promise.all(subscribers.map((subscriber) => this.transport.publish({
                room: `user:${subscriber}`,
                type: `${channel}:message`,
                entityId: channel,
                action: 'created',
                payload: {
                    channel,
                    message,
                    subscriber,
                },
            })));
        });
    }
    subscribe(channel, subscriberIds, onMessage) {
        const unsubscribers = subscriberIds.map((subscriberId) => this.transport.subscribe({
            room: `user:${subscriberId}`,
            subscriberId,
            eventTypes: [`${channel}:message`],
            from: 'cursor',
        }, (event) => __awaiter(this, void 0, void 0, function* () {
            yield onMessage(event.payload.message);
        })));
        return () => __awaiter(this, void 0, void 0, function* () {
            yield Promise.all(unsubscribers.map((unsubscribe) => Promise.resolve(unsubscribe())));
        });
    }
}
exports.default = PubSub;
