import { collection, doc, type CollectionReference, type DocumentReference, type Firestore } from 'firebase/firestore';
import { encodeRoom } from '../rooms';

export class FirestoreRealtimePaths {
  constructor(
    private readonly firestore: Firestore,
    private readonly rootCollection = 'realtimeRooms',
  ) {}

  roomDoc(room: string): DocumentReference {
    return doc(this.firestore, this.rootCollection, encodeRoom(room));
  }

  eventsCollection(room: string): CollectionReference {
    return collection(this.roomDoc(room), 'events');
  }

  eventDoc(room: string, eventId: string): DocumentReference {
    return doc(this.eventsCollection(room), eventId);
  }

  subscribersCollection(room: string): CollectionReference {
    return collection(this.roomDoc(room), 'subscribers');
  }

  subscriberDoc(room: string, subscriberId: string): DocumentReference {
    return doc(this.subscribersCollection(room), subscriberId);
  }
}
