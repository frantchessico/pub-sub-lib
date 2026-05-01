import { FirestoreFallbackTransport, room } from './src';

const firebaseConfig = {
  apiKey: process.env.FIREBASE_API_KEY,
  authDomain: process.env.FIREBASE_AUTH_DOMAIN,
  databaseURL: process.env.FIREBASE_DATABASE_URL,
  projectId: process.env.FIREBASE_PROJECT_ID,
  storageBucket: process.env.FIREBASE_STORAGE_BUCKET,
  messagingSenderId: process.env.FIREBASE_MESSAGING_SENDER_ID,
  appId: process.env.FIREBASE_APP_ID,
};

const fallback = new FirestoreFallbackTransport({
  firebaseConfig,
  subscriberId: 'zero:local-device',
  app: 'client',
});

const userRoom = room.user('zero');

const unsubscribe = fallback.subscribe(
  {
    room: userRoom,
    eventTypes: ['newsletter:message'],
    from: 'cursor',
  },
  (event) => {
    console.log('Received message:', event.payload);
  },
);

void fallback.publish({
  room: userRoom,
  type: 'newsletter:message',
  action: 'created',
  entityId: 'newsletter',
  payload: {
    name: 'Nome Exemplo',
    email: 'exemplo@example.com',
  },
});

process.on('SIGINT', async () => {
  await unsubscribe();
  await fallback.close();
  process.exit(0);
});
