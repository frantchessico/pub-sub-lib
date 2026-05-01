import type { Unsubscribe } from '../core/types';

export class RoomManager {
  private readonly rooms = new Map<string, Map<string, Unsubscribe>>();

  add(connectionId: string, room: string, unsubscribe: Unsubscribe): void {
    const roomConnections = this.rooms.get(room) ?? new Map<string, Unsubscribe>();
    const previous = roomConnections.get(connectionId);
    if (previous) {
      void previous();
    }
    roomConnections.set(connectionId, unsubscribe);
    this.rooms.set(room, roomConnections);
  }

  remove(connectionId: string, room: string): void {
    const roomConnections = this.rooms.get(room);
    const unsubscribe = roomConnections?.get(connectionId);
    if (unsubscribe) {
      void unsubscribe();
    }
    roomConnections?.delete(connectionId);
    if (roomConnections?.size === 0) {
      this.rooms.delete(room);
    }
  }

  removeConnection(connectionId: string): void {
    Array.from(this.rooms.keys()).forEach((room) => this.remove(connectionId, room));
  }
}

