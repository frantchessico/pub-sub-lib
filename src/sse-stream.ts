export type SseMessage = {
  id?: string;
  event?: string;
  data: string;
  retry?: number;
};

export function parseSseBlock(block: string): SseMessage | null {
  const lines = block.split(/\r?\n/);
  let id: string | undefined;
  let event: string | undefined;
  let retry: number | undefined;
  const dataLines: string[] = [];

  for (const line of lines) {
    if (!line || line.startsWith(':')) {
      continue;
    }

    const separator = line.indexOf(':');
    const field = separator === -1 ? line : line.slice(0, separator);
    const value = separator === -1 ? '' : line.slice(separator + 1).replace(/^ /, '');

    if (field === 'id') {
      id = value;
    } else if (field === 'event') {
      event = value;
    } else if (field === 'data') {
      dataLines.push(value);
    } else if (field === 'retry') {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) {
        retry = parsed;
      }
    }
  }

  if (dataLines.length === 0 && !event && !id) {
    return null;
  }

  return {
    id,
    event,
    data: dataLines.join('\n'),
    retry,
  };
}

export async function* readSseStream(body: ReadableStream<Uint8Array>): AsyncGenerator<SseMessage> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      let boundary = buffer.indexOf('\n\n');
      while (boundary !== -1) {
        const block = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        const message = parseSseBlock(block);
        if (message) {
          yield message;
        }
        boundary = buffer.indexOf('\n\n');
      }
    }

    if (buffer.trim()) {
      const message = parseSseBlock(buffer);
      if (message) {
        yield message;
      }
    }
  } finally {
    reader.releaseLock();
  }
}

export function formatSseCursor(room: string, sequence: number) {
  return `${encodeURIComponent(room)}@${sequence}`;
}

export function parseSseCursor(value: string): { room?: string; sequence: number } {
  const at = value.lastIndexOf('@');
  if (at === -1) {
    const sequence = Number(value);
    return { sequence: Number.isFinite(sequence) && sequence > 0 ? sequence : 0 };
  }

  const room = decodeURIComponent(value.slice(0, at));
  const sequence = Number(value.slice(at + 1));
  return {
    room: room || undefined,
    sequence: Number.isFinite(sequence) && sequence > 0 ? sequence : 0,
  };
}

export function serializeSinceMap(sinceByRoom: Map<string, number>) {
  return Array.from(sinceByRoom.entries())
    .filter(([, sequence]) => sequence > 0)
    .map(([room, sequence]) => formatSseCursor(room, sequence))
    .join(',');
}

export function deserializeSinceMap(raw: string) {
  const map = new Map<string, number>();
  for (const token of raw.split(',').map((part) => part.trim()).filter(Boolean)) {
    const parsed = parseSseCursor(token);
    if (parsed.room) {
      map.set(parsed.room, Math.max(map.get(parsed.room) ?? 0, parsed.sequence));
    }
  }
  return map;
}
