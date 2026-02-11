export interface ParsedNodeEvent {
  payload?: string;
}

function Concurrent(_: unknown, __?: string, ___?: PropertyDescriptor): void {
  // Placeholder decorator to satisfy taskpool concurrent requirements.
}

@Concurrent
export function parseNodeEvents(raw: string): ParsedNodeEvent[] {
  if (!raw || raw.length === 0) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) {
      return [];
    }
    const result: ParsedNodeEvent[] = [];
    parsed.forEach((entry: unknown) => {
      if (entry && typeof entry === 'object') {
        const candidate = entry as { payload?: string };
        if (typeof candidate.payload === 'string') {
          result.push({ payload: candidate.payload });
          return;
        }
      }
      result.push({});
    });
    return result;
  } catch (_) {
    return [];
  }
}
