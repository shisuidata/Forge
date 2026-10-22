import type { ChannelEventInput } from "./contracts.js";
import { TaskStateError } from "../task-store.js";

export type ChannelEventStatus = "processing" | "completed" | "failed";

/**
 * Idempotency namespace for one trusted principal. Event IDs are caller-supplied,
 * so the same ID from another tenant or user is a different event.
 */
export function channelEventScope(principal: {
  organization_id: string; workspace_id: string; principal_id: string;
}): string {
  return JSON.stringify([principal.organization_id, principal.workspace_id, principal.principal_id]);
}

/** Pre-scope records whose principal cannot be recovered; never matches a live scope. */
export const LEGACY_CHANNEL_EVENT_SCOPE = "legacy";

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort()
    .map((key) => [key, canonical((value as Record<string, unknown>)[key])]));
}

/** A replay must carry the same input; a reused ID with different content is rejected. */
export function assertSameChannelEvent(record: ChannelEventRecord, input: ChannelEventInput): void {
  if (JSON.stringify(canonical(record.input)) !== JSON.stringify(canonical(input))) {
    throw new TaskStateError("ChannelEvent id was reused with different input");
  }
}

export interface ChannelEventRecord {
  channel: ChannelEventInput["channel"];
  scope: string;
  event_id: string;
  event_type: ChannelEventInput["event_type"];
  status: ChannelEventStatus;
  task_run_id: string | null;
  received_at: string;
  updated_at: string;
  error: string | null;
  input: ChannelEventInput;
}

export interface ChannelEventStore {
  claim(input: ChannelEventInput, scope: string): { record: ChannelEventRecord; created: boolean };
  complete(channel: ChannelEventInput["channel"], scope: string, eventId: string, taskRunId: string): ChannelEventRecord;
  fail(channel: ChannelEventInput["channel"], scope: string, eventId: string, error: string): ChannelEventRecord;
  get(channel: ChannelEventInput["channel"], scope: string, eventId: string): ChannelEventRecord | undefined;
}

export class InMemoryChannelEventStore implements ChannelEventStore {
  readonly #records = new Map<string, ChannelEventRecord>();
  constructor(private readonly assertTask: (taskRunId: string) => void) {}

  checkpoint(): () => void {
    const snapshot = structuredClone(this.#records);
    return () => {
      this.#records.clear();
      for (const [key, value] of snapshot) this.#records.set(key, value);
    };
  }

  get(channel: ChannelEventInput["channel"], scope: string, eventId: string): ChannelEventRecord | undefined {
    const record = this.#records.get(JSON.stringify([channel, scope, eventId]));
    return record === undefined ? undefined : structuredClone(record);
  }

  claim(input: ChannelEventInput, scope: string): { record: ChannelEventRecord; created: boolean } {
    const existing = this.get(input.channel, scope, input.event_id);
    if (existing !== undefined) return { record: existing, created: false };
    const taskRunId = input.event_type === "action" ? input.task_run_id : null;
    if (taskRunId !== null) this.assertTask(taskRunId);
    const now = new Date().toISOString();
    const record: ChannelEventRecord = { channel: input.channel, scope, event_id: input.event_id,
      event_type: input.event_type, status: "processing", task_run_id: taskRunId,
      received_at: now, updated_at: now, error: null, input: structuredClone(input) };
    this.#records.set(JSON.stringify([input.channel, scope, input.event_id]), record);
    return { record: structuredClone(record), created: true };
  }

  complete(channel: ChannelEventInput["channel"], scope: string, eventId: string, taskRunId: string): ChannelEventRecord {
    return this.#finish(channel, scope, eventId, "completed", taskRunId, null);
  }

  fail(channel: ChannelEventInput["channel"], scope: string, eventId: string, error: string): ChannelEventRecord {
    return this.#finish(channel, scope, eventId, "failed", null, error.slice(0, 2_000));
  }

  #finish(channel: ChannelEventInput["channel"], scope: string, eventId: string,
    status: "completed" | "failed", taskRunId: string | null, error: string | null): ChannelEventRecord {
    const current = this.get(channel, scope, eventId);
    if (current === undefined) throw new TaskStateError(`ChannelEvent not found: ${channel}/${eventId}`);
    if (current.status === status) return current;
    if (current.status !== "processing") throw new TaskStateError(`ChannelEvent is already terminal: ${current.status}`);
    if (taskRunId !== null) this.assertTask(taskRunId);
    const updated = { ...current, status, task_run_id: taskRunId, error, updated_at: new Date().toISOString() };
    this.#records.set(JSON.stringify([channel, scope, eventId]), updated);
    return structuredClone(updated);
  }
}
