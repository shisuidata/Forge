import type { ChannelEventInput } from "./contracts.js";
import { TaskStateError } from "../task-store.js";

export type ChannelEventStatus = "processing" | "completed" | "failed";

export interface ChannelEventRecord {
  channel: ChannelEventInput["channel"];
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
  claim(input: ChannelEventInput): { record: ChannelEventRecord; created: boolean };
  complete(channel: ChannelEventInput["channel"], eventId: string, taskRunId: string): ChannelEventRecord;
  fail(channel: ChannelEventInput["channel"], eventId: string, error: string): ChannelEventRecord;
  get(channel: ChannelEventInput["channel"], eventId: string): ChannelEventRecord | undefined;
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

  get(channel: ChannelEventInput["channel"], eventId: string): ChannelEventRecord | undefined {
    const record = this.#records.get(`${channel}\0${eventId}`);
    return record === undefined ? undefined : structuredClone(record);
  }

  claim(input: ChannelEventInput): { record: ChannelEventRecord; created: boolean } {
    const existing = this.get(input.channel, input.event_id);
    if (existing !== undefined) return { record: existing, created: false };
    const taskRunId = input.event_type === "action" ? input.task_run_id : null;
    if (taskRunId !== null) this.assertTask(taskRunId);
    const now = new Date().toISOString();
    const record: ChannelEventRecord = { channel: input.channel, event_id: input.event_id,
      event_type: input.event_type, status: "processing", task_run_id: taskRunId,
      received_at: now, updated_at: now, error: null, input: structuredClone(input) };
    this.#records.set(`${input.channel}\0${input.event_id}`, record);
    return { record: structuredClone(record), created: true };
  }

  complete(channel: ChannelEventInput["channel"], eventId: string, taskRunId: string): ChannelEventRecord {
    return this.#finish(channel, eventId, "completed", taskRunId, null);
  }

  fail(channel: ChannelEventInput["channel"], eventId: string, error: string): ChannelEventRecord {
    return this.#finish(channel, eventId, "failed", null, error.slice(0, 2_000));
  }

  #finish(channel: ChannelEventInput["channel"], eventId: string,
    status: "completed" | "failed", taskRunId: string | null, error: string | null): ChannelEventRecord {
    const current = this.get(channel, eventId);
    if (current === undefined) throw new TaskStateError(`ChannelEvent not found: ${channel}/${eventId}`);
    if (current.status === status) return current;
    if (current.status !== "processing") throw new TaskStateError(`ChannelEvent is already terminal: ${current.status}`);
    if (taskRunId !== null) this.assertTask(taskRunId);
    const updated = { ...current, status, task_run_id: taskRunId, error, updated_at: new Date().toISOString() };
    this.#records.set(`${channel}\0${eventId}`, updated);
    return structuredClone(updated);
  }
}
