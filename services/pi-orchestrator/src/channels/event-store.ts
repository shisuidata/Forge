import type { ChannelEventInput } from "./contracts.js";

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
