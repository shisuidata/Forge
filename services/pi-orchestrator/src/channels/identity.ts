import { existsSync, readFileSync } from "node:fs";

import type { TaskChannel } from "../task-store.js";
import type { ChannelIdentity } from "./contracts.js";

export class ChannelIdentityError extends Error {}

export class ChannelIdentityResolver {
  readonly #identities = new Map<string, ChannelIdentity>();

  constructor(identityMapPath: string) {
    if (!existsSync(identityMapPath)) return;
    let value: unknown;
    try {
      value = JSON.parse(readFileSync(identityMapPath, "utf8")) as unknown;
    } catch (error) {
      throw new ChannelIdentityError(`Invalid channel identity map JSON: ${identityMapPath}`, {
        cause: error,
      });
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new ChannelIdentityError("Channel identity map must be an object");
    }
    for (const [channel, channelEntries] of Object.entries(value)) {
      if (channel !== "feishu" && channel !== "dingtalk") {
        throw new ChannelIdentityError(`Unsupported identity-map channel: ${channel}`);
      }
      if (
        typeof channelEntries !== "object" ||
        channelEntries === null ||
        Array.isArray(channelEntries)
      ) {
        throw new ChannelIdentityError(`Identity map for ${channel} must be an object`);
      }
      for (const [externalUserId, rawIdentity] of Object.entries(channelEntries)) {
        if (
          externalUserId.trim().length === 0 ||
          typeof rawIdentity !== "object" ||
          rawIdentity === null ||
          Array.isArray(rawIdentity)
        ) {
          throw new ChannelIdentityError(`Invalid identity entry for ${channel}`);
        }
        const identity = rawIdentity as Record<string, unknown>;
        const mapped: ChannelIdentity = {
          org_id: this.#required(identity.org_id, "org_id"),
          team_id: this.#required(identity.team_id, "team_id"),
          user_id: this.#required(identity.user_id, "user_id"),
        };
        this.#identities.set(`${channel}:${externalUserId}`, mapped);
      }
    }
  }

  resolve(
    channel: Exclude<TaskChannel, "web" | "api">,
    externalUserId: string,
  ): ChannelIdentity {
    const identity = this.#identities.get(`${channel}:${externalUserId}`);
    if (identity === undefined) {
      throw new ChannelIdentityError("Channel identity is not authorized");
    }
    return structuredClone(identity);
  }

  get size(): number {
    return this.#identities.size;
  }

  #required(value: unknown, field: string): string {
    if (typeof value !== "string" || value.trim().length === 0) {
      throw new ChannelIdentityError(`${field} must be a non-empty string`);
    }
    return value;
  }
}
