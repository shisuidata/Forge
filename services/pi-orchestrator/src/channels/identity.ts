import {
  chmodSync,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname } from "node:path";

import type { TaskChannel } from "../task-store.js";
import type { ChannelIdentity } from "./contracts.js";

export class ChannelIdentityError extends Error {}

type ExternalChannel = Exclude<TaskChannel, "web" | "api">;
type IdentityDocument = Record<ExternalChannel, Record<string, ChannelIdentity>>;

export class ChannelIdentityResolver {
  readonly #identities = new Map<string, ChannelIdentity>();
  readonly #identityMapPath: string;
  readonly #document: IdentityDocument = { feishu: {}, dingtalk: {} };
  #feishuIdentityCount = 0;

  constructor(identityMapPath: string) {
    this.#identityMapPath = identityMapPath;
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
        this.#validateExternalUserId(externalUserId);
        if (
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
        this.#document[channel][externalUserId] = mapped;
        this.#identities.set(`${channel}:${externalUserId}`, mapped);
        if (channel === "feishu") this.#feishuIdentityCount += 1;
      }
    }
  }

  resolve(channel: ExternalChannel, externalUserId: string): ChannelIdentity {
    const identity = this.#identities.get(`${channel}:${externalUserId}`);
    if (identity === undefined) {
      throw new ChannelIdentityError("Channel identity is not authorized");
    }
    return structuredClone(identity);
  }

  bindFirstFeishu(externalUserId: string, identity: ChannelIdentity): ChannelIdentity {
    this.#validateExternalUserId(externalUserId);
    const existing = this.#identities.get(`feishu:${externalUserId}`);
    if (existing !== undefined) return structuredClone(existing);
    if (this.#feishuIdentityCount !== 0) {
      throw new ChannelIdentityError("Channel identity is not authorized");
    }
    const mapped: ChannelIdentity = {
      org_id: this.#required(identity.org_id, "org_id"),
      team_id: this.#required(identity.team_id, "team_id"),
      user_id: this.#required(identity.user_id, "user_id"),
    };
    this.#document.feishu[externalUserId] = mapped;
    this.#persist();
    this.#identities.set(`feishu:${externalUserId}`, mapped);
    this.#feishuIdentityCount = 1;
    return structuredClone(mapped);
  }

  get size(): number {
    return this.#identities.size;
  }

  get feishuIdentityCount(): number {
    return this.#feishuIdentityCount;
  }

  #persist(): void {
    const directory = dirname(this.#identityMapPath);
    mkdirSync(directory, { recursive: true, mode: 0o700 });
    const temporaryPath = `${this.#identityMapPath}.tmp-${process.pid}-${Date.now()}`;
    try {
      writeFileSync(temporaryPath, `${JSON.stringify(this.#document, null, 2)}\n`, {
        encoding: "utf8",
        mode: 0o600,
        flag: "wx",
      });
      chmodSync(temporaryPath, 0o600);
      renameSync(temporaryPath, this.#identityMapPath);
    } catch (error) {
      try {
        if (existsSync(temporaryPath)) unlinkSync(temporaryPath);
      } catch {
        // Preserve the original persistence failure.
      }
      throw new ChannelIdentityError("Failed to persist channel identity binding", { cause: error });
    }
  }

  #validateExternalUserId(value: string): void {
    if (
      value.trim().length === 0 ||
      value.length > 256 ||
      /[\u0000-\u001f\u007f]/.test(value) ||
      value === "__proto__" ||
      value === "constructor" ||
      value === "prototype"
    ) {
      throw new ChannelIdentityError("Invalid external channel user ID");
    }
  }

  #required(value: unknown, field: string): string {
    if (typeof value !== "string" || value.trim().length === 0) {
      throw new ChannelIdentityError(`${field} must be a non-empty string`);
    }
    return value;
  }
}
