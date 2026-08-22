import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const moduleDir = dirname(fileURLToPath(import.meta.url));
const defaultSkillsRoot = resolve(moduleDir, "../../../../拾穗 DATA");
const defaultAgentDir = resolve(moduleDir, "../.runtime");

export interface OrchestratorConfig {
  host: string;
  port: number;
  skillsRoot: string;
  agentDir: string;
  stateDbPath: string;
  channelIdentityMapPath: string;
  channelServiceKeys: string[];
  adminServiceKeys: string[];
  forgeBaseUrl: string;
  forgeApiKey: string | undefined;
  forgePiServiceKey: string | undefined;
  forgeTimeoutMs: number;
  stageTimeoutMs: number;
  stageLeaseMs: number;
  reconciliationIntervalMs: number;
  piModelProvider: string | undefined;
  piModelId: string | undefined;
  piModelRevision: string | null;
}

function parsePort(raw: string | undefined): number {
  if (raw === undefined) return 4310;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`Invalid PI_ORCHESTRATOR_PORT: ${raw}`);
  }
  return port;
}

function parsePositiveInteger(raw: string | undefined, fallback: number, name: string): number {
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`Invalid ${name}: ${raw}`);
  }
  return value;
}

export function computePiModelRevision(options: {
  agentDir: string;
  provider: string | undefined;
  modelId: string | undefined;
}): string | null {
  if (options.provider === undefined || options.modelId === undefined) return null;
  const modelsPath = resolve(options.agentDir, "models.json");
  try {
    const catalog = readFileSync(modelsPath);
    return `sha256:${createHash("sha256")
      .update(options.provider).update("\0").update(options.modelId).update("\0").update(catalog)
      .digest("hex")}`;
  } catch {
    return `unresolved:${options.provider}/${options.modelId}`;
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): OrchestratorConfig {
  const piModelProvider = env.PI_MODEL_PROVIDER || undefined;
  const piModelId = env.PI_MODEL_ID || undefined;
  if ((piModelProvider === undefined) !== (piModelId === undefined)) {
    throw new Error("PI_MODEL_PROVIDER and PI_MODEL_ID must be configured together");
  }
  const agentDir = resolve(env.PI_ORCHESTRATOR_AGENT_DIR ?? defaultAgentDir);
  const stageTimeoutMs = parsePositiveInteger(
    env.PI_STAGE_TIMEOUT_MS,
    240_000,
    "PI_STAGE_TIMEOUT_MS",
  );
  const stageLeaseMs = parsePositiveInteger(
    env.PI_STAGE_LEASE_MS,
    300_000,
    "PI_STAGE_LEASE_MS",
  );
  if (stageLeaseMs <= stageTimeoutMs) {
    throw new Error("PI_STAGE_LEASE_MS must be greater than PI_STAGE_TIMEOUT_MS");
  }
  const forgeTimeoutMs = parsePositiveInteger(
    env.FORGE_REQUEST_TIMEOUT_MS,
    220_000,
    "FORGE_REQUEST_TIMEOUT_MS",
  );
  if (forgeTimeoutMs >= stageTimeoutMs) {
    throw new Error("FORGE_REQUEST_TIMEOUT_MS must be less than PI_STAGE_TIMEOUT_MS");
  }
  const piModelRevision = computePiModelRevision({
    agentDir,
    provider: piModelProvider,
    modelId: piModelId,
  });
  return {
    host: env.PI_ORCHESTRATOR_HOST ?? "127.0.0.1",
    port: parsePort(env.PI_ORCHESTRATOR_PORT),
    skillsRoot: resolve(env.SHISUI_DATA_SKILLS_DIR ?? defaultSkillsRoot),
    agentDir,
    stateDbPath: resolve(
      env.PI_ORCHESTRATOR_STATE_DB ?? resolve(agentDir, "state/orchestrator.sqlite3"),
    ),
    channelIdentityMapPath: resolve(
      env.PI_CHANNEL_IDENTITY_MAP ?? resolve(agentDir, "channel-identities.json"),
    ),
    channelServiceKeys: (env.PI_CHANNEL_SERVICE_KEYS ?? "")
      .split(",")
      .map((key) => key.trim())
      .filter((key) => key.length > 0),
    adminServiceKeys: (env.PI_ADMIN_SERVICE_KEYS ?? "")
      .split(",")
      .map((key) => key.trim())
      .filter((key) => key.length > 0),
    forgeBaseUrl: (env.FORGE_BASE_URL ?? "http://127.0.0.1:8000").replace(/\/$/, ""),
    forgeApiKey: env.FORGE_API_KEY || undefined,
    forgePiServiceKey: env.FORGE_PI_SERVICE_KEY || undefined,
    forgeTimeoutMs,
    stageTimeoutMs,
    stageLeaseMs,
    reconciliationIntervalMs: parsePositiveInteger(
      env.PI_RECONCILIATION_INTERVAL_MS,
      30_000,
      "PI_RECONCILIATION_INTERVAL_MS",
    ),
    piModelProvider,
    piModelId,
    piModelRevision,
  };
}
