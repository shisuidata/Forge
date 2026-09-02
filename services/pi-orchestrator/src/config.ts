import { createHash } from "node:crypto";
import { readFileSync, statSync } from "node:fs";
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
  channelAutoBindFirstFeishu: boolean;
  channelBootstrapIdentity: {
    org_id: string;
    team_id: string;
    user_id: string;
  };
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
  benchmarkModelProvider: string;
  benchmarkModelId: string;
  benchmarkModelRevision: string;
  benchmarkConcurrency: number;
  modelControlDbPath: string | undefined;
}

function applyModelSecretReference(env: NodeJS.ProcessEnv): void {
  const reference = env.PI_MODEL_SECRET_REF;
  if (reference === undefined || reference.length === 0) return;
  const match = /^file-env:(.+)#([A-Z][A-Z0-9_]*)$/.exec(reference);
  if (match?.[1] === undefined || match[2] === undefined) {
    throw new Error("PI_MODEL_SECRET_REF must use file-env:/absolute/path#VARIABLE");
  }
  const path = resolve(match[1]);
  const mode = statSync(path).mode & 0o777;
  if (mode !== 0o600) {
    throw new Error("PI model Secret file must have mode 600");
  }
  const keyName = match[2];
  const line = readFileSync(path, "utf8").split(/\r?\n/).find(
    (candidate) => candidate.startsWith(`${keyName}=`),
  );
  if (line === undefined) throw new Error("PI model Secret variable is missing");
  let value = line.slice(keyName.length + 1).trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
    value = value.slice(1, -1);
  }
  if (value.length === 0) throw new Error("PI model Secret variable is empty");
  env.ARK_API_KEY = value;
}

function parsePort(raw: string | undefined): number {
  if (raw === undefined) return 4310;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error(`Invalid PI_ORCHESTRATOR_PORT: ${raw}`);
  }
  return port;
}

function parseBoolean(raw: string | undefined, fallback = false): boolean {
  if (raw === undefined) return fallback;
  if (raw === "true") return true;
  if (raw === "false") return false;
  throw new Error(`Invalid boolean value: ${raw}`);
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
  applyModelSecretReference(env);
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
  const benchmarkModelProvider = env.PI_BENCHMARK_MODEL_PROVIDER ?? "volcengine-coding-plan";
  const benchmarkModelId = env.PI_BENCHMARK_MODEL_ID ?? "deepseek-v4-flash";
  const benchmarkModelRevision = computePiModelRevision({
    agentDir,
    provider: benchmarkModelProvider,
    modelId: benchmarkModelId,
  }) ?? "unresolved:" + benchmarkModelProvider + "/" + benchmarkModelId;
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
    channelAutoBindFirstFeishu: parseBoolean(env.PI_CHANNEL_AUTO_BIND_FIRST_FEISHU),
    channelBootstrapIdentity: {
      org_id: env.PI_CHANNEL_BOOTSTRAP_ORG_ID ?? "org_default",
      team_id: env.PI_CHANNEL_BOOTSTRAP_TEAM_ID ?? "team_default",
      user_id: env.PI_CHANNEL_BOOTSTRAP_USER_ID ?? "feishu_owner",
    },
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
    benchmarkModelProvider,
    benchmarkModelId,
    benchmarkModelRevision,
    benchmarkConcurrency: parsePositiveInteger(
      env.PI_BENCHMARK_CONCURRENCY,
      2,
      "PI_BENCHMARK_CONCURRENCY",
    ),
    modelControlDbPath: env.PI_MODEL_CONTROL_DB_PATH
      ? resolve(env.PI_MODEL_CONTROL_DB_PATH)
      : env.MODEL_CONTROL_DB_PATH ? resolve(env.MODEL_CONTROL_DB_PATH) : undefined,
  };
}
