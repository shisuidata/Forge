import { join } from "node:path";

import {
  createAgentSession,
  ModelRuntime,
  SessionManager,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";

import type { OrchestratorConfig } from "./config.js";
import { ForgeQueryRunClient } from "./forge/query-run-client.js";
import { loadMvpSkillResources, MVP_SKILL_NAMES } from "./skills.js";
import {
  createForgePrepareQueryTool,
  type TrustedTaskContext,
} from "./tools/forge-prepare-query.js";

export interface RuntimeCapabilities {
  orchestrator: "pi";
  builtinToolsEnabled: false;
  forgeTools: readonly string[];
  artifactTools: readonly string[];
  skills: readonly string[];
  modelExecutionConfigured: boolean;
  modelExecutionStatus: "unconfigured" | "ready" | "unavailable";
}

export const RUNTIME_CAPABILITIES: RuntimeCapabilities = {
  orchestrator: "pi",
  builtinToolsEnabled: false,
  forgeTools: ["forge_prepare_query"],
  artifactTools: [
    "submit_clarification_artifact",
    "submit_metric_definition_artifact",
    "submit_analysis_artifact",
    "submit_advisory_artifact",
    "submit_rendered_output_artifact",
  ],
  skills: MVP_SKILL_NAMES,
  modelExecutionConfigured: false,
  modelExecutionStatus: "unconfigured",
};

export async function inspectRuntime(config: OrchestratorConfig): Promise<RuntimeCapabilities> {
  const resources = await loadMvpSkillResources({
    cwd: config.skillsRoot,
    agentDir: config.agentDir,
    skillsRoot: config.skillsRoot,
  });
  let modelExecutionStatus: RuntimeCapabilities["modelExecutionStatus"] = "unconfigured";
  if (config.piModelProvider !== undefined && config.piModelId !== undefined) {
    try {
      const runtime = await ModelRuntime.create({
        authPath: join(config.agentDir, "auth.json"),
        modelsPath: join(config.agentDir, "models.json"),
        refreshOnCreate: false,
        allowModelNetwork: false,
      });
      const available = await runtime.getAvailable(config.piModelProvider);
      modelExecutionStatus = available.some((model) => model.id === config.piModelId)
        ? "ready"
        : "unavailable";
    } catch {
      modelExecutionStatus = "unavailable";
    }
  }
  return {
    ...RUNTIME_CAPABILITIES,
    skills: resources.skills.map((skill) => skill.name),
    modelExecutionConfigured: modelExecutionStatus === "ready",
    modelExecutionStatus,
  };
}

export async function createRestrictedTaskSession(options: {
  config: OrchestratorConfig;
  modelRuntime: ModelRuntime;
  task: TrustedTaskContext;
}) {
  const resources = await loadMvpSkillResources({
    cwd: options.config.skillsRoot,
    agentDir: options.config.agentDir,
    skillsRoot: options.config.skillsRoot,
  });
  const settingsManager = SettingsManager.inMemory({
    enableSkillCommands: false,
    compaction: { enabled: true },
  });
  const forgeClient = new ForgeQueryRunClient({
    baseUrl: options.config.forgeBaseUrl,
    timeoutMs: options.config.forgeTimeoutMs,
    ...(options.config.forgePiServiceKey === undefined
      ? {}
      : { serviceKey: options.config.forgePiServiceKey }),
  });
  const forgePrepareQuery = createForgePrepareQueryTool({
    client: forgeClient,
    task: options.task,
  });

  return createAgentSession({
    cwd: options.config.skillsRoot,
    agentDir: options.config.agentDir,
    modelRuntime: options.modelRuntime,
    resourceLoader: resources.loader,
    settingsManager,
    sessionManager: SessionManager.inMemory(options.config.skillsRoot),
    noTools: "builtin",
    tools: ["forge_prepare_query"],
    customTools: [forgePrepareQuery],
  });
}
