import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import { inspectRuntime } from "../src/runtime.js";
import {
  loadMvpSkillResources,
  loadStageSkillResources,
  MVP_SKILL_NAMES,
} from "../src/skills.js";


test("Stage lease must outlive the Stage timeout", () => {
  assert.throws(
    () =>
      loadConfig({
        PI_STAGE_TIMEOUT_MS: "1000",
        PI_STAGE_LEASE_MS: "1000",
      }),
    /must be greater/,
  );
});

test("Forge HTTP timeout must finish before the Stage timeout", () => {
  assert.throws(
    () =>
      loadConfig({
        FORGE_REQUEST_TIMEOUT_MS: "240000",
        PI_STAGE_TIMEOUT_MS: "240000",
      }),
    /must be less/,
  );
  const config = loadConfig({});
  assert.equal(config.forgeTimeoutMs, 220_000);
  assert.ok(config.forgeTimeoutMs < config.stageTimeoutMs);
});

test("state database defaults under the dedicated agent directory", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "forge-pi-agent-"));
  const config = loadConfig({ PI_ORCHESTRATOR_AGENT_DIR: agentDir });
  assert.equal(config.stateDbPath, join(agentDir, "state/orchestrator.sqlite3"));
});

test("runtime loads the 23 explicitly authorized production Skills", async () => {
  const config = loadConfig({});
  const resources = await loadMvpSkillResources({
    cwd: config.skillsRoot,
    agentDir: await mkdtemp(join(tmpdir(), "forge-pi-agent-")),
    skillsRoot: config.skillsRoot,
  });

  assert.deepEqual(
    resources.skills.map((skill) => skill.name).sort(),
    [...MVP_SKILL_NAMES].sort(),
  );
  assert.equal(resources.loader.getExtensions().extensions.length, 0);
  assert.equal(resources.loader.getAgentsFiles().agentsFiles.length, 0);
});


test("stage runtime injects exactly one authorized Skill in full", async () => {
  const config = loadConfig({});
  const resources = await loadStageSkillResources({
    cwd: config.skillsRoot,
    agentDir: await mkdtemp(join(tmpdir(), "forge-pi-agent-")),
    skillsRoot: config.skillsRoot,
    skillName: "data-requirement-clarifier",
  });
  assert.deepEqual(resources.skills.map((skill) => skill.name), [
    "data-requirement-clarifier",
  ]);
  assert.match(resources.loader.getSystemPrompt() ?? "", /<AUTHORIZED_SKILL>/);
  assert.match(resources.loader.getSystemPrompt() ?? "", /把模糊的数据需求整理成/);
  assert.equal(resources.loader.getExtensions().extensions.length, 0);
});


test("runtime capabilities state that built-in tools are disabled", async () => {
  const config = loadConfig({});
  const capabilities = await inspectRuntime({
    ...config,
    agentDir: await mkdtemp(join(tmpdir(), "forge-pi-agent-")),
  });

  assert.equal(capabilities.orchestrator, "pi");
  assert.equal(capabilities.builtinToolsEnabled, false);
  assert.deepEqual(capabilities.forgeTools, ["forge_prepare_query"]);
  assert.deepEqual(capabilities.artifactTools, [
    "submit_clarification_artifact",
    "submit_metric_definition_artifact",
    "submit_analysis_artifact",
    "submit_advisory_artifact",
    "submit_rendered_output_artifact",
  ]);
  assert.equal(capabilities.modelExecutionConfigured, false);
  assert.deepEqual([...capabilities.skills].sort(), [...MVP_SKILL_NAMES].sort());
});


test("Pi model catalog produces an immutable non-secret Stage revision", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "forge-pi-model-revision-"));
  await writeFile(join(agentDir, "models.json"), JSON.stringify({ providers: { demo: { models: [{ id: "m1" }] } } }));
  const first = loadConfig({
    PI_ORCHESTRATOR_AGENT_DIR: agentDir,
    PI_MODEL_PROVIDER: "demo",
    PI_MODEL_ID: "m1",
  });
  const second = loadConfig({
    PI_ORCHESTRATOR_AGENT_DIR: agentDir,
    PI_MODEL_PROVIDER: "demo",
    PI_MODEL_ID: "m1",
  });
  assert.match(first.piModelRevision ?? "", /^sha256:[a-f0-9]{64}$/);
  assert.equal(first.piModelRevision, second.piModelRevision);
  await writeFile(join(agentDir, "models.json"), JSON.stringify({ providers: { demo: { models: [{ id: "m1", maxTokens: 2 }] } } }));
  const changed = loadConfig({
    PI_ORCHESTRATOR_AGENT_DIR: agentDir,
    PI_MODEL_PROVIDER: "demo",
    PI_MODEL_ID: "m1",
  });
  assert.notEqual(changed.piModelRevision, first.piModelRevision);
});

test("runtime reports unavailable until the dedicated model catalog is ready", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "forge-pi-agent-"));
  const config = loadConfig({
    PI_MODEL_PROVIDER: "test-provider",
    PI_MODEL_ID: "test-model",
  });
  const unavailable = await inspectRuntime({ ...config, agentDir });
  assert.equal(unavailable.modelExecutionConfigured, false);
  assert.equal(unavailable.modelExecutionStatus, "unavailable");

  await writeFile(
    join(agentDir, "models.json"),
    JSON.stringify({
      providers: {
        "test-provider": {
          baseUrl: "http://127.0.0.1:9/v1",
          api: "openai-completions",
          apiKey: "test-only-key",
          models: [{ id: "test-model" }],
        },
      },
    }),
  );
  const ready = await inspectRuntime({ ...config, agentDir });
  assert.equal(ready.modelExecutionConfigured, true);
  assert.equal(ready.modelExecutionStatus, "ready");
});


test("runtime fails closed when model configuration is partial", () => {
  assert.throws(
    () => loadConfig({ PI_MODEL_PROVIDER: "openai" }),
    /must be configured together/,
  );
});


test("runtime fails closed when the skills package is unavailable", async () => {
  const config = loadConfig({ SHISUI_DATA_SKILLS_DIR: "/missing/skills-package" });
  await assert.rejects(() => inspectRuntime(config));
});
