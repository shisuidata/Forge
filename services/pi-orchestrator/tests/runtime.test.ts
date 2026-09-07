import assert from "node:assert/strict";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";
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
import { createSkillFixture } from "./skill-fixture.js";

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

test("Pi model Secret reference reads one variable from a mode-600 file without persisting it", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-secret-"));
  const secretPath = join(directory, "forge.env");
  await writeFile(secretPath, "LLM_API_KEY=secret-for-test\nDATABASE_PASSWORD=must-not-load\n");
  await chmod(secretPath, 0o600);
  const env: NodeJS.ProcessEnv = {
    PI_MODEL_SECRET_REF: `file-env:${secretPath}#LLM_API_KEY`,
  };
  const config = loadConfig(env);
  assert.equal(env.ARK_API_KEY, "secret-for-test");
  assert.doesNotMatch(JSON.stringify(config), /secret-for-test|must-not-load/);

  await chmod(secretPath, 0o644);
  assert.throws(() => loadConfig(env), /mode 600/);
});

test("state database defaults under the dedicated agent directory", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "forge-pi-agent-"));
  const config = loadConfig({ PI_ORCHESTRATOR_AGENT_DIR: agentDir });
  assert.equal(config.stateDbPath, join(agentDir, "state/orchestrator.sqlite3"));
});

test("runtime discovers authorized Skills without admitting an unlisted package entry", async (context) => {
  const { skillsRoot, agentDir } = await createSkillFixture(context);
  const resources = await loadMvpSkillResources({ cwd: skillsRoot, agentDir, skillsRoot });

  assert.deepEqual(
    resources.skills.map((skill) => skill.name).sort(),
    [...MVP_SKILL_NAMES].sort(),
  );
  assert.equal(resources.loader.getExtensions().extensions.length, 0);
  assert.equal(resources.loader.getAgentsFiles().agentsFiles.length, 0);
});


test("stage runtime injects only the authorized Skill, excluding other available Skills", async (context) => {
  const { skillsRoot, agentDir, documents } = await createSkillFixture(context);
  const resources = await loadStageSkillResources({
    cwd: skillsRoot, agentDir, skillsRoot, skillName: "data-requirement-clarifier",
  });
  assert.deepEqual(resources.skills.map((skill) => skill.name), [
    "data-requirement-clarifier",
  ]);
  const prompt = resources.loader.getSystemPrompt() ?? "";
  assert.ok(prompt.includes(documents.get("data-requirement-clarifier")!));
  assert.doesNotMatch(prompt, /Fixture boundary marker: metric-definition-reviewer/);
  assert.doesNotMatch(prompt, /Fixture boundary marker: unlisted-skill/);
});


test("runtime capabilities state that built-in tools are disabled", async (context) => {
  const { skillsRoot, agentDir } = await createSkillFixture(context);
  const config = loadConfig({ SHISUI_DATA_SKILLS_DIR: skillsRoot, PI_ORCHESTRATOR_AGENT_DIR: agentDir });
  const capabilities = await inspectRuntime(config);

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

test("runtime reports unavailable until the dedicated model catalog is ready", async (context) => {
  const { skillsRoot, agentDir } = await createSkillFixture(context);
  const config = loadConfig({
    SHISUI_DATA_SKILLS_DIR: skillsRoot,
    PI_ORCHESTRATOR_AGENT_DIR: agentDir,
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
