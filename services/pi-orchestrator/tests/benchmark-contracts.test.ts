import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { computePiModelRevision, loadConfig } from "../src/config.js";


test("benchmark provider and model are selectable independently from production Pi binding", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "pi-benchmark-models-"));
  await writeFile(join(agentDir, "models.json"), JSON.stringify({ providers: {} }));
  const config = loadConfig({
    PI_ORCHESTRATOR_AGENT_DIR: agentDir,
    PI_MODEL_PROVIDER: "production-provider",
    PI_MODEL_ID: "production-model",
    PI_BENCHMARK_MODEL_PROVIDER: "volcengine-coding-plan",
    PI_BENCHMARK_MODEL_ID: "deepseek-v4-flash",
    PI_BENCHMARK_CONCURRENCY: "3",
  });
  assert.equal(config.piModelProvider, "production-provider");
  assert.equal(config.piModelId, "production-model");
  assert.equal(config.benchmarkModelProvider, "volcengine-coding-plan");
  assert.equal(config.benchmarkModelId, "deepseek-v4-flash");
  assert.equal(config.benchmarkConcurrency, 3);
  assert.match(config.benchmarkModelRevision, /^sha256:/);
});


test("benchmark model revision binds provider, model, and catalog", async () => {
  const agentDir = await mkdtemp(join(tmpdir(), "pi-benchmark-revision-"));
  await writeFile(join(agentDir, "models.json"), "{\"providers\":{}}");
  const first = computePiModelRevision({ agentDir, provider: "p", modelId: "a" });
  const second = computePiModelRevision({ agentDir, provider: "p", modelId: "b" });
  assert.notEqual(first, second);
});
