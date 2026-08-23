import assert from "node:assert/strict";
import { mkdtemp } from "node:fs/promises";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import {
  attemptModelStage,
  resolveStageModelBinding,
  skillModelStage,
} from "../src/model-bindings.js";

function bindingDb(path: string) {
  const db = new DatabaseSync(path);
  db.exec(`
    CREATE TABLE model_profile_revisions (
      revision_id TEXT PRIMARY KEY, profile_id TEXT, config_json TEXT,
      validation_report_json TEXT
    );
    CREATE TABLE active_model_bindings (
      scope TEXT PRIMARY KEY, revision_id TEXT, previous_revision_id TEXT,
      binding_version INTEGER, updated_at TEXT
    );
    CREATE TABLE model_control_settings (
      setting_key TEXT PRIMARY KEY, value_json TEXT
    );
  `);
  const add = (stage: string, scope: string, gate: string, model: string) => {
    const revision = `sha256:${stage.padEnd(64, "a").slice(0, 64)}`;
    db.prepare("INSERT INTO model_profile_revisions VALUES (?, ?, ?, ?)").run(
      revision, `${stage}-profile`,
      JSON.stringify({ provider: "openai", model, capabilities: { pi_provider_id: "pi-openai" } }),
      JSON.stringify({ [gate]: { passed: true } }),
    );
    db.prepare("INSERT INTO active_model_bindings VALUES (?, ?, NULL, 1, ?)").run(
      scope, revision, new Date().toISOString(),
    );
  };
  add("analysis", "pi.analysis", "capability_gate", "analysis-model");
  add("query", "forge.query_planning", "quality_gate", "query-model");
  db.close();
}

test("Pi resolves a hot stage binding and pins its validated revision", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-stage-model-"));
  const path = join(directory, "models.db");
  bindingDb(path);
  const config = loadConfig({
    PI_MODEL_CONTROL_DB_PATH: path,
    PI_MODEL_PROVIDER: "default-provider",
    PI_MODEL_ID: "default-model",
  });

  const analysis = resolveStageModelBinding(config, "analysis");
  assert.equal(analysis?.provider, "pi-openai");
  assert.equal(analysis?.modelId, "analysis-model");
  assert.equal(analysis?.gateClass, "capability");
  assert.match(analysis?.revisionId ?? "", /^sha256:/);
  assert.equal(resolveStageModelBinding(config, "report"), undefined);
  assert.equal(skillModelStage("data-analysis-report-writer"), "report");
  assert.equal(attemptModelStage("business_root_cause_analysis"), "analysis");
  assert.equal(attemptModelStage("query_prepare"), "query_generation");
});

test("Pi honors the durable compatibility-only SQL gate switch", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-stage-model-compat-"));
  const path = join(directory, "models.db");
  bindingDb(path);
  const db = new DatabaseSync(path);
  db.prepare("UPDATE model_profile_revisions SET validation_report_json=? WHERE profile_id='query-profile'").run(
    JSON.stringify({ capability_gate: { passed: true }, quality_gate: { passed: false } }),
  );
  db.prepare("INSERT INTO model_control_settings VALUES ('sql_quality_gate_enabled', 'false')").run();
  db.close();

  const binding = resolveStageModelBinding(loadConfig({ PI_MODEL_CONTROL_DB_PATH: path }), "query_generation");
  assert.equal(binding?.modelId, "query-model");
  assert.equal(binding?.gateClass, "sql_critical");
});

test("Pi rejects an active binding whose required gate is no longer valid", async () => {
  const directory = await mkdtemp(join(tmpdir(), "forge-stage-model-invalid-"));
  const path = join(directory, "models.db");
  bindingDb(path);
  const db = new DatabaseSync(path);
  db.prepare("UPDATE model_profile_revisions SET validation_report_json='{}' WHERE profile_id='analysis-profile'").run();
  db.close();
  const config = loadConfig({ PI_MODEL_CONTROL_DB_PATH: path });
  assert.throws(() => resolveStageModelBinding(config, "analysis"), /no longer satisfies/);
});
