import assert from "node:assert/strict";
import test from "node:test";
import { OrchestratorApplication, type ForgeQueryRunPort, type TrustedRequestPrincipal } from "../src/application.js";
import { loadConfig } from "../src/config.js";
import type { QueryRunReview } from "../src/forge/query-run-client.js";
import { InMemoryOrchestratorState } from "../src/orchestrator-state.js";

const sqlHash = `sha256:${"a".repeat(64)}`;
const assuranceHash = `sha256:${"b".repeat(64)}`;
const contextHash = `sha256:${"c".repeat(64)}`;
const principal = (id: string, role: TrustedRequestPrincipal["role"] = "member"): TrustedRequestPrincipal => ({
  principal_id: id, principal_type: "human", organization_id: "org_demo", workspace_id: "team_demo", role,
  identity_revision: 1, credential_id: `cr_${id}`, credential_revision: 1, authentication_method: "api_key",
  authenticated_at: new Date().toISOString(), channel_binding_id: null, channel_binding_revision: null,
});

function fixture(mode: "autonomous" | "human_review") {
  const calls: string[] = [];
  let runId = "tr_placeholder";
  const state = new InMemoryOrchestratorState();
  const mandate = { status: "active", delegated_mandate: {
    task_run_id: runId, organization_id: "org_demo", workspace_id: "team_demo", purpose: "bounded data analysis",
    delegate_principal: { principal_id: "pi_service" }, accountable_principal: { principal_id: "alice" },
  }, policy: { reviewer_principal_id: "bob" } };
  const review = (status: QueryRunReview["status"]): QueryRunReview => ({
    schema_version: 2, query_run_id: "qr_demo", task_run_id: runId, status,
    decision: { verdict: "allow", review_required: status === "review_required", execution_authorized: status === "executing" || status === "completed", execution_mode: mode },
    context: { principal_context: {}, delegated_mandate: {}, purpose: "bounded data analysis", resource_scope: [] },
    candidate: { input_kind: "direct_sql", candidate_revision: "query-candidate-v1", producer_revision: "test", candidate_hash: sqlHash },
    review: { sql: "SELECT 7 AS n", sql_hash: sqlHash, assurance_report_hash: assuranceHash,
      enforcement_context_hash: contextHash, expires_at: new Date(Date.now() + 60_000).toISOString() },
    assurance: { status: "passed" },
    lineage: { assurance_revision: "query-assurance-v2", policy_revision: "scoped-policy-v1",
      registry_revision: "test-registry", assurance_report_hash: assuranceHash,
      enforcement_context_hash: contextHash, authorization_hash: status === "executing" || status === "completed" ? contextHash : null },
    result: status === "completed" ? { columns: ["n"], rows: [[7]], row_count: 1, truncated: false,
      execution_ms: 1, executed_at: new Date().toISOString() } : null,
    failure: null, authorization: status === "executing" || status === "completed" ? { kind: mode } : null,
  });
  const forge: ForgeQueryRunPort = {
    async getMandate() { calls.push("mandate:get"); return structuredClone(mandate); },
    async createQueryRun(input) {
      calls.push(`prepare:${input.mandateId}:${input.purpose}:${input.candidate?.kind}`);
      return review(mode === "autonomous" ? "ready" : "review_required");
    },
    async executeQueryRun(input) { calls.push(`execute:${input.idempotencyKey}`); return review("executing"); },
    async approveQueryRun(input) { calls.push(`approve:${input.controlToken}`); return review("executing"); },
    async getQueryRun() { calls.push("observe"); return review("completed"); },
  };
  const application = new OrchestratorApplication({ config: loadConfig({ PI_EXECUTION_PRINCIPAL_ID: "pi_service" }),
    state, forgeClient: forge });
  const task = application.createTask({ org_id: "org_demo", team_id: "team_demo", user_id: "alice",
    channel: "web", intent: "query_prepare", message: "Seven" }, principal("alice", "admin")).task;
  runId = task.task_run_id;
  mandate.delegated_mandate.task_run_id = runId;
  return { application, task, calls, state, forge };
}

const candidate = { kind: "direct_sql" as const, sql: "SELECT 7 AS n" };

test("unmandated query saves the exact candidate and resumes only after admin attaches bounded mandate", async () => {
  const { application, task, calls } = fixture("human_review");
  const pending = await application.prepareQuery(task.task_run_id, { question: "Seven", dialect: "sqlite", candidate, idempotencyKey: "prepare-1" });
  assert.equal(pending.task.status, "needs_input");
  assert.equal(pending.task.current_stage, "query_authorization");
  assert.equal(pending.result, null);
  assert.deepEqual(calls, []);
  const attached = await application.attachMandate(task.task_run_id,
    { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  assert.equal(attached.status, "waiting_for_query_approval");
  assert.deepEqual(calls, ["mandate:get", "mandate:get", "prepare:md_demo:bounded data analysis:direct_sql"]);
  assert.equal(application.getEvents(task.task_run_id).filter(event => event.event_type === "task.mandate_attached").length, 1);
  await application.attachMandate(task.task_run_id, { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  assert.equal(application.getEvents(task.task_run_id).filter(event => event.event_type === "task.mandate_attached").length, 1);
});

test("caller abort during query prepare is a retryable interruption like other Stages", async () => {
  const { application, task, calls, forge } = fixture("human_review");
  await application.attachMandate(task.task_run_id, { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  const controller = new AbortController();
  const createQueryRun = forge.createQueryRun;
  forge.createQueryRun = async () => {
    calls.push("prepare:hung");
    controller.abort(new Error("Synthetic caller disconnected"));
    return new Promise<never>(() => undefined);
  };
  await assert.rejects(application.prepareQuery(task.task_run_id,
    { question: "Seven", candidate, idempotencyKey: "prepare-aborted" }, controller.signal));
  assert.equal(application.getTask(task.task_run_id)?.status, "ready_for_query");
  assert.equal(application.getTask(task.task_run_id)?.current_stage, "query_prepare_retry");
  assert.equal(application.getStageAttempts(task.task_run_id).at(-1)?.status, "interrupted");
  forge.createQueryRun = createQueryRun;
  const retried = await application.prepareQuery(task.task_run_id,
    { question: "Seven", candidate, idempotencyKey: "prepare-retry" });
  assert.equal(retried.task.status, "waiting_for_query_approval");
});

test("autonomous query claims before response and shares result Artifact without human approval event", async () => {
  const { application, task, calls } = fixture("autonomous");
  await application.attachMandate(task.task_run_id, { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  const prepared = await application.prepareQuery(task.task_run_id, { question: "Seven", candidate, idempotencyKey: "prepare-auto" });
  assert.equal(prepared.task.status === "querying" || prepared.task.status === "completed", true);
  assert.equal(calls.filter(call => call.startsWith("execute:")).length, 1);
  assert.equal(prepared.events.some(event => event.event_type === "query.execution_authorized"), true);
  const completed = await application.finishQueryExecution(task.task_run_id, "qr_demo");
  assert.deepEqual(completed.artifact.payload.rows, [[7]]);
  assert.equal(application.getArtifacts(task.task_run_id).filter(item => item.artifact_type === "query_result").length, 1);
  assert.equal(calls.filter(call => call.startsWith("execute:")).length, 1);
});

test("designated independent reviewer alone can claim; token never enters persisted events", async () => {
  const { application, task, calls } = fixture("human_review");
  await application.attachMandate(task.task_run_id, { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  await application.prepareQuery(task.task_run_id, { question: "Seven", candidate });
  assert.equal(application.getQueryReviewProjection(task.task_run_id, principal("bob", "reviewer"))?.query_run_id, "qr_demo");
  const input = { queryRunId: "qr_demo", sqlHash, assuranceReportHash: assuranceHash,
    enforcementContextHash: contextHash, idempotencyKey: "approve-1", controlToken: "one-use-secret" };
  await assert.rejects(application.approveQuery(task.task_run_id, input, principal("alice", "admin")));
  assert.deepEqual(calls.filter(call => call.startsWith("approve:")), []);
  const claimed = await application.approveQuery(task.task_run_id, input, principal("bob", "reviewer"));
  assert.equal(claimed.result.status, "executing");
  assert.equal(application.getQueryReviewProjection(task.task_run_id, principal("bob", "reviewer")), undefined);
  assert.equal(calls.filter(call => call.startsWith("approve:")).length, 1);
  assert.equal(JSON.stringify(application.getEvents(task.task_run_id)).includes("one-use-secret"), false);
  await application.finishQueryExecution(task.task_run_id, "qr_demo");
  assert.equal(application.getQueryReviewProjection(task.task_run_id, principal("bob", "reviewer")), undefined);
});

test("channel approval card is visible only to designated reviewer", async () => {
  const { application, task } = fixture("human_review");
  await application.attachMandate(task.task_run_id, { mandateId: "md_demo", expectedMandateId: null }, principal("alice", "admin"));
  await application.prepareQuery(task.task_run_id, { question: "Seven", candidate });
  assert.equal(application.getChannelPresentation(task.task_run_id, principal("alice", "admin")).actions
    .some(action => action.type === "approve_query"), false);
  assert.equal(application.getChannelPresentation(task.task_run_id, principal("bob", "reviewer")).actions
    .some(action => action.type === "approve_query"), true);
  assert.equal(application.getChannelPresentation(task.task_run_id).actions
    .some(action => action.type === "approve_query"), false);
});
