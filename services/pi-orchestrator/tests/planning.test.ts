import assert from "node:assert/strict";
import test from "node:test";

import type { Artifact } from "../src/artifacts.js";
import { routeChannelMessage } from "../src/channels/intent.js";
import {
  buildExecutionPlan,
  reviseExecutionPlan,
  validateExecutionPlanPayload,
} from "../src/planning.js";

test("structured channel router distinguishes action, workflow, clarification and forbidden", () => {
  assert.equal(routeChannelMessage("记住我以后默认看自然月").kind, "action");
  assert.equal(routeChannelMessage("分析销售额下降原因并生成图表报告").kind, "workflow");
  assert.equal(routeChannelMessage("看看").kind, "clarification");
  assert.equal(routeChannelMessage("帮我 DROP TABLE orders").kind, "forbidden");
  const workflow = routeChannelMessage("分析销售额下降原因并生成图表报告");
  assert.equal(workflow.requires_fresh_data, true);
  assert.deepEqual(workflow.requested_deliverables, ["query_result", "analysis", "chart", "report"]);
});

test("deliverable planner creates dependency-bound workflow and immutable revisions", () => {
  const route = routeChannelMessage("分析销售额下降原因并生成图表报告");
  const initial = buildExecutionPlan(route, "分析销售额下降原因并生成图表报告");
  assert.equal(validateExecutionPlanPayload(initial), undefined);
  assert.deepEqual(initial.steps.map((step) => step.capability), ["query", "analysis", "chart", "report"]);
  assert.equal(initial.steps[0]?.status, "ready");
  assert.equal(initial.steps[1]?.status, "pending");

  const artifact = {
    artifact_id: "ar_plan_001", artifact_type: "execution_plan", schema_version: 1,
    task_run_id: "tr_demo", producer: "pi-planner", created_at: new Date().toISOString(), payload: initial,
  } as Artifact;
  const afterQuery = reviseExecutionPlan(artifact, { query: "completed" });
  assert.equal(afterQuery.plan_revision, 2);
  assert.equal(afterQuery.supersedes_artifact_id, "ar_plan_001");
  assert.equal(afterQuery.steps.find((step) => step.capability === "analysis")?.status, "ready");
  assert.equal(afterQuery.steps.find((step) => step.capability === "chart")?.status, "ready");
});

test("execution plan validation rejects unsupported state and dependency", () => {
  const plan = buildExecutionPlan(routeChannelMessage("统计订单量"), "统计订单量");
  const invalid = structuredClone(plan);
  invalid.steps[0]!.depends_on = ["step_missing"];
  assert.match(validateExecutionPlanPayload(invalid) ?? "", /dependency/);
});
