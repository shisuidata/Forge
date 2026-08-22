import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import test from "node:test";

import type { ExtensionContext } from "@earendil-works/pi-coding-agent";

import { ForgeClient, ForgeClientError } from "../src/forge/client.js";
import { ForgeQueryRunClient } from "../src/forge/query-run-client.js";
import { createForgePrepareQueryTool } from "../src/tools/forge-prepare-query.js";

interface MockForgeOptions {
  statusCode?: number;
  response: unknown;
}

async function startMockForge(context: { after: (fn: () => void) => void }, options: MockForgeOptions) {
  let receivedBody: Record<string, unknown> | undefined;
  let receivedApiKey: string | undefined;
  let receivedPiServiceKey: string | undefined;
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    receivedBody = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<
      string,
      unknown
    >;
    receivedApiKey = request.headers["x-api-key"] as string | undefined;
    receivedPiServiceKey = request.headers["x-pi-service-key"] as string | undefined;
    response.writeHead(options.statusCode ?? 200, { "content-type": "application/json" });
    response.end(JSON.stringify(options.response));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  return {
    baseUrl: `http://127.0.0.1:${address.port}`,
    received: () => ({ body: receivedBody, apiKey: receivedApiKey, piServiceKey: receivedPiServiceKey }),
  };
}

function validResponse(overrides: Record<string, unknown> = {}) {
  return {
    status: "needs_review",
    question: "查询订单 ID",
    user_id: "trusted-user",
    forge_json: { scan: "orders", select: ["orders.id"] },
    sql: "SELECT orders.id FROM orders",
    dialect: "postgresql",
    review_required: true,
    can_execute: false,
    retry_count: 0,
    text: "",
    error: "",
    ...overrides,
  };
}


test("Forge client forwards the trusted user and service API key", async (context) => {
  const mock = await startMockForge(context, { response: validResponse() });
  const client = new ForgeClient({
    baseUrl: mock.baseUrl,
    apiKey: "service-secret",
    timeoutMs: 2_000,
  });

  const result = await client.prepareQuery({
    question: "查询订单 ID",
    userId: "trusted-user",
    dialect: "postgresql",
  });

  assert.equal(result.status, "needs_review");
  assert.equal(result.can_execute, false);
  assert.deepEqual(mock.received().body, {
    question: "查询订单 ID",
    user_id: "trusted-user",
    dialect: "postgresql",
  });
  assert.equal(mock.received().apiKey, "service-secret");
});


test("Forge client fails closed if prepare-query claims it can execute", async (context) => {
  const mock = await startMockForge(context, {
    response: validResponse({ can_execute: true }),
  });
  const client = new ForgeClient({ baseUrl: mock.baseUrl, timeoutMs: 2_000 });

  await assert.rejects(
    () => client.prepareQuery({ question: "查询订单 ID", userId: "trusted-user" }),
    /non-executable review boundary/,
  );
});


test("Forge client rejects execution results on the prepare-only endpoint", async (context) => {
  const mock = await startMockForge(context, {
    response: validResponse({ rows: [[1]], columns: ["id"] }),
  });
  const client = new ForgeClient({ baseUrl: mock.baseUrl, timeoutMs: 2_000 });

  await assert.rejects(
    () => client.prepareQuery({ question: "查询订单 ID", userId: "trusted-user" }),
    /unexpectedly returned execution results/,
  );
});


test("Forge client preserves bounded HTTP failure information", async (context) => {
  const mock = await startMockForge(context, {
    statusCode: 401,
    response: { detail: "Unauthorized" },
  });
  const client = new ForgeClient({ baseUrl: mock.baseUrl, timeoutMs: 2_000 });

  await assert.rejects(
    () => client.prepareQuery({ question: "查询订单 ID", userId: "trusted-user" }),
    (error: unknown) => error instanceof ForgeClientError && error.statusCode === 401,
  );
});


test("Forge context client validates bounded evidence and forwards Pi identity", async (context) => {
  const evidenceRef = `ctx_${"a".repeat(24)}`;
  const mock = await startMockForge(context, {
    response: {
      status: "ok", question: "销售额口径", bounded: true,
      evidence: [{ evidence_ref: evidenceRef, source_type: "metric", title: "销售额",
        content: "默认使用订单支付金额", score: 9 }],
      evidence_count: 1, context_revision: `sha256:${"b".repeat(64)}`,
    },
  });
  const client = new ForgeQueryRunClient({
    baseUrl: mock.baseUrl, serviceKey: "pi-service-secret", timeoutMs: 2_000,
  });

  const result = await client.searchContext({
    orgId: "org_demo", teamId: "team_demo", userId: "trusted-user", question: "销售额口径",
  });

  assert.equal(result.evidence[0]?.evidence_ref, evidenceRef);
  assert.equal(mock.received().piServiceKey, "pi-service-secret");
  assert.equal(mock.received().body?.user_id, "trusted-user");
});

test("Forge report client accepts only bounded publication metadata", async (context) => {
  const mock = await startMockForge(context, {
    response: {
      status: "accepted",
      report: {
        report_id: "rp_demo001", task_run_id: "tr_demo001", revision: 1,
        bundle_hash: `sha256:${"a".repeat(64)}`, title: "分析报告", status: "published",
        pdf_status: "ready", pptx_status: "ready",
        internal_url: "https://forge.test/reports/rp_demo001",
        technical_url: "https://forge.test/reports/rp_demo001/technical",
        pdf_url: "https://forge.test/reports/rp_demo001/download/pdf",
        pptx_url: "https://forge.test/reports/rp_demo001/download/pptx",
        created_at: "2026-08-21T17:00:00Z", updated_at: "2026-08-21T17:00:01Z",
      },
    },
  });
  const client = new ForgeQueryRunClient({
    baseUrl: mock.baseUrl, serviceKey: "pi-service-secret", timeoutMs: 2_000,
  });
  const result = await client.createReport({
    report_id: "rp_demo001", task_run_id: "tr_demo001", bundle_hash: `sha256:${"a".repeat(64)}`,
  }, "report-idempotency");
  assert.equal(result.status, "published");
  assert.equal(result.pdf_url, "https://forge.test/reports/rp_demo001/download/pdf");
  assert.equal(mock.received().piServiceKey, "pi-service-secret");
});

test("Pi tool creates a persisted QueryRun with identity from TaskRun", async (context) => {
  const mock = await startMockForge(context, {
    response: {
      query_run_id: "qr_demo_001",
      task_run_id: "tr_demo",
      status: "needs_review",
      question: "查询订单 ID",
      user_id: "trusted-user",
      datasource_id: "demo",
      forge_json: { scan: "orders", select: ["orders.id"] },
      sql: "SELECT orders.id FROM orders",
      sql_hash: "sha256:reviewed",
      dialect: "postgresql",
      registry_version: "sha256:registry",
      assurance_report: { status: "passed" },
      assurance_report_hash: `sha256:${"b".repeat(64)}`,
      assurance_revision: "query-assurance-v1",
      policy_revision: "convention-policy-v1",
      model_revision: "sha256:model",
      assurance_registry_revision: "sha256:assurance-registry",
      review_required: true,
      can_execute: false,
      expires_at: "2026-08-21T18:00:00Z",
      error: "",
    },
  });
  const client = new ForgeQueryRunClient({
    baseUrl: mock.baseUrl,
    serviceKey: "pi-service-secret",
    timeoutMs: 2_000,
  });
  const tool = createForgePrepareQueryTool({
    client,
    task: {
      taskRunId: "tr_demo",
      orgId: "org_demo",
      teamId: "team_growth",
      userId: "trusted-user",
    },
  });

  const result = await tool.execute(
    "tool_call_1",
    { question: "查询订单 ID", dialect: "postgresql" },
    undefined,
    undefined,
    {} as ExtensionContext,
  );

  assert.equal(tool.name, "forge_prepare_query");
  assert.equal(mock.received().body?.user_id, "trusted-user");
  assert.equal(mock.received().body?.task_run_id, "tr_demo");
  assert.match(result.content[0]?.type === "text" ? result.content[0].text : "", /qr_demo_001/);
});
