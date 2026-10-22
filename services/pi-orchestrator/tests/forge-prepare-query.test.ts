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


test("governed Forge client submits candidate with mandate only, no caller-supplied identity", async (context) => {
  const seen: Array<{ path: string; body: Record<string, unknown>; key: string | undefined }> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    seen.push({ path: request.url ?? "", body, key: request.headers["x-api-key"] as string | undefined });
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ schema_version: 2, query_run_id: "qr_demo", task_run_id: "tr_demo",
      status: "review_required", decision: { execution_mode: "human_review", execution_authorized: false },
      candidate: { input_kind: "direct_sql" }, lineage: { registry_revision: "registry" },
      review: { sql: "SELECT 7", sql_hash: "sha256:" + "a".repeat(64),
        assurance_report_hash: "sha256:" + "b".repeat(64), enforcement_context_hash: "sha256:" + "c".repeat(64), expires_at: "2099-01-01T00:00:00Z" } }));
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const address = server.address() as AddressInfo;
  const client = new ForgeQueryRunClient({ baseUrl: "http://127.0.0.1:" + address.port, serviceKey: "pi-service", timeoutMs: 2_000 });
  const prepared = await client.createQueryRun({ taskRunId: "tr_demo", mandateId: "md_demo", purpose: "bounded",
    question: "Seven", dialect: "sqlite", candidate: { kind: "direct_sql", sql: "SELECT 7" }, idempotencyKey: "prepare-key" });
  assert.equal(prepared.status, "review_required");
  assert.deepEqual(seen, [{ path: "/api/v2/enforce/query-runs", key: "pi-service", body: {
    schema_version: 2, mandate_id: "md_demo", task_run_id: "tr_demo", purpose: "bounded", question: "Seven",
    dialect: "sqlite", candidate: { kind: "direct_sql", sql: "SELECT 7" },
  } }]);
});

test("Pi query tool can only use injected mandate and never exposes identity or approval", async () => {
  const calls: Record<string, unknown>[] = [];
  const client = { async createQueryRun(input: Record<string, unknown>) {
    calls.push(input);
    return { query_run_id: "qr_demo", task_run_id: "tr_demo", status: "review_required",
      review: { sql: "SELECT 7", sql_hash: "hash", assurance_report_hash: "assurance",
        enforcement_context_hash: "context", expires_at: "2099-01-01T00:00:00Z" },
      lineage: { registry_revision: "registry" }, decision: { review_required: true, execution_mode: "human_review" },
      failure: null };
  } } as unknown as ForgeQueryRunClient;
  const tool = createForgePrepareQueryTool({ client, task: { taskRunId: "tr_demo", mandateId: "md_demo", purpose: "bounded" } });
  const result = await tool.execute("tool-call", { question: "Seven", dialect: "sqlite" }, undefined, undefined, {} as ExtensionContext);
  assert.equal(calls[0]?.mandateId, "md_demo");
  assert.equal(calls[0]?.taskRunId, "tr_demo");
  assert.equal("userId" in calls[0]!, false);
  assert.equal("orgId" in calls[0]!, false);
  assert.match(result.content[0]?.type === "text" ? result.content[0].text : "", /qr_demo/);
});

test("candidate-free request first generates candidate, then prepares only through public v2", async (context) => {
  const paths: string[] = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<string, unknown>;
    paths.push(request.url ?? "");
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify(request.url === "/api/internal/query-candidates"
      ? { candidate: { kind: "direct_sql", sql: "SELECT 7" } }
      : { schema_version: 2, query_run_id: "qr_demo", task_run_id: "tr_demo", status: "ready",
        decision: { execution_mode: "autonomous", execution_authorized: false },
        candidate: { input_kind: "direct_sql" }, lineage: { registry_revision: "registry" },
        review: { sql_hash: "sha256:" + "a".repeat(64), assurance_report_hash: "sha256:" + "b".repeat(64),
          enforcement_context_hash: "sha256:" + "c".repeat(64) } }));
    if (request.url === "/api/v2/enforce/query-runs") assert.deepEqual(body.candidate, { kind: "direct_sql", sql: "SELECT 7" });
  });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  context.after(() => server.close());
  const client = new ForgeQueryRunClient({ baseUrl: "http://127.0.0.1:" + (server.address() as AddressInfo).port,
    serviceKey: "pi-service", timeoutMs: 2_000 });
  const prepared = await client.createQueryRun({ taskRunId: "tr_demo", mandateId: "md_demo", purpose: "bounded",
    question: "Seven", idempotencyKey: "prepare" });
  assert.equal(prepared.status, "ready");
  assert.deepEqual(paths, ["/api/internal/query-candidates", "/api/v2/enforce/query-runs"]);
});
