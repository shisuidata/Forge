import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadConfig } from "../src/config.js";
import { createOrchestratorServer } from "../src/server.js";

function address(server: { address(): unknown }): string {
  return `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
}

const principal = {
  principal_id: "alice", principal_type: "human", organization_id: "org_demo", workspace_id: "team_demo",
  role: "admin", identity_revision: 1, credential_id: "cred_alice", credential_revision: 1,
  authentication_method: "api_key", authenticated_at: "2026-09-23T00:00:00Z",
  channel_binding_id: null, channel_binding_revision: null,
};

test("benchmark authentication remains separate from task control", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-benchmark-auth-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const server = createOrchestratorServer({ ...loadConfig({}), agentDir: directory,
    channelServiceKeys: ["benchmark-key"], stateDbPath: join(directory, "state.sqlite") });
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  assert.equal((await fetch(address(server) + "/health/live")).status, 200);
  assert.equal((await fetch(address(server) + "/v1/benchmarks", { headers: { "x-channel-service-key": "benchmark-key" } })).status, 200);
  assert.equal((await fetch(address(server) + "/v1/tasks", { headers: { "x-channel-service-key": "benchmark-key" } })).status, 401);
});

test("task requests resolve byte-bound tickets before parsing, enforce scope, and keep internal scope service-only", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-control-test-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const tickets = new Map<string, { method: string; path: string; body: string; action: string; task: string | null; principal?: typeof principal }>();
  let resolves = 0;
  const forge = createServer(async (request, response) => {
    if (request.url !== "/api/internal/governance/control-tokens/resolve" || request.headers["x-api-key"] !== "pi-key") {
      response.writeHead(401).end(); return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const payload = JSON.parse(Buffer.concat(chunks).toString()) as Record<string, string | null>;
    resolves++;
    const ticket = tickets.get(payload.token ?? "");
    if (!ticket || payload.method !== ticket.method || payload.path !== ticket.path ||
        payload.body_hash !== `sha256:${createHash("sha256").update(ticket.body).digest("hex")}`) {
      response.writeHead(403).end(); return;
    }
    response.writeHead(200, { "content-type": "application/json" });
    response.end(JSON.stringify({ principal: ticket.principal ?? principal, action: ticket.action, task_run_id: ticket.task,
      query_run_id: null, approval_hashes: null, idempotency_key: null }));
  });
  await new Promise<void>(resolve => forge.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise<void>(resolve => forge.close(() => resolve())));
  const config = loadConfig({ PI_ORCHESTRATOR_AGENT_DIR: directory, PI_ORCHESTRATOR_STATE_DB: join(directory, "state.sqlite"),
    PI_CHANNEL_SERVICE_KEYS: "service-key", FORGE_PI_SERVICE_KEY: "pi-key", FORGE_BASE_URL: address(forge) });
  const server = createOrchestratorServer(config);
  await new Promise<void>(resolve => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const base = address(server);
  const body = JSON.stringify({ channel: "web", intent: "query_prepare", message: "hello" });
  tickets.set("create", { method: "POST", path: "/v1/tasks", body, action: "task.write", task: null });
  const headers = { "x-channel-service-key": "service-key", "x-forge-control-token": "create", "content-type": "application/json" };
  assert.equal((await fetch(base + "/v1/tasks", { method: "POST", headers: { "x-channel-service-key": "service-key" }, body })).status, 401);
  assert.equal((await fetch(base + "/v1/tasks", { method: "POST", headers, body: body + " " })).status, 403);
  assert.equal((await fetch(base + "/v1/tasks?extra=1", { method: "POST", headers, body })).status, 403);
  assert.equal((await fetch(base + "/v1/tasks", { method: "POST", headers, body: JSON.stringify({ ...JSON.parse(body), user_id: "bob" }) })).status, 403);
  const forgedBody = JSON.stringify({ channel: "web", intent: "query_prepare", message: "hello", user_id: "bob" });
  tickets.set("forged", { method: "POST", path: "/v1/tasks", body: forgedBody, action: "task.write", task: null });
  assert.equal((await fetch(base + "/v1/tasks", { method: "POST", headers: { ...headers, "x-forge-control-token": "forged" }, body: forgedBody })).status, 403);
  tickets.set("wrong-action", { method: "POST", path: "/v1/tasks", body, action: "task.read", task: null });
  assert.equal((await fetch(base + "/v1/tasks", { method: "POST", headers: { ...headers, "x-forge-control-token": "wrong-action" }, body })).status, 403);
  const created = await fetch(base + "/v1/tasks", { method: "POST", headers, body });
  assert.equal(created.status, 201);
  const task = (await created.json() as { task: { task_run_id: string; org_id: string; team_id: string; user_id: string } }).task;
  assert.deepEqual([task.org_id, task.team_id, task.user_id], ["org_demo", "team_demo", "alice"]);
  assert.ok(resolves >= 4);
  const attachBody = JSON.stringify({ mandate_id: "md_demo", expected_mandate_id: null });
  tickets.set("attach-member", { method: "POST", path: `/v1/tasks/${task.task_run_id}/mandate`, body: attachBody,
    action: "task.attach_mandate", task: task.task_run_id, principal: { ...principal, role: "member" } });
  const attachResponse = await fetch(`${base}/v1/tasks/${task.task_run_id}/mandate`, { method: "POST",
    headers: { "x-channel-service-key": "service-key", "x-forge-control-token": "attach-member", "idempotency-key": "attach-1" }, body: attachBody });
  assert.equal(attachResponse.status, 403);
  const scope = `${base}/v1/internal/task-scopes/${task.task_run_id}`;
  assert.equal((await fetch(scope)).status, 401);
  const scopeResponse = await fetch(scope, { headers: { "x-channel-service-key": "service-key" } });
  assert.equal(scopeResponse.status, 200);
  const internal = await scopeResponse.json() as Record<string, unknown>;
  assert.equal(internal.user_id, "alice");
  assert.equal(internal.mandate_id, null);
  assert.equal((internal as Record<string, unknown>).message, undefined);
  tickets.set("list", { method: "GET", path: "/v1/tasks?org_id=org_demo&team_id=team_demo", body: "", action: "task.read", task: null });
  const list = await fetch(base + "/v1/tasks?team_id=team_demo&org_id=org_demo",
    { headers: { "x-channel-service-key": "service-key", "x-forge-control-token": "list" } });
  assert.equal(list.status, 200);
  tickets.set("read", { method: "GET", path: `/v1/tasks/${task.task_run_id}`, body: "", action: "task.read", task: task.task_run_id });
  assert.equal((await fetch(`${base}/v1/tasks/${task.task_run_id}`, { headers: { "x-channel-service-key": "service-key", "x-forge-control-token": "read" } })).status, 200);
  assert.equal((await fetch(`${base}/v1/tasks/${task.task_run_id}/artifacts`, { headers: { "x-channel-service-key": "service-key", "x-forge-control-token": "read" } })).status, 403);
});
