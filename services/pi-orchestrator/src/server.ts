import { createHash, timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { pathToFileURL } from "node:url";

import { OrchestratorApplication, type TrustedRequestPrincipal } from "./application.js";
import { BenchmarkInputError, PiBenchmarkRuntime } from "./benchmark-runtime.js";
import { parseChannelEvent } from "./channels/contracts.js";
import { ChannelIdentityError, identityFromTrustedBinding } from "./channels/identity.js";
import { loadConfig, type OrchestratorConfig } from "./config.js";
import { FORGE_DIALECTS, type ForgeDialect } from "./forge/client.js";
import type { QueryCandidateInput } from "./forge/query-run-client.js";
import { inspectRuntime } from "./runtime.js";
import {
  ADVISORY_SKILL_NAMES,
  AUTHORIZED_SKILL_NAMES,
  type AdvisorySkillName,
  type AuthorizedSkillName,
} from "./skills.js";
import { SkillPolicyConflictError } from "./skill-policy.js";
import { SqliteOrchestratorState } from "./sqlite-store.js";
import {
  TASK_STATUSES,
  TaskStateError,
  type TaskChannel,
  type TaskStatus,
} from "./task-store.js";
import { correlatedHandler, currentRequestId } from "./request-context.js";
import { ForgeClientError } from "./forge/client.js";

const MAX_BODY_BYTES = 64 * 1024;
const CHANNELS = new Set<TaskChannel>(["web", "feishu", "dingtalk", "api"]);
const CONVERSATION_CHANNELS = new Set<Exclude<TaskChannel, "api">>(["web", "feishu", "dingtalk"]);
const TASK_STATUS_SET = new Set<string>(TASK_STATUSES);
const DIALECTS = new Set<string>(FORGE_DIALECTS);
const AUTHORIZED_SKILLS = new Set<string>(AUTHORIZED_SKILL_NAMES);
const ADVISORY_SKILLS = new Set<string>(ADVISORY_SKILL_NAMES);

class RequestError extends Error {}

function parseQueryCandidate(value: unknown): QueryCandidateInput | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new RequestError("candidate must be an object");
  }
  const candidate = value as Record<string, unknown>;
  const producerRevision = candidate.producer_revision;
  if (
    producerRevision !== undefined &&
    (typeof producerRevision !== "string" || producerRevision.trim().length === 0)
  ) {
    throw new RequestError("candidate producer_revision must be a non-empty string");
  }
  if (candidate.kind === "direct_sql") {
    if (Object.keys(candidate).some((key) => !["kind", "sql", "producer_revision"].includes(key))) {
      throw new RequestError("direct_sql candidate has unsupported fields");
    }
    if (typeof candidate.sql !== "string" || candidate.sql.trim().length === 0) {
      throw new RequestError("direct_sql candidate requires non-empty sql");
    }
    return {
      kind: "direct_sql",
      sql: candidate.sql,
      ...(producerRevision === undefined ? {} : { producer_revision: producerRevision }),
    };
  }
  if (candidate.kind === "forge_json") {
    if (
      Object.keys(candidate).some(
        (key) => !["kind", "forge_json", "producer_revision"].includes(key),
      )
    ) {
      throw new RequestError("forge_json candidate has unsupported fields");
    }
    if (
      typeof candidate.forge_json !== "object" ||
      candidate.forge_json === null ||
      Array.isArray(candidate.forge_json)
    ) {
      throw new RequestError("forge_json candidate requires an object payload");
    }
    return {
      kind: "forge_json",
      forge_json: candidate.forge_json as Record<string, unknown>,
      ...(producerRevision === undefined ? {} : { producer_revision: producerRevision }),
    };
  }
  throw new RequestError("candidate kind must be direct_sql or forge_json");
}

class ChannelAuthenticationError extends Error {}

class ControlAuthenticationError extends Error { constructor(message: string, readonly statusCode = 403) { super(message); } }
type ControlAction = "task.read" | "task.write" | "task.attach_mandate" | "query.approve";
interface ResolvedControl {
  principal: TrustedRequestPrincipal;
  action: ControlAction;
  task_run_id: string | null;
  query_run_id: string | null;
  approval_hashes: Record<string, string> | null;
  idempotency_key: string | null;
}
const rawBodies = new WeakMap<IncomingMessage, Buffer>();
async function readRaw(request: IncomingMessage): Promise<Buffer> {
  const cached = rawBodies.get(request);
  if (cached) return cached;
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const bytes = Buffer.from(chunk);
    size += bytes.length;
    if (size > MAX_BODY_BYTES) throw new RequestError("request body is too large");
    chunks.push(bytes);
  }
  const bytes = Buffer.concat(chunks);
  rawBodies.set(request, bytes);
  return bytes;
}
function canonicalRequestPath(url: URL): string {
  const compare = (a: string, b: string) => a < b ? -1 : a > b ? 1 : 0;
  const params = [...url.searchParams.entries()].sort(([ak, av], [bk, bv]) => compare(ak, bk) || compare(av, bv));
  const query = new URLSearchParams(params).toString();
  return url.pathname + (query ? "?" + query : "");
}
async function resolveRequestControl(request: IncomingMessage, url: URL, config: OrchestratorConfig): Promise<ResolvedControl> {
  requireChannelAuthentication(request, config.channelServiceKeys);
  const token = request.headers["x-forge-control-token"];
  if (typeof token !== "string" || !token || !config.forgePiServiceKey) {
    throw new ControlAuthenticationError("Missing control credential", 401);
  }
  const raw = await readRaw(request);
  let response: Response;
  try {
    response = await fetch(config.forgeBaseUrl + "/api/internal/governance/control-tokens/resolve", {
      method: "POST",
      headers: { "X-API-Key": config.forgePiServiceKey, "Content-Type": "application/json" },
      body: JSON.stringify({ token, method: request.method, path: canonicalRequestPath(url),
        body_hash: "sha256:" + createHash("sha256").update(raw).digest("hex"),
        idempotency_key: request.headers["idempotency-key"] ?? null }),
      signal: AbortSignal.timeout(Math.min(config.forgeTimeoutMs, 10000)),
    });
  } catch {
    throw new ControlAuthenticationError("Control authority unavailable", 503);
  }
  if (!response.ok) throw new ControlAuthenticationError("Invalid or expired control token", response.status === 503 ? 503 : 403);
  const result = await response.json() as ResolvedControl;
  if (!result || !result.principal || !result.principal.principal_id) throw new ControlAuthenticationError("Invalid control response", 503);
  return result;
}
function assertControl(control: ResolvedControl, action: ControlAction, taskRunId?: string): void {
  if (control.action !== action || (taskRunId !== undefined && control.task_run_id !== taskRunId)) {
    throw new ControlAuthenticationError("Control action or task scope mismatch");
  }
}
function assertTaskAccess(application: OrchestratorApplication, principal: TrustedRequestPrincipal, taskId: string) {
  const task = application.getTask(taskId);
  if (!task || task.org_id !== principal.organization_id || task.team_id !== principal.workspace_id ||
      (task.user_id !== principal.principal_id && !(principal.principal_type === "human" && principal.role === "admin"))) {
    throw new ControlAuthenticationError("Task not found", 404);
  }
  return task;
}
function assertScope(principal: TrustedRequestPrincipal, orgId: string, teamId: string, userId?: string): void {
  if (orgId !== principal.organization_id || teamId !== principal.workspace_id ||
      (userId !== undefined && userId !== principal.principal_id && !(principal.principal_type === "human" && principal.role === "admin"))) {
    throw new ControlAuthenticationError("Scope not found", 404);
  }
}
function secureEquals(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  return leftBuffer.length === rightBuffer.length && timingSafeEqual(leftBuffer, rightBuffer);
}

function requireChannelAuthentication(
  request: IncomingMessage,
  configuredKeys: string[],
): void {
  const key = request.headers["x-channel-service-key"];
  if (
    configuredKeys.length === 0 ||
    typeof key !== "string" ||
    !configuredKeys.some((candidate) => secureEquals(candidate, key))
  ) {
    throw new ChannelAuthenticationError("Invalid channel service credential");
  }
}


function sendJson(response: ServerResponse, statusCode: number, body: unknown): void {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  const payload = statusCode >= 400 && body && typeof body === "object" && !Array.isArray(body)
    ? { ...body, request_id: currentRequestId() } : body;
  response.end(JSON.stringify(payload));
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  try {
    const value = JSON.parse((await readRaw(request)).toString("utf8")) as unknown;
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new RequestError("request body must be a JSON object");
    }
    return value as Record<string, unknown>;
  } catch (error) {
    if (error instanceof RequestError) throw error;
    throw new RequestError("request body must contain valid JSON");
  }
}

function sendAccepted(
  response: ServerResponse,
  application: OrchestratorApplication,
  taskRunId: string,
): void {
  sendJson(response, 202, {
    status: "accepted",
    task: application.getTask(taskRunId),
    attempts: application.getStageAttempts(taskRunId),
    poll: {
      task: `/v1/tasks/${taskRunId}`,
      events: `/v1/tasks/${taskRunId}/events`,
      artifacts: `/v1/tasks/${taskRunId}/artifacts`,
      attempts: `/v1/tasks/${taskRunId}/attempts`,
    },
  });
}

async function maybeRunAsync<T>(options: {
  response: ServerResponse;
  application: OrchestratorApplication;
  taskRunId: string;
  respondAsync: boolean;
  operation: Promise<T>;
}): Promise<T | undefined> {
  if (!options.respondAsync) return await options.operation;
  let settled: { value: T } | { error: unknown } | undefined;
  void options.operation.then(
    (value) => { settled = { value }; },
    (error: unknown) => { settled = { error }; },
  );
  await Promise.resolve();
  if (settled !== undefined && "error" in settled) throw settled.error;
  if (settled !== undefined && "value" in settled) return settled.value;
  sendAccepted(options.response, options.application, options.taskRunId);
  return undefined;
}

function requireScopeId(encodedValue: string, field: string): string {
  let value: string;
  try {
    value = decodeURIComponent(encodedValue);
  } catch {
    throw new RequestError(`${field} is not valid URL encoding`);
  }
  if (!/^[A-Za-z0-9_.-]{1,128}$/.test(value)) {
    throw new RequestError(`${field} contains unsupported characters`);
  }
  return value;
}

function requireProductIdentifier(encodedValue: string, field: string): string {
  let value: string;
  try {
    value = decodeURIComponent(encodedValue);
  } catch {
    throw new RequestError(`${field} is not valid URL encoding`);
  }
  if (!/^[A-Za-z0-9_.:-]{1,256}$/.test(value)) {
    throw new RequestError(`${field} contains unsupported characters`);
  }
  return value;
}

function requireString(body: Record<string, unknown>, field: string): string {
  const value = body[field];
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new RequestError(`${field} must be a non-empty string`);
  }
  return value;
}

export function createOrchestratorServer(
  config: OrchestratorConfig,
  application?: OrchestratorApplication,
) {
  let state: SqliteOrchestratorState | undefined;
  let reconciliationTimer: NodeJS.Timeout | undefined;
  if (application === undefined) {
    state = new SqliteOrchestratorState(config.stateDbPath);
    state.reconcileExpiredAttempts();
    reconciliationTimer = setInterval(
      () => {
        state?.reconcileExpiredAttempts();
        application?.observePendingQueryExecutions();
      },
      config.reconciliationIntervalMs,
    );
    reconciliationTimer.unref();
    application = new OrchestratorApplication({
      config,
      state,
    });
  }
  application.observePendingQueryExecutions();
  const benchmarkRuntime = new PiBenchmarkRuntime(config, application);
  const server = createServer(correlatedHandler(async (request, response) => {
    try {
      const url = new URL(
        request.url ?? "/",
        `http://${request.headers.host ?? "localhost"}`,
      );

      if (request.method === "GET" && url.pathname === "/health/live") {
        sendJson(response, 200, { status: "ok", service: "forge-pi-orchestrator" });
        return;
      }

      if (
        request.method === "GET" &&
        (url.pathname === "/health/readiness" || url.pathname === "/v1/runtime/capabilities")
      ) {
        try {
          const capabilities = await inspectRuntime(config);
          sendJson(response, 200, {
            status: capabilities.modelExecutionConfigured ? "ok" : "degraded",
            capabilities: {
              ...capabilities,
              channelIngressConfigured: config.channelServiceKeys.length > 0 && !!config.forgePiServiceKey,
              authorizedChannelIdentities: "forge-governed",
            },
          });
        } catch (error) {
          sendJson(response, 503, {
            status: "fail",
            error: error instanceof Error ? error.message : "runtime inspection failed",
          });
        }
        return;
      }
      const internalScope = url.pathname.match(/^\/v1\/internal\/task-scopes\/(tr_[A-Za-z0-9_-]+)$/);
      if (request.method === "GET" && internalScope?.[1]) {
        requireChannelAuthentication(request, config.channelServiceKeys);
        const task = application.getTask(internalScope[1]);
        if (!task) { sendJson(response, 404, { status: "not_found" }); return; }
        sendJson(response, 200, { task_id: task.task_run_id, org_id: task.org_id, team_id: task.team_id,
          user_id: task.user_id, mandate_id: task.metadata.mandate_id ?? null,
          execution_actor_id: task.metadata.execution_actor_id ?? null, status: task.status });
        return;
      }
      let control: ResolvedControl | undefined;
      if (url.pathname.startsWith("/v1/tasks") || url.pathname.startsWith("/v1/conversations") ||
          url.pathname === "/v1/channel-events" || /^\/v1\/orgs\/[^/]+\/teams\/[^/]+\/skill-policy$/.test(url.pathname)) {
        control = await resolveRequestControl(request, url, config);
        const taskId = /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)(?:\/|$)/.exec(url.pathname)?.[1];
        const action: ControlAction = request.method === "GET" ? "task.read"
          : url.pathname.endsWith("/mandate") ? "task.attach_mandate"
          : url.pathname.endsWith("/approve-query") ? "query.approve" : "task.write";
        if (url.pathname !== "/v1/channel-events") assertControl(control, action, taskId);
        if (taskId && !url.pathname.endsWith("/approve-query") && !url.pathname.endsWith("/query-review")) assertTaskAccess(application, control.principal, taskId);
      }

      if (url.pathname === "/v1/benchmarks/models" && request.method === "GET") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        sendJson(response, 200, { models: await benchmarkRuntime.modelOptions() });
        return;
      }
      if (url.pathname === "/v1/benchmarks" && request.method === "POST") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        const body = await readJson(request);
        const provider = requireString(body, "provider");
        const model = requireString(body, "model");
        if (body.limit !== undefined && (typeof body.limit !== "number" || !Number.isSafeInteger(body.limit) || body.limit <= 0)) throw new RequestError("limit must be a positive integer");
        if (!Number.isSafeInteger(body.confirm_model_calls) || Number(body.confirm_model_calls) < 0) throw new RequestError("Explicit nonnegative confirm_model_calls is required");
        if (body.case_ids !== undefined && (!Array.isArray(body.case_ids) || !body.case_ids.length || body.case_ids.some((id) => typeof id !== "string" || !id.trim()) || new Set(body.case_ids).size !== body.case_ids.length)) throw new RequestError("case_ids must contain unique nonempty IDs");
        if (body.protocol_manifest !== undefined && (!body.protocol_manifest || typeof body.protocol_manifest !== "object" || Array.isArray(body.protocol_manifest))) throw new RequestError("protocol_manifest must be an object");
        const run = await benchmarkRuntime.start({
          provider,
          model,
          ...(body.limit !== undefined ? { limit: body.limit as number } : {}),
          ...(body.case_ids !== undefined ? { caseIds: body.case_ids as string[] } : {}),
          confirmModelCalls: body.confirm_model_calls as number,
          ...(body.protocol_manifest !== undefined ? { protocolManifest: body.protocol_manifest as Record<string, unknown> } : {}),
        });
        sendJson(response, 202, run);
        return;
      }
      if (url.pathname === "/v1/benchmarks" && request.method === "GET") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        sendJson(response, 200, { runs: benchmarkRuntime.history(Number(url.searchParams.get("limit") ?? 20)) });
        return;
      }
      if (url.pathname === "/v1/benchmarks/latest" && request.method === "GET") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        sendJson(response, 200, { run: benchmarkRuntime.latest() ?? null });
        return;
      }
      const benchmarkMatch = url.pathname.match(new RegExp("^/v1/benchmarks/(pbr_[A-Za-z0-9_-]+)$"));
      if (benchmarkMatch?.[1] && request.method === "GET") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        const run = benchmarkRuntime.get(benchmarkMatch[1]);
        if (!run) { sendJson(response, 404, { error: "Benchmark run not found" }); return; }
        sendJson(response, 200, run);
        return;
      }
      const benchmarkLogsMatch = url.pathname.match(new RegExp("^/v1/benchmarks/(pbr_[A-Za-z0-9_-]+)/logs$"));
      if (benchmarkLogsMatch?.[1] && request.method === "GET") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        const arm = url.searchParams.get("arm");
        const stage = url.searchParams.get("stage");
        const caseId = url.searchParams.get("case_id");
        const search = url.searchParams.get("search");
        sendJson(response, 200, benchmarkRuntime.logs(benchmarkLogsMatch[1], {
          ...(arm ? { arm } : {}),
          ...(stage ? { stage } : {}),
          ...(caseId ? { caseId } : {}),
          ...(search ? { search } : {}),
          limit: Number(url.searchParams.get("limit") ?? 100),
          offset: Number(url.searchParams.get("offset") ?? 0),
        }));
        return;
      }
      const benchmarkControlMatch = url.pathname.match(new RegExp("^/v1/benchmarks/(pbr_[A-Za-z0-9_-]+)/(pause|resume|stop)$"));
      if (benchmarkControlMatch?.[1] && benchmarkControlMatch[2] && request.method === "POST") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        const run = benchmarkControlMatch[2] === "pause"
          ? benchmarkRuntime.pause(benchmarkControlMatch[1])
          : benchmarkControlMatch[2] === "resume"
            ? benchmarkRuntime.resume(benchmarkControlMatch[1])
            : benchmarkRuntime.stop(benchmarkControlMatch[1]);
        sendJson(response, 200, run);
        return;
      }

      if (request.method === "POST" && url.pathname === "/v1/channel-events") {
        if (!control) throw new ControlAuthenticationError("Missing control token", 401);
        let event: ReturnType<typeof parseChannelEvent>;
        try {
          event = parseChannelEvent(await readJson(request));
        } catch (error) {
          throw new RequestError(
            error instanceof Error ? error.message : "Invalid ChannelEvent",
          );
        }
        const channelAction = event.event_type === "action" && event.payload.action === "approve_query" ? "query.approve" : "task.write";
        assertControl(control, channelAction, event.task_run_id ?? undefined);
        identityFromTrustedBinding(control.principal, event.channel, event.external_user_id);
        if (event.task_run_id && channelAction !== "query.approve") assertTaskAccess(application, control.principal, event.task_run_id);
        if (channelAction === "query.approve" && (!event.task_run_id || control.query_run_id !== event.payload.query_run_id ||
            control.idempotency_key !== request.headers["idempotency-key"] ||
            control.approval_hashes?.sql_hash !== event.payload.sql_hash ||
            control.approval_hashes?.assurance_report_hash !== event.payload.assurance_report_hash ||
            control.approval_hashes?.enforcement_context_hash !== event.payload.enforcement_context_hash)) {
          throw new ControlAuthenticationError("Approval scope mismatch");
        }
        const operation = application.ingestChannelEvent(event, control.principal, request.headers["x-forge-control-token"] as string);
        const taskRunId = application.getTaskRunIdForChannelEvent(event.channel, event.event_id, control.principal);
        if (channelAction === "query.approve") {
          const result = await operation;
          sendJson(response, result.duplicate ? 200 : 202, result);
          return;
        }
        if (taskRunId === null) return await operation.then(
          (result) => sendJson(response, 200, result),
        );
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId,
          respondAsync: true,
          operation,
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const skillPolicyMatch = url.pathname.match(
        /^\/v1\/orgs\/([^/]+)\/teams\/([^/]+)\/skill-policy$/,
      );
      if (skillPolicyMatch?.[1] !== undefined && skillPolicyMatch[2] !== undefined) {
        if (control!.principal.principal_type !== "human" || control!.principal.role !== "admin") throw new ControlAuthenticationError("Admin required");
        const orgId = requireScopeId(skillPolicyMatch[1], "org_id");
        const teamId = requireScopeId(skillPolicyMatch[2], "team_id");
        assertScope(control!.principal, orgId, teamId);
        if (request.method === "GET") {
          sendJson(response, 200, {
            policy: application.getTeamSkillPolicy(orgId, teamId),
            defaults: AUTHORIZED_SKILL_NAMES,
          });
          return;
        }
        if (request.method === "PUT") {
          const body = await readJson(request);
          if (!Array.isArray(body.enabled_skills) || body.enabled_skills.some(
            (name) => typeof name !== "string" || !AUTHORIZED_SKILLS.has(name),
          )) {
            throw new RequestError("enabled_skills must contain only authorized Skill names");
          }
          if (!Number.isInteger(body.expected_version) || Number(body.expected_version) < 0) {
            throw new RequestError("expected_version must be a non-negative integer");
          }
          const policy = application.configureTeamSkills({
            orgId,
            teamId,
            enabledSkills: body.enabled_skills as AuthorizedSkillName[],
            expectedVersion: Number(body.expected_version),
            actor: control!.principal.principal_id,
          });
          sendJson(response, 200, { policy });
          return;
        }
      }

      if (request.method === "GET" && url.pathname === "/v1/tasks") {
        const orgId = requireScopeId(url.searchParams.get("org_id") ?? "", "org_id");
        const teamId = requireScopeId(url.searchParams.get("team_id") ?? "", "team_id");
        const userValue = url.searchParams.get("user_id");
        const userId = userValue === null ? undefined : requireScopeId(userValue, "user_id");
        assertScope(control!.principal, orgId, teamId, userId);
        const channelValue = url.searchParams.get("channel");
        if (channelValue !== null && !CHANNELS.has(channelValue as TaskChannel)) {
          throw new RequestError(`unsupported channel: ${channelValue}`);
        }
        const statusValue = url.searchParams.get("status");
        if (statusValue !== null && !TASK_STATUS_SET.has(statusValue)) {
          throw new RequestError(`unsupported status: ${statusValue}`);
        }
        const limitValue = Number(url.searchParams.get("limit") ?? "50");
        if (!Number.isInteger(limitValue) || limitValue < 1 || limitValue > 100) {
          throw new RequestError("limit must be an integer from 1 to 100");
        }
        const tasks = application.listTasks({
          orgId,
          teamId,
          limit: limitValue,
          ...(control!.principal.principal_type === "human" && control!.principal.role === "admin"
            ? (userId === undefined ? {} : { userId }) : { userId: control!.principal.principal_id }),
          ...(channelValue === null ? {} : { channel: channelValue as TaskChannel }),
          ...(statusValue === null ? {} : { status: statusValue as TaskStatus }),
        });
        sendJson(response, 200, { tasks });
        return;
      }

      if (request.method === "GET" && url.pathname === "/v1/conversations") {
        response.setHeader("cache-control", "no-store");
        requireChannelAuthentication(request, config.channelServiceKeys);
        const orgId = requireScopeId(url.searchParams.get("org_id") ?? "", "org_id");
        const teamId = requireScopeId(url.searchParams.get("team_id") ?? "", "team_id");
        const userId = requireScopeId(url.searchParams.get("user_id") ?? "", "user_id");
        assertScope(control!.principal, orgId, teamId, userId);
        const channelValue = url.searchParams.get("channel") ?? "";
        if (!CONVERSATION_CHANNELS.has(channelValue as Exclude<TaskChannel, "api">)) {
          throw new RequestError(`unsupported conversation channel: ${channelValue}`);
        }
        const limit = Number(url.searchParams.get("limit") ?? "20");
        if (!Number.isInteger(limit) || limit < 1 || limit > 50) {
          throw new RequestError("limit must be an integer from 1 to 50");
        }
        const cursor = url.searchParams.get("cursor");
        if (cursor !== null && (cursor.length === 0 || cursor.length > 512)) {
          throw new RequestError("cursor is invalid");
        }
        const projection = application.listConversationProjections({
          orgId,
          teamId,
          userId,
          channel: channelValue as Exclude<TaskChannel, "api">,
          limit,
          ...(cursor === null ? {} : { cursor }),
        });
        sendJson(response, 200, projection);
        return;
      }

      const conversationMatch = url.pathname.match(/^\/v1\/conversations\/([^/]+)$/);
      if (request.method === "GET" && conversationMatch?.[1] !== undefined) {
        response.setHeader("cache-control", "no-store");
        requireChannelAuthentication(request, config.channelServiceKeys);
        const orgId = requireScopeId(url.searchParams.get("org_id") ?? "", "org_id");
        const teamId = requireScopeId(url.searchParams.get("team_id") ?? "", "team_id");
        const userId = requireScopeId(url.searchParams.get("user_id") ?? "", "user_id");
        assertScope(control!.principal, orgId, teamId, userId);
        const channelValue = url.searchParams.get("channel") ?? "";
        if (!CONVERSATION_CHANNELS.has(channelValue as Exclude<TaskChannel, "api">)) {
          throw new RequestError(`unsupported conversation channel: ${channelValue}`);
        }
        const cursor = url.searchParams.get("cursor");
        if (cursor !== null && (cursor.length === 0 || cursor.length > 512)) {
          throw new RequestError("cursor is invalid");
        }
        const conversation = application.getConversationProjection({
          orgId,
          teamId,
          userId,
          channel: channelValue as Exclude<TaskChannel, "api">,
          conversationId: requireProductIdentifier(conversationMatch[1], "conversation_id"),
          ...(cursor === null ? {} : { cursor }),
        });
        if (conversation === undefined) {
          sendJson(response, 404, { status: "not_found" });
        } else {
          sendJson(response, 200, { conversation });
        }
        return;
      }

      if (request.method === "POST" && url.pathname === "/v1/tasks") {
        const body = await readJson(request);
        const channel = requireString(body, "channel");
        if (!CHANNELS.has(channel as TaskChannel)) {
          throw new RequestError(`unsupported channel: ${channel}`);
        }
        for (const [field, actual] of [["org_id", control!.principal.organization_id], ["team_id", control!.principal.workspace_id], ["user_id", control!.principal.principal_id]] as const) {
          if (body[field] !== undefined && body[field] !== actual) throw new ControlAuthenticationError("Task identity mismatch");
        }
        const result = application.createTask({
          org_id: control!.principal.organization_id,
          team_id: control!.principal.workspace_id,
          user_id: control!.principal.principal_id,
          channel: channel as TaskChannel,
          intent: requireString(body, "intent"),
          message: requireString(body, "message"),
          ...(typeof body.channel_conversation_id === "string"
            ? { channel_conversation_id: body.channel_conversation_id }
            : {}),
          ...(typeof body.correlation_id === "string"
            ? { correlation_id: body.correlation_id }
            : {}),
        }, control!.principal);
        sendJson(response, 201, result);
        return;
      }

      const taskMatch = url.pathname.match(/^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)$/);
      if (request.method === "GET" && taskMatch?.[1] !== undefined) {
        const task = application.getTask(taskMatch[1]);
        if (task === undefined) {
          sendJson(response, 404, { status: "not_found" });
        } else {
          sendJson(response, 200, { task });
        }
        return;
      }
      const queryReviewMatch = url.pathname.match(/^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/query-review$/);
      if (request.method === "GET" && queryReviewMatch?.[1]) {
        if (control!.principal.principal_type !== "human" || !["reviewer", "admin"].includes(control!.principal.role)) throw new ControlAuthenticationError("Reviewer required");
        const projection = await application.getQueryReviewProjection(queryReviewMatch[1], control!.principal);
        if (!projection) { sendJson(response, 404, { status: "not_found" }); return; }
        sendJson(response, 200, { review: projection });
        return;
      }

      const taskDetailMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/detail$/,
      );
      if (request.method === "GET" && taskDetailMatch?.[1] !== undefined) {
        response.setHeader("cache-control", "no-store");
        requireChannelAuthentication(request, config.channelServiceKeys);
        const orgId = requireScopeId(url.searchParams.get("org_id") ?? "", "org_id");
        const teamId = requireScopeId(url.searchParams.get("team_id") ?? "", "team_id");
        const userId = requireScopeId(url.searchParams.get("user_id") ?? "", "user_id");
        assertScope(control!.principal, orgId, teamId, userId);
        const channelValue = url.searchParams.get("channel") ?? "";
        if (!CHANNELS.has(channelValue as TaskChannel)) {
          throw new RequestError(`unsupported channel: ${channelValue}`);
        }
        const detail = application.getTaskDetailProjection({
          orgId,
          teamId,
          userId,
          channel: channelValue as TaskChannel,
          taskRunId: taskDetailMatch[1],
        });
        if (detail === undefined) {
          sendJson(response, 404, { status: "not_found" });
        } else {
          sendJson(response, 200, { detail });
        }
        return;
      }

      const presentationMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/presentation$/,
      );
      if (request.method === "GET" && presentationMatch?.[1] !== undefined) {
        requireChannelAuthentication(request, config.channelServiceKeys);
        sendJson(response, 200, {
          presentation: application.getChannelPresentation(presentationMatch[1], control!.principal),
        });
        return;
      }

      const eventsMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/events$/,
      );
      if (request.method === "GET" && eventsMatch?.[1] !== undefined) {
        if (application.getTask(eventsMatch[1]) === undefined) {
          sendJson(response, 404, { status: "not_found" });
          return;
        }
        const after = Number(url.searchParams.get("after") ?? "0");
        sendJson(response, 200, {
          events: application.getEvents(
            eventsMatch[1],
            Number.isInteger(after) && after >= 0 ? after : 0,
          ),
        });
        return;
      }

      const artifactsMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/artifacts$/,
      );
      if (request.method === "GET" && artifactsMatch?.[1] !== undefined) {
        if (application.getTask(artifactsMatch[1]) === undefined) {
          sendJson(response, 404, { status: "not_found" });
          return;
        }
        sendJson(response, 200, {
          artifacts: application.getArtifacts(artifactsMatch[1]),
        });
        return;
      }

      const attemptsMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/attempts$/,
      );
      if (request.method === "GET" && attemptsMatch?.[1] !== undefined) {
        if (application.getTask(attemptsMatch[1]) === undefined) {
          sendJson(response, 404, { status: "not_found" });
          return;
        }
        sendJson(response, 200, {
          attempts: application.getStageAttempts(attemptsMatch[1]),
        });
        return;
      }

      const clarifyMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/clarify$/,
      );
      if (request.method === "POST" && clarifyMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: clarifyMatch[1],
          respondAsync: body.async === true,
          operation: application.clarifyRequirement(clarifyMatch[1], {
            message: requireString(body, "message"),
            ...(typeof body.idempotency_key === "string"
              ? { idempotencyKey: body.idempotency_key }
              : {}),
          }),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const metricMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/review-metric$/,
      );
      if (request.method === "POST" && metricMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: metricMatch[1],
          respondAsync: body.async === true,
          operation: application.reviewMetricDefinition(metricMatch[1], {
            message: requireString(body, "message"),
            ...(typeof body.idempotency_key === "string"
              ? { idempotencyKey: body.idempotency_key }
              : {}),
          }),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const supplementMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/supplements$/,
      );
      if (request.method === "POST" && supplementMatch?.[1] !== undefined) {
        const body = await readJson(request);
        if (
          typeof body.suggested_query_index !== "number" ||
          !Number.isInteger(body.suggested_query_index)
        ) {
          throw new RequestError("suggested_query_index must be an integer");
        }
        const result = application.createSupplementTask(supplementMatch[1], {
          suggestedQueryIndex: body.suggested_query_index,
          idempotencyKey: requireString(body, "idempotency_key"),
        });
        sendJson(response, 201, result);
        return;
      }

      const resumeAnalysisMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/resume-analysis$/,
      );
      if (request.method === "POST" && resumeAnalysisMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: resumeAnalysisMatch[1],
          respondAsync: body.async === true,
          operation: application.resumeAnalysisWithSupplement(
            resumeAnalysisMatch[1],
            {
              childTaskRunId: requireString(body, "child_task_run_id"),
              idempotencyKey: requireString(body, "idempotency_key"),
            },
          ),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const advisoryMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/run-skill$/,
      );
      if (request.method === "POST" && advisoryMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const skillName = requireString(body, "skill_name");
        if (!ADVISORY_SKILLS.has(skillName)) {
          throw new RequestError("skill_name is not an authorized Advisory Skill");
        }
        const operation = application.runAdvisory(advisoryMatch[1], {
          skillName: skillName as AdvisorySkillName,
          prompt: requireString(body, "prompt"),
          idempotencyKey: requireString(body, "idempotency_key"),
        });
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: advisoryMatch[1],
          respondAsync: true,
          operation,
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const analyzeMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/analyze$/,
      );
      if (request.method === "POST" && analyzeMatch?.[1] !== undefined) {
        const body = await readJson(request);
        if (body.question !== undefined && typeof body.question !== "string") {
          throw new RequestError("question must be a string");
        }
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: analyzeMatch[1],
          respondAsync: body.async === true,
          operation: application.analyzeTask(analyzeMatch[1], {
            ...(typeof body.question === "string" ? { question: body.question } : {}),
            ...(typeof body.idempotency_key === "string"
              ? { idempotencyKey: body.idempotency_key }
              : {}),
          }),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      const reportMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/render-report$/,
      );
      if (request.method === "POST" && reportMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: reportMatch[1],
          respondAsync: body.async === true,
          operation: application.renderReport(reportMatch[1], {
            audience: requireString(body, "audience"),
            ...(typeof body.idempotency_key === "string"
              ? { idempotencyKey: body.idempotency_key }
              : {}),
          }),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }
      const mandateMatch = url.pathname.match(/^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/mandate$/);
      if (request.method === "POST" && mandateMatch?.[1]) {
        if (control!.principal.principal_type !== "human" || control!.principal.role !== "admin") throw new ControlAuthenticationError("Admin required");
        if (typeof request.headers["idempotency-key"] !== "string" || !request.headers["idempotency-key"]) throw new RequestError("Idempotency-Key is required");
        const body = await readJson(request);
        if (Object.keys(body).some(key => key !== "mandate_id" && key !== "expected_mandate_id") ||
            (body.expected_mandate_id !== null && typeof body.expected_mandate_id !== "string")) throw new RequestError("Invalid mandate attachment");
        const result = await application.attachMandate(mandateMatch[1], {
          mandateId: requireString(body, "mandate_id"), expectedMandateId: body.expected_mandate_id,
        }, control!.principal);
        sendJson(response, 200, result);
        return;
      }

      const approveMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/approve-query$/,
      );
      if (request.method === "POST" && approveMatch?.[1] !== undefined) {
        const task = application.getTask(approveMatch[1]);
        if (!task || task.org_id !== control!.principal.organization_id || task.team_id !== control!.principal.workspace_id) throw new ControlAuthenticationError("Task not found", 404);
        if (control!.principal.principal_type !== "human" || (control!.principal.role !== "reviewer" && control!.principal.role !== "admin")) throw new ControlAuthenticationError("Reviewer required");
        const body = await readJson(request);
        if (Object.keys(body).some(key => !["query_run_id", "sql_hash", "assurance_report_hash", "enforcement_context_hash", "idempotency_key"].includes(key)))
          throw new RequestError("Unsupported approval field");
        const queryRunId = requireString(body, "query_run_id");
        const sqlHash = requireString(body, "sql_hash");
        const assuranceReportHash = requireString(body, "assurance_report_hash");
        const enforcementContextHash = requireString(body, "enforcement_context_hash");
        const idempotencyKey = request.headers["idempotency-key"];
        if (typeof idempotencyKey !== "string" || !idempotencyKey ||
            (body.idempotency_key !== undefined && body.idempotency_key !== idempotencyKey) ||
            control!.query_run_id !== queryRunId || control!.idempotency_key !== idempotencyKey ||
            control!.approval_hashes?.sql_hash !== sqlHash ||
            control!.approval_hashes?.assurance_report_hash !== assuranceReportHash ||
            control!.approval_hashes?.enforcement_context_hash !== enforcementContextHash) throw new ControlAuthenticationError("Approval scope mismatch");
        const result = await application.approveQuery(approveMatch[1], {
          queryRunId, sqlHash, assuranceReportHash, enforcementContextHash, idempotencyKey,
          controlToken: request.headers["x-forge-control-token"] as string,
        }, control!.principal);
        sendJson(response, result.result.status === "completed" ? 200 : 202, result);
        return;
      }

      const prepareMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/prepare-query$/,
      );
      if (request.method === "POST" && prepareMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const dialectValue = body.dialect;
        if (
          dialectValue !== undefined &&
          (typeof dialectValue !== "string" || !DIALECTS.has(dialectValue))
        ) {
          throw new RequestError(`unsupported dialect: ${String(dialectValue)}`);
        }
        const input: {
          question: string;
          dialect?: ForgeDialect;
          candidate?: QueryCandidateInput;
          idempotencyKey?: string;
        } = {
          question: requireString(body, "question"),
        };
        if (typeof dialectValue === "string") input.dialect = dialectValue as ForgeDialect;
        const candidate = parseQueryCandidate(body.candidate);
        if (candidate !== undefined) input.candidate = candidate;
        if (typeof body.idempotency_key === "string") {
          input.idempotencyKey = body.idempotency_key;
        }
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: prepareMatch[1],
          respondAsync: body.async === true,
          operation: application.prepareQuery(prepareMatch[1], input),
        });
        if (result !== undefined) sendJson(response, 200, result);
        return;
      }

      sendJson(response, 404, { status: "not_found" });
    } catch (error) {
      if (error instanceof RequestError || error instanceof BenchmarkInputError) {
        sendJson(response, 400, { status: "invalid_request", error: error.message });
      } else if (error instanceof ControlAuthenticationError) {
        sendJson(response, error.statusCode, { status: error.statusCode === 404 ? "not_found" : "forbidden", error: error.message });
      } else if (error instanceof ChannelAuthenticationError) {
        sendJson(response, 401, { status: "unauthorized", error: error.message });
      } else if (error instanceof ChannelIdentityError) {
        sendJson(response, 403, { status: "forbidden", error: error.message });
      } else if (error instanceof SkillPolicyConflictError) {
        sendJson(response, 409, { status: "conflict", error: error.message });
      } else if (error instanceof TaskStateError) {
        const statusCode = error.message.includes("not found") ? 404 : 409;
        sendJson(response, statusCode, { status: "task_error", error: error.message });
      } else if (error instanceof ForgeClientError) {
        const statusCode = [400, 403, 404, 409].includes(error.statusCode ?? 0) ? error.statusCode! : 502;
        sendJson(response, statusCode, { status: "upstream_error", code: error.code, error: "Forge request failed" });
      } else {
        sendJson(response, 500, { status: "error", error: "internal orchestrator error" });
      }
    }
  }));
  const closeHttp = server.close.bind(server);
  let stateClosed = false;
  server.close = (callback) => {
    if (reconciliationTimer !== undefined) clearInterval(reconciliationTimer);
    const drained = benchmarkRuntime.close();
    closeHttp((error) => {
      void drained.then(() => {
        if (!stateClosed) { state?.close(); stateClosed = true; }
        callback?.(error);
      }, (cause: unknown) => {
        if (!stateClosed) { state?.close(); stateClosed = true; }
        const failure = cause instanceof Error ? cause : new Error("Benchmark shutdown failed");
        if (callback) callback(failure); else server.emit("error", failure);
      });
    });
    return server;
  };
  return server;
}

const isMain =
  process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href;

if (isMain) {
  const config = loadConfig();
  const server = createOrchestratorServer(config);
  server.listen(config.port, config.host, () => {
    console.log(`forge-pi-orchestrator listening on http://${config.host}:${config.port}`);
  });
}
