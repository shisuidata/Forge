import { timingSafeEqual } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { pathToFileURL } from "node:url";

import { OrchestratorApplication } from "./application.js";
import { parseChannelEvent, type ChannelIdentity } from "./channels/contracts.js";
import { ChannelIdentityError, ChannelIdentityResolver } from "./channels/identity.js";
import { loadConfig, type OrchestratorConfig } from "./config.js";
import { FORGE_DIALECTS, type ForgeDialect } from "./forge/client.js";
import { inspectRuntime } from "./runtime.js";
import {
  ADVISORY_SKILL_NAMES,
  AUTHORIZED_SKILL_NAMES,
  type AdvisorySkillName,
  type AuthorizedSkillName,
} from "./skills.js";
import { SkillPolicyConflictError } from "./skill-policy.js";
import { SqliteOrchestratorState } from "./sqlite-store.js";
import { TaskStateError, type TaskChannel } from "./task-store.js";

const MAX_BODY_BYTES = 64 * 1024;
const CHANNELS = new Set<TaskChannel>(["web", "feishu", "dingtalk", "api"]);
const DIALECTS = new Set<string>(FORGE_DIALECTS);
const AUTHORIZED_SKILLS = new Set<string>(AUTHORIZED_SKILL_NAMES);
const ADVISORY_SKILLS = new Set<string>(ADVISORY_SKILL_NAMES);

class RequestError extends Error {}
class ChannelAuthenticationError extends Error {}
class AdminAuthenticationError extends Error {}

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

function requireAdminAuthentication(
  request: IncomingMessage,
  configuredKeys: string[],
): void {
  const key = request.headers["x-admin-service-key"];
  if (
    configuredKeys.length === 0 ||
    typeof key !== "string" ||
    !configuredKeys.some((candidate) => secureEquals(candidate, key))
  ) {
    throw new AdminAuthenticationError("Invalid admin service credential");
  }
}

function sendJson(response: ServerResponse, statusCode: number, body: unknown): void {
  response.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  response.end(JSON.stringify(body));
}

async function readJson(request: IncomingMessage): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.from(chunk);
    size += buffer.length;
    if (size > MAX_BODY_BYTES) throw new RequestError("request body is too large");
    chunks.push(buffer);
  }
  try {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
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
  const channelIdentities = new ChannelIdentityResolver(config.channelIdentityMapPath);
  if (application === undefined) {
    state = new SqliteOrchestratorState(config.stateDbPath);
    state.reconcileExpiredAttempts();
    reconciliationTimer = setInterval(
      () => state?.reconcileExpiredAttempts(),
      config.reconciliationIntervalMs,
    );
    reconciliationTimer.unref();
    application = new OrchestratorApplication({
      config,
      tasks: state.tasks,
      events: state.events,
      artifacts: state.artifacts,
      attempts: state.attempts,
      channelEvents: state.channelEvents,
      skillPolicies: state.skillPolicies,
      transactions: state.transactions,
    });
  }
  const server = createServer(async (request, response) => {
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
              channelIngressConfigured:
                config.channelServiceKeys.length > 0 &&
                (channelIdentities.size > 0 || config.channelAutoBindFirstFeishu),
              authorizedChannelIdentities: channelIdentities.size,
              feishuAutoBindingPending:
                config.channelAutoBindFirstFeishu && channelIdentities.feishuIdentityCount === 0,
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

      if (request.method === "POST" && url.pathname === "/v1/channel-events") {
        requireChannelAuthentication(request, config.channelServiceKeys);
        let event: ReturnType<typeof parseChannelEvent>;
        try {
          event = parseChannelEvent(await readJson(request));
        } catch (error) {
          throw new RequestError(
            error instanceof Error ? error.message : "Invalid ChannelEvent",
          );
        }
        let identity: ChannelIdentity;
        try {
          identity = channelIdentities.resolve(event.channel, event.external_user_id);
        } catch (error) {
          if (
            error instanceof ChannelIdentityError &&
            config.channelAutoBindFirstFeishu &&
            event.channel === "feishu" &&
            event.event_type === "message" &&
            event.payload.chat_type === "p2p"
          ) {
            identity = channelIdentities.bindFirstFeishu(
              event.external_user_id,
              config.channelBootstrapIdentity,
            );
          } else {
            throw error;
          }
        }
        const operation =
          event.event_type === "message"
            ? application.ingestChannelMessage(event, identity)
            : application.ingestChannelAction(event, identity);
        const taskRunId = application.getTaskRunIdForChannelEvent(event.channel, event.event_id);
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
        requireAdminAuthentication(request, config.adminServiceKeys);
        const orgId = requireScopeId(skillPolicyMatch[1], "org_id");
        const teamId = requireScopeId(skillPolicyMatch[2], "team_id");
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
            actor: requireString(body, "actor"),
          });
          sendJson(response, 200, { policy });
          return;
        }
      }

      if (request.method === "POST" && url.pathname === "/v1/tasks") {
        const body = await readJson(request);
        const channel = requireString(body, "channel");
        if (!CHANNELS.has(channel as TaskChannel)) {
          throw new RequestError(`unsupported channel: ${channel}`);
        }
        const result = application.createTask({
          org_id: requireString(body, "org_id"),
          team_id: requireString(body, "team_id"),
          user_id: requireString(body, "user_id"),
          channel: channel as TaskChannel,
          intent: requireString(body, "intent"),
          message: requireString(body, "message"),
          ...(typeof body.channel_conversation_id === "string"
            ? { channel_conversation_id: body.channel_conversation_id }
            : {}),
          ...(typeof body.correlation_id === "string"
            ? { correlation_id: body.correlation_id }
            : {}),
        });
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

      const presentationMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/presentation$/,
      );
      if (request.method === "GET" && presentationMatch?.[1] !== undefined) {
        requireChannelAuthentication(request, config.channelServiceKeys);
        sendJson(response, 200, {
          presentation: application.getChannelPresentation(presentationMatch[1]),
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

      const approveMatch = url.pathname.match(
        /^\/v1\/tasks\/(tr_[A-Za-z0-9_-]+)\/approve-query$/,
      );
      if (request.method === "POST" && approveMatch?.[1] !== undefined) {
        const body = await readJson(request);
        const result = await maybeRunAsync({
          response,
          application,
          taskRunId: approveMatch[1],
          respondAsync: body.async === true,
          operation: application.approveQuery(approveMatch[1], {
            queryRunId: requireString(body, "query_run_id"),
            sqlHash: requireString(body, "sql_hash"),
            idempotencyKey: requireString(body, "idempotency_key"),
          }),
        });
        if (result !== undefined) sendJson(response, 200, result);
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
          idempotencyKey?: string;
        } = {
          question: requireString(body, "question"),
        };
        if (typeof dialectValue === "string") input.dialect = dialectValue as ForgeDialect;
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
      if (error instanceof RequestError) {
        sendJson(response, 400, { status: "invalid_request", error: error.message });
      } else if (
        error instanceof ChannelAuthenticationError ||
        error instanceof AdminAuthenticationError ||
        error instanceof ChannelIdentityError
      ) {
        sendJson(response, 403, { status: "forbidden", error: error.message });
      } else if (error instanceof SkillPolicyConflictError) {
        sendJson(response, 409, { status: "conflict", error: error.message });
      } else if (error instanceof TaskStateError) {
        const statusCode = error.message.includes("not found") ? 404 : 409;
        sendJson(response, statusCode, { status: "task_error", error: error.message });
      } else {
        sendJson(response, 500, { status: "error", error: "internal orchestrator error" });
      }
    }
  });
  if (state !== undefined) {
    server.once("close", () => {
      if (reconciliationTimer !== undefined) clearInterval(reconciliationTimer);
      state?.close();
    });
  }
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
