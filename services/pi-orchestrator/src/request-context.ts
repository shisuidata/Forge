import { AsyncLocalStorage } from "node:async_hooks";
import { randomUUID } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";

const context = new AsyncLocalStorage<{ requestId: string }>();
const SAFE_REQUEST_ID = /^[A-Za-z0-9_.:-]{1,128}$/;

export function currentRequestId(): string | undefined {
  return context.getStore()?.requestId;
}

export function correlatedHandler(
  handler: (request: IncomingMessage, response: ServerResponse) => Promise<void>,
): (request: IncomingMessage, response: ServerResponse) => void {
  return (request, response) => {
    const supplied = request.headers["x-request-id"];
    const requestId = typeof supplied === "string" && SAFE_REQUEST_ID.test(supplied) ? supplied : randomUUID();
    response.setHeader("x-request-id", requestId);
    const started = performance.now();
    response.once("finish", () => {
      // Identifiers/status only: never log URLs, credentials, SQL, request bodies or result rows.
      console.info(JSON.stringify({ event: "http.request", request_id: requestId,
        method: request.method, status: response.statusCode, duration_ms: Math.round(performance.now() - started) }));
    });
    context.run({ requestId }, () => { void handler(request, response); });
  };
}
