"""Safe HTTP errors and request correlation without request/response body logging."""
from __future__ import annotations

import time
from uuid import uuid4

from starlette.responses import JSONResponse

from diagnostics import current_request_id, record, request_id, safe_id


def error_body(status_code: int, code: str | None = None) -> dict:
    default = {
        400: "invalid_request", 401: "unauthorized", 403: "forbidden", 404: "not_found",
        409: "conflict", 422: "invalid_request", 429: "rate_limited",
        502: "upstream_unavailable", 503: "upstream_unavailable", 504: "upstream_timeout",
    }.get(status_code, "internal_error")
    code = safe_id(code) or default
    message = {
        400: "请求参数无效", 401: "请重新登录", 403: "当前操作未获授权", 404: "资源不存在或不可见",
        409: "当前状态不允许此操作，请刷新后重试", 422: "请求参数无效", 429: "请求过于频繁，请稍后重试",
        502: "上游服务暂时不可用，操作结果可能未知", 503: "服务暂时不可用",
        504: "上游响应超时，操作结果可能未知",
    }.get(status_code, "服务暂时无法完成请求")
    return {"status": default, "code": code, "error": message, "request_id": current_request_id()}


def error_response(status_code: int, code: str | None = None) -> JSONResponse:
    return JSONResponse(error_body(status_code, code), status_code=status_code,
                        headers={"Cache-Control": "no-store", "X-Request-ID": current_request_id()})


def upstream_error(status_code: int, body: dict) -> tuple[int, dict]:
    # Resource scope must remain indistinguishable from absence.
    status = 404 if status_code in {403, 404} else status_code if status_code in {400, 409, 422, 429, 503, 504} else 502
    code = "not_found" if status == 404 else body.get("code")
    return status, error_body(status, code)


class RequestCorrelationMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers", []))
        incoming = headers.get(b"x-request-id", b"").decode("ascii", errors="ignore")
        correlation = safe_id(incoming) or f"req_{uuid4().hex}"
        token = request_id.set(correlation)
        scope.setdefault("state", {})["request_id"] = correlation
        started = time.monotonic()
        status = 500

        async def correlated_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message = {**message, "headers": [(key, value) for key, value in message.get("headers", [])
                                                   if key.lower() != b"x-request-id"] + [(b"x-request-id", correlation.encode())]}
            await send(message)

        try:
            await self.app(scope, receive, correlated_send)
        finally:
            record("http_request", status_code=status, duration_ms=round((time.monotonic() - started) * 1000),
                   **{key: value for key, value in scope.get("path_params", {}).items()
                      if key in {"task_run_id", "query_run_id", "attempt_id"}})
            request_id.reset(token)
