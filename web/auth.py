"""
Forge 认证鉴权模块。

Web UI  — cookie-based session（HMAC-SHA256 签名，有效期 7 天）
API     — X-API-Key header 或 api_key query param

auth disabled（默认）时所有 Depends 直接放行，不影响现有行为。
"""
from __future__ import annotations

import hashlib
import hmac
import time
from typing import Optional

from fastapi import Depends, Request, Response
from fastapi.responses import RedirectResponse

from config import cfg

# Cookie / HMAC 常量
_COOKIE_NAME    = "forge_session"
_SESSION_TTL    = 7 * 24 * 3600   # 7 天（秒）
_HMAC_SEP       = ":"
_HMAC_ALGORITHM = "sha256"


# ── 内部工具 ──────────────────────────────────────────────────────────────────

def _sign(payload: str) -> str:
    """用 admin_password 对 payload 做 HMAC-SHA256 签名，返回 hex digest。"""
    key = (cfg.AUTH_ADMIN_PASSWORD or "forge-default-secret").encode()
    return hmac.new(key, payload.encode(), _HMAC_ALGORITHM).hexdigest()


def _make_session_value(user_id: str) -> str:
    """生成 cookie value：user_id:timestamp:signature"""
    ts      = str(int(time.time()))
    payload = f"{user_id}{_HMAC_SEP}{ts}"
    sig     = _sign(payload)
    return f"{payload}{_HMAC_SEP}{sig}"


def _verify_session_value(value: str) -> Optional[str]:
    """验证 cookie value，返回 user_id；无效或过期返回 None。"""
    try:
        user_id, ts, sig = value.split(_HMAC_SEP, 2)
    except ValueError:
        return None

    # 验签
    payload  = f"{user_id}{_HMAC_SEP}{ts}"
    expected = _sign(payload)
    if not hmac.compare_digest(expected, sig):
        return None

    # 过期检查
    try:
        issued_at = int(ts)
    except ValueError:
        return None
    if time.time() - issued_at > _SESSION_TTL:
        return None

    return user_id


# ── 公开 API ──────────────────────────────────────────────────────────────────

def verify_web_request(request: Request) -> bool:
    """检查 cookie forge_session，返回 bool。"""
    value = request.cookies.get(_COOKIE_NAME, "")
    if not value:
        return False
    return _verify_session_value(value) is not None


def set_session_cookie(response: Response, user_id: str) -> None:
    """设置 forge_session cookie（httponly, samesite=lax）。"""
    value = _make_session_value(user_id)
    response.set_cookie(
        key      = _COOKIE_NAME,
        value    = value,
        max_age  = _SESSION_TTL,
        httponly = True,
        samesite = "lax",
        secure   = bool(getattr(cfg, "AUTH_COOKIE_SECURE", False)),
    )


def clear_session_cookie(response: Response) -> None:
    """清除 forge_session cookie。"""
    response.delete_cookie(key=_COOKIE_NAME)


# ── FastAPI Dependencies ──────────────────────────────────────────────────────

async def require_web_auth(request: Request):
    """
    FastAPI dependency for Web UI routes.

    - auth disabled → 直接放行
    - auth enabled  → 验证 cookie，失败时重定向到 /login
    """
    if not cfg.AUTH_ENABLED:
        return
    if verify_web_request(request):
        return
    raise _LoginRedirect(request.url.path)


async def require_pi_service_auth(request: Request):
    """Authenticate the internal Pi control plane with a dedicated service key.

    This gate is independent from AUTH_ENABLED and normal Forge API keys. An
    empty PI_SERVICE_API_KEYS list always denies access.
    """
    key = request.headers.get("X-Pi-Service-Key") or ""
    if cfg.PI_SERVICE_API_KEYS and any(
        hmac.compare_digest(key, valid_key) for valid_key in cfg.PI_SERVICE_API_KEYS
    ):
        return "pi-orchestrator"
    from fastapi import HTTPException
    raise HTTPException(status_code=401, detail="Unauthorized: invalid Pi service key")


async def require_api_auth(request: Request) -> dict[str, str | None]:
    """Authenticate a public API request and return its credential binding."""
    if not cfg.AUTH_ENABLED:
        return {
            "method": "local",
            "assurance_level": "single_factor",
            "session_id_hash": None,
        }
    if verify_api_key(request) or verify_web_request(request):
        return api_auth_binding(request)
    from fastapi import HTTPException
    raise HTTPException(status_code=401, detail="Unauthorized: invalid or missing API key")


def _request_api_key(request: Request) -> str:
    return (
        request.headers.get("X-API-Key")
        or request.query_params.get("api_key")
        or ""
    )


def _public_api_keys() -> list[str]:
    return [*cfg.AUTH_API_KEYS, *cfg.ENFORCE_REVIEWER_API_KEYS]


def verify_api_key(request: Request) -> bool:
    """检查 X-API-Key header 或 api_key query param，返回 bool。"""
    key = _request_api_key(request)
    return bool(key) and any(
        hmac.compare_digest(key, valid_key) for valid_key in _public_api_keys()
    )


def api_auth_binding(request: Request) -> dict[str, str | None]:
    """Return a non-secret binding for the credential that authenticated the request."""
    key = _request_api_key(request)
    if key and any(hmac.compare_digest(key, valid_key) for valid_key in _public_api_keys()):
        return {
            "method": "service_key",
            "assurance_level": "service_asserted",
            "session_id_hash": "sha256:" + hashlib.sha256(key.encode("utf-8")).hexdigest(),
        }
    cookie = request.cookies.get(_COOKIE_NAME, "")
    if cookie and _verify_session_value(cookie) is not None:
        return {
            "method": "local",
            "assurance_level": "single_factor",
            "session_id_hash": "sha256:" + hashlib.sha256(cookie.encode("utf-8")).hexdigest(),
        }
    return {
        "method": "local",
        "assurance_level": "single_factor",
        "session_id_hash": None,
    }


def credential_binding_matches_principal_context(
    principal_context: object,
    auth_binding: dict[str, str | None],
) -> bool:
    """Check that a governed record belongs to the current non-secret credential binding."""
    authentication = (
        principal_context.get("authentication_context")
        if isinstance(principal_context, dict)
        else None
    )
    return isinstance(authentication, dict) and all(
        authentication.get(field) == auth_binding[field]
        for field in ("method", "assurance_level", "session_id_hash")
    )


async def require_enforce_reviewer_auth(request: Request) -> str:
    """Require an explicitly configured reviewer credential in authenticated deployments."""
    if not cfg.AUTH_ENABLED:
        return "local-reviewer"
    from fastapi import HTTPException
    if not cfg.ENFORCE_REVIEWER_API_KEYS:
        raise HTTPException(status_code=503, detail="Enforce reviewer credentials are not configured")
    key = _request_api_key(request)
    if key and any(
        hmac.compare_digest(key, valid_key)
        for valid_key in cfg.ENFORCE_REVIEWER_API_KEYS
    ):
        return "configured-reviewer"
    raise HTTPException(status_code=403, detail="Reviewer authorization required")


# ── 内部异常（用于重定向）────────────────────────────────────────────────────

class _LoginRedirect(Exception):
    def __init__(self, next_path: str = "/chat"):
        self.next_path = next_path
