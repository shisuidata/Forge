"""
Forge 认证鉴权模块。

Web UI  — cookie-based session（HMAC-SHA256 签名，有效期 7 天）
API     — X-API-Key header 或 api_key query param

auth disabled（默认）时所有 Depends 直接放行，不影响现有行为。
"""
from __future__ import annotations

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
    if time.time() - int(ts) > _SESSION_TTL:
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


async def require_api_auth(request: Request):
    """
    FastAPI dependency for /api/* routes.

    - auth disabled → 直接放行
    - auth enabled  → 验证 X-API-Key header / ?api_key= query param / Web session cookie
      Web UI 用 cookie 登录后调用 /api/* 时，cookie 也视为有效凭证。
    """
    if not cfg.AUTH_ENABLED:
        return
    if verify_api_key(request):
        return
    if verify_web_request(request):   # Web UI 用户持有有效 session cookie
        return
    from fastapi import HTTPException
    raise HTTPException(status_code=401, detail="Unauthorized: invalid or missing API key")


def verify_api_key(request: Request) -> bool:
    """检查 X-API-Key header 或 api_key query param，返回 bool。"""
    if not cfg.AUTH_API_KEYS:
        # 没配置 API key 列表时，auth enabled 但 api_keys 为空 → 拒绝所有
        return False
    key = (
        request.headers.get("X-API-Key")
        or request.query_params.get("api_key")
        or ""
    )
    return key in cfg.AUTH_API_KEYS


# ── 内部异常（用于重定向）────────────────────────────────────────────────────

class _LoginRedirect(Exception):
    def __init__(self, next_path: str = "/chat"):
        self.next_path = next_path
