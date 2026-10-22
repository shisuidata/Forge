"""Registered credentials, revocable human sessions, and same-origin CSRF protection."""
from __future__ import annotations

import hashlib
import hmac
import sqlite3
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, Response

from config import cfg
from forge.governance import AuthenticatedPrincipal, GovernanceError
from forge.governance_credentials import authenticate_credential

_COOKIE_NAME = "forge_session"


def set_governance_session_cookie(response: Response, token: str, *, max_age: int = 3600) -> None:
    response.set_cookie(
        key=_COOKIE_NAME, value=token, max_age=max_age, httponly=True, samesite="lax",
        secure=bool(cfg.AUTH_COOKIE_SECURE),
    )


def clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=_COOKIE_NAME)


def csrf_token_for_session(token: str) -> str:
    return hmac.new(token.encode("utf-8"), b"forge-csrf-v1", hashlib.sha256).hexdigest()


async def _check_cookie_mutation(request: Request, token: str) -> None:
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    origin = request.headers.get("origin", "")
    parsed = urlsplit(origin)
    if (parsed.scheme, parsed.netloc) != (request.url.scheme, request.url.netloc) or not parsed.netloc:
        raise HTTPException(status_code=403, detail="csrf_origin_invalid")
    if request.headers.get("content-type", "").split(";", 1)[0].lower() in {
        "application/x-www-form-urlencoded", "multipart/form-data",
    }:
        submitted = str((await request.form()).get("csrf_token", ""))
    else:
        submitted = request.headers.get("X-CSRF-Token", "")
    if not hmac.compare_digest(submitted, csrf_token_for_session(token)):
        raise HTTPException(status_code=403, detail="csrf_token_invalid")


async def require_runtime_principal(request: Request) -> AuthenticatedPrincipal:
    """A presented invalid key never falls back to an otherwise valid cookie."""
    if not cfg.AUTH_ENABLED:
        raise HTTPException(status_code=401, detail="authentication_required")
    key_present = "X-API-Key" in request.headers
    token = request.headers.get("X-API-Key", "") if key_present else request.cookies.get(_COOKIE_NAME, "")
    try:
        principal = authenticate_credential(token, kind=None if key_present else "session")
    except GovernanceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.code) from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
    if principal is None or (key_present and principal.authentication_method == "session"):
        raise HTTPException(status_code=401, detail="invalid_credential")
    if not key_present:
        await _check_cookie_mutation(request, token)
    return principal


async def require_human_principal(request: Request) -> AuthenticatedPrincipal:
    principal = await require_runtime_principal(request)
    if principal.principal_type != "human":
        raise HTTPException(status_code=403, detail="human_principal_required")
    return principal


async def require_admin_principal(request: Request) -> AuthenticatedPrincipal:
    principal = await require_human_principal(request)
    if principal.role != "admin":
        raise HTTPException(status_code=403, detail="admin_principal_required")
    return principal


async def require_pi_service_auth(request: Request) -> AuthenticatedPrincipal:
    """Only a registered Pi service credential can resolve internal control tokens."""
    if not cfg.AUTH_ENABLED:
        raise HTTPException(status_code=401, detail="authentication_required")
    token = request.headers.get("X-API-Key", "")
    try:
        principal = authenticate_credential(token, kind="pi_service") if token else None
    except GovernanceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.code) from exc
    except (OSError, sqlite3.Error) as exc:
        raise HTTPException(status_code=503, detail="governance_unavailable") from exc
    if principal is None or principal.principal_type != "service" or principal.principal_id != cfg.PI_EXECUTION_PRINCIPAL_ID:
        raise HTTPException(status_code=401, detail="invalid_pi_service_credential")
    return principal
