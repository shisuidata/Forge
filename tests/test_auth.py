from __future__ import annotations

from fastapi import Request

from web import auth


def test_verify_session_rejects_non_numeric_timestamp(monkeypatch):
    monkeypatch.setattr(auth.cfg, "AUTH_ADMIN_PASSWORD", "secret")
    bad_value = "admin:not-a-timestamp:signature"

    assert auth._verify_session_value(bad_value) is None


def test_verify_api_key_uses_configured_keys(monkeypatch):
    monkeypatch.setattr(auth.cfg, "AUTH_API_KEYS", ["alpha", "beta"])

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/test",
        "headers": [(b"x-api-key", b"beta")],
        "query_string": b"",
    }
    request = Request(scope)

    assert auth.verify_api_key(request) is True


def test_session_cookie_can_be_marked_secure(monkeypatch):
    from starlette.responses import Response

    monkeypatch.setattr(auth.cfg, "AUTH_ADMIN_PASSWORD", "secret")
    monkeypatch.setattr(auth.cfg, "AUTH_COOKIE_SECURE", True)

    response = Response()
    auth.set_session_cookie(response, "admin")

    assert "secure" in response.headers["set-cookie"].lower()
