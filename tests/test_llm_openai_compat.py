from __future__ import annotations

import json

import pytest


def test_openai_compatible_call_uses_configured_base_url_and_tools(monkeypatch):
    from agent import llm

    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "tool_calls": [
                                {
                                    "function": {
                                        "name": "generate_forge_query",
                                        "arguments": json.dumps({
                                            "scan": "orders",
                                            "select": ["orders.id"],
                                        }),
                                    }
                                }
                            ]
                        }
                    }
                ]
            }

    def fake_post(url, headers, json, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["json"] = json
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("httpx.post", fake_post)
    monkeypatch.setattr(llm.cfg, "LLM_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3")
    monkeypatch.setattr(llm.cfg, "LLM_API_KEY", "test-key")
    monkeypatch.setattr(llm.cfg, "LLM_MODEL", "doubao-seed-2-1-pro-260628")
    monkeypatch.setattr(llm.cfg, "LLM_TOOL_CHOICE", "auto")
    monkeypatch.setattr(llm.cfg, "LLM_TIMEOUT_SECONDS", 90)

    result = llm._call_openai(
        messages=[{"role": "user", "content": "查询订单"}],
        system="system prompt",
        tools=[
            {
                "name": "generate_forge_query",
                "description": "Generate Forge JSON.",
                "input_schema": {
                    "type": "object",
                    "properties": {"scan": {"type": "string"}},
                    "required": ["scan"],
                },
            }
        ],
    )

    assert captured["url"] == "https://ark.cn-beijing.volces.com/api/v3/chat/completions"
    assert captured["headers"]["Authorization"] == "Bearer test-key"
    assert captured["json"]["model"] == "doubao-seed-2-1-pro-260628"
    assert captured["json"]["tools"][0]["type"] == "function"
    assert captured["json"]["tools"][0]["function"]["parameters"]["required"] == ["scan"]
    assert captured["timeout"] == 90
    assert result == {
        "tool": "generate_forge_query",
        "input": {"scan": "orders", "select": ["orders.id"]},
    }


def test_openai_plain_text_call_omits_tools_and_tool_choice(monkeypatch):
    from agent import llm

    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "plain response"}}]}

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse()

    monkeypatch.setattr("httpx.post", fake_post)
    monkeypatch.setattr(llm.cfg, "LLM_TOOL_CHOICE", "required")

    result = llm._call_openai([], "system", tools=[])

    assert "tools" not in captured
    assert "tool_choice" not in captured
    assert result == {"tool": None, "text": "plain response"}


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("auto", "auto"),
        ("required", "required"),
        (
            "named",
            {"type": "function", "function": {"name": "generate_forge_query"}},
        ),
    ],
)
def test_openai_tool_choice_modes(monkeypatch, mode, expected):
    from agent import llm

    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{
                    "message": {
                        "tool_calls": [{
                            "function": {
                                "name": "generate_forge_query",
                                "arguments": '{"scan":"orders","select":["orders.id"]}',
                            }
                        }]
                    }
                }]
            }

    def fake_post(url, headers, json, timeout):
        captured.update(json)
        return FakeResponse()

    monkeypatch.setattr("httpx.post", fake_post)
    monkeypatch.setattr(llm.cfg, "LLM_TOOL_CHOICE", mode)

    llm._call_openai([], "system", tools=[{
        "name": "generate_forge_query",
        "description": "Generate query",
        "input_schema": {"type": "object"},
    }])

    assert captured["tool_choice"] == expected


def test_openai_empty_choices_has_compatibility_error(monkeypatch):
    from agent import llm

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": []}

    monkeypatch.setattr("httpx.post", lambda *args, **kwargs: FakeResponse())

    with pytest.raises(llm.LLMCompatibilityError, match="choices"):
        llm._call_openai([], "system", tools=[])


def test_openai_required_mode_rejects_missing_tool_call(monkeypatch):
    from agent import llm

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "I cannot call tools"}}]}

    monkeypatch.setattr("httpx.post", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(llm.cfg, "LLM_TOOL_CHOICE", "required")

    with pytest.raises(llm.LLMCompatibilityError, match="tool_calls"):
        llm._call_openai([], "system", tools=[{
            "name": "generate_forge_query",
            "description": "Generate query",
            "input_schema": {"type": "object"},
        }])


def test_openai_invalid_tool_arguments_has_compatibility_error(monkeypatch):
    from agent import llm

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [{"message": {"tool_calls": [{"function": {
                    "name": "generate_forge_query",
                    "arguments": "not-json",
                }}]}}]
            }

    monkeypatch.setattr("httpx.post", lambda *args, **kwargs: FakeResponse())
    monkeypatch.setattr(llm.cfg, "LLM_TOOL_CHOICE", "auto")

    with pytest.raises(llm.LLMCompatibilityError, match="arguments"):
        llm._call_openai([], "system", tools=[{
            "name": "generate_forge_query",
            "description": "Generate query",
            "input_schema": {"type": "object"},
        }])


def test_openai_http_error_is_sanitized(monkeypatch):
    import httpx
    from agent import llm

    request = httpx.Request("POST", "https://provider.example/v1/chat/completions")
    response = httpx.Response(401, request=request)

    def fake_post(*args, **kwargs):
        raise httpx.HTTPStatusError("unauthorized", request=request, response=response)

    monkeypatch.setattr("httpx.post", fake_post)
    monkeypatch.setattr(llm.cfg, "LLM_API_KEY", "secret-must-not-leak")
    monkeypatch.setattr(llm.cfg, "LLM_BASE_URL", "https://provider.example/v1")

    with pytest.raises(llm.LLMCompatibilityError, match="HTTP 401") as exc_info:
        llm._call_openai([], "system", tools=[])

    assert "secret-must-not-leak" not in str(exc_info.value)
