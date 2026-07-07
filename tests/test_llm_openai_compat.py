from __future__ import annotations

import json


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
    assert result == {
        "tool": "generate_forge_query",
        "input": {"scan": "orders", "select": ["orders.id"]},
    }
