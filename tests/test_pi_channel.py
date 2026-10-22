from __future__ import annotations


import pytest

from web.pi_channel import (
    PiChannelClient,
    PiChannelError,
    action_progress_presentation,
    presentation_to_feishu_card,
    stable_channel_action_event_id,

)



def test_feishu_card_preserves_only_channel_action_contract():
    card = presentation_to_feishu_card(
        {
            "kind": "query_review",
            "title": "Forge SQL 审核",
            "markdown": "```sql\nSELECT 1\n```",
            "fields": [],
            "table": None,
            "actions": [
                {
                    "type": "approve_query",
                    "label": "确认执行",
                    "task_run_id": "tr_demo",
                    "payload": {
                        "query_run_id": "qr_demo",
                        "sql_hash": "sha256:" + "a" * 64,
                    },
                    "style": "primary",
                }
            ],
        },
        external_user_id="ou_demo",
        conversation_id="oc_demo",
    )
    button = card["body"]["elements"][-1]["columns"][0]["elements"][0]
    assert button["value"] == {
        "pi_action": True,
        "action_type": "approve_query",
        "task_run_id": "tr_demo",
        "payload": {
            "query_run_id": "qr_demo",
            "sql_hash": "sha256:" + "a" * 64,
        },
        "user_id": "ou_demo",
        "conversation_id": "oc_demo",
    }


def test_feishu_needs_input_card_uses_form_submit_contract():
    card = presentation_to_feishu_card(
        {
            "kind": "needs_input",
            "title": "需要补充信息",
            "markdown": "请补充时间范围",
            "fields": [],
            "table": None,
            "actions": [
                {
                    "type": "provide_input",
                    "label": "提交补充信息",
                    "task_run_id": "tr_demo",
                    "payload": {"requires_text": True},
                    "style": "primary",
                }
            ],
        },
        external_user_id="ou_demo",
        conversation_id="oc_demo",
    )

    form = next(item for item in card["body"]["elements"] if item.get("tag") == "form")
    assert form["elements"][0]["name"] == "text"
    assert form["elements"][1]["name"] == "clarification_submit_tr_demo"
    assert form["elements"][1]["form_action_type"] == "submit"
    assert "action_type" not in form["elements"][1]
    assert form["elements"][1]["value"]["action_type"] == "provide_input"
    names = [form["name"], *(item["name"] for item in form["elements"])]
    assert all(names)
    assert len(names) == len(set(names))


def test_action_progress_presentation_is_specific_and_non_actionable():
    progress = action_progress_presentation("analyze")
    assert progress["kind"] == "progress"
    assert progress["title"] == "正在分析结果"
    assert "自动更新" in progress["markdown"]
    assert progress["actions"] == []


def test_feishu_action_id_is_stable_for_retries_and_changes_with_payload():
    first = stable_channel_action_event_id(
        "om_card", "tr_demo", "analyze", {"z": 1, "a": "same"}
    )
    retry = stable_channel_action_event_id(
        "om_card", "tr_demo", "analyze", {"a": "same", "z": 1}
    )
    changed = stable_channel_action_event_id(
        "om_card", "tr_demo", "analyze", {"a": "changed", "z": 1}
    )
    assert first == retry
    assert first.startswith("feishu_action_")
    assert changed != first



def test_pi_channel_client_requires_channel_service_key():
    with pytest.raises(PiChannelError, match="PI_CHANNEL_SERVICE_KEY"):
        PiChannelClient(base_url="http://pi.test", service_key="")


def test_unbound_channel_cannot_submit_or_poll(monkeypatch):
    from forge.governance_credentials import authenticate_channel
    monkeypatch.setattr("forge.governance_credentials.authenticate_channel", lambda channel, user: None)
    client = PiChannelClient(base_url="http://pi.test", service_key="synthetic-key")
    with pytest.raises(PiChannelError, match="channel_identity_unbound"):
        client.submit_message(event_id="event_1", external_user_id="unknown",
                              conversation_id="chat_1", message_id="msg_1", text="hello")
    with pytest.raises(PiChannelError, match="channel_identity_unbound"):
        client.get_presentation("tr_demo", external_user_id="unknown")


def test_channel_approval_ticket_binds_exact_event_bytes_and_reviewer(monkeypatch):
    from forge.governance import AuthenticatedPrincipal
    principal = AuthenticatedPrincipal(
        "reviewer_b", "human", "org", "team", "reviewer", 1, None, None,
        "service_key", "now", "feishu:operator_b", 1,
    )
    observed = {}
    class Response:
        status_code = 202
        def json(self):
            return {"status": "executing"}
    def mint(actor, method, path, body, action, **scope):
        observed["ticket"] = (actor, method, path, body, action, scope)
        return "synthetic-control-ticket"
    def send(method, url, **kwargs):
        observed["request"] = kwargs
        return Response()
    monkeypatch.setattr("forge.governance_credentials.authenticate_channel", lambda channel, user: principal)
    monkeypatch.setattr("forge.governance.issue_control_token", mint)
    monkeypatch.setattr("web.pi_channel.httpx.request", send)
    client = PiChannelClient(base_url="http://pi.test", service_key="synthetic-key")
    client.submit_action(event_id="event_b", external_user_id="operator_b",
                         conversation_id="chat_1", message_id="msg_1",
                         task_run_id="tr_a", action="approve_query",
                         payload={"query_run_id": "qr_a", "sql_hash": "s",
                                  "assurance_report_hash": "a", "enforcement_context_hash": "e"})
    actor, method, path, signed, action, scope = observed["ticket"]
    assert actor is principal and method == "POST" and path == "/v1/channel-events"
    assert action == "query.approve" and scope["query_run_id"] == "qr_a"
    assert scope["task_run_id"] == "tr_a" and scope["idempotency_key"] == "event_b"
    assert scope["approval_hashes"] == {
        "sql_hash": "s", "assurance_report_hash": "a", "enforcement_context_hash": "e",
    }
    assert observed["request"]["content"] == signed
    assert observed["request"]["headers"]["Idempotency-Key"] == "event_b"
    assert observed["request"]["headers"]["X-Forge-Control-Token"] == "synthetic-control-ticket"
