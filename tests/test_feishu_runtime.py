from __future__ import annotations

from pathlib import Path

import pytest

from web.feishu_runtime import FeishuRuntimeStatus, FeishuRuntimeSupervisor


class _FakeProcess:
    def __init__(self) -> None:
        self.running = True
        self.terminated = False

    def poll(self):
        return None if self.running else 0

    def terminate(self):
        self.terminated = True
        self.running = False

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self.running = False


def test_feishu_action_replaces_buttons_with_progress_before_final_card(monkeypatch):
    import web.feishu_pi as feishu_pi

    class _FakePiClient:
        def submit_action(self, **kwargs):
            return {"status": "accepted", "task": {"task_run_id": "tr_demo"}}

        def wait_for_presentation(self, task_run_id):
            assert task_run_id == "tr_demo"
            return {
                "kind": "analysis", "title": "分析完成", "markdown": "已完成",
                "fields": [], "table": None, "actions": [],
            }

    cards: list[dict] = []
    monkeypatch.setattr(feishu_pi, "_get_pi_client", lambda: _FakePiClient())
    monkeypatch.setattr(feishu_pi, "_update_card", lambda _message_id, card: cards.append(card))

    feishu_pi._process_action(
        "ou_demo", "oc_demo", "om_card", "evt_action", "analyze", "tr_demo", {}
    )

    assert len(cards) == 2
    assert cards[0]["header"]["title"]["content"] == "正在分析结果"
    assert "自动更新" in cards[0]["body"]["elements"][0]["content"]
    assert all(item.get("tag") != "button" for item in cards[0]["body"]["elements"])
    assert cards[1]["header"]["title"]["content"] == "分析完成"


def test_feishu_runtime_requires_enabled_credentials_and_channel_key(tmp_path, monkeypatch):
    config_path = tmp_path / "forge.yaml"
    config_path.write_text("feishu:\n  pi_enabled: true\n  app_id: cli_demo\n  app_secret: secret\n")
    supervisor = FeishuRuntimeSupervisor(config_path)

    status = supervisor.reload()

    assert status.enabled is True
    assert status.credentials_configured is True
    assert status.channel_key_configured is False
    assert status.process_running is False
    assert "Channel Service Key" in (status.last_error or "")


def test_feishu_runtime_hot_reloads_one_managed_process(tmp_path, monkeypatch):
    config_path = tmp_path / "forge.yaml"
    config_path.write_text(
        "feishu:\n  pi_enabled: true\n  app_id: cli_demo\n  app_secret: secret\n"
        "pi_orchestrator:\n  channel_service_key: channel-secret\n"
    )
    processes: list[_FakeProcess] = []

    def fake_popen(*args, **kwargs):
        process = _FakeProcess()
        processes.append(process)
        assert "secret" not in " ".join(args[0])
        assert "LLM_API_KEY" not in kwargs["env"]
        assert kwargs["env"]["FORGE_DISABLE_DOTENV"] == "true"
        return process

    monkeypatch.setattr("web.feishu_runtime.subprocess.Popen", fake_popen)
    supervisor = FeishuRuntimeSupervisor(config_path)

    first = supervisor.reload()
    second = supervisor.reload()

    assert first.process_running is True
    assert second.process_running is True
    assert len(processes) == 2
    assert processes[0].terminated is True
    assert processes[1].terminated is False
    supervisor.stop()
    assert processes[1].terminated is True


@pytest.mark.asyncio
async def test_feishu_settings_masks_all_channel_secrets(client, monkeypatch):
    import web.routes.settings as settings

    monkeypatch.setattr(settings, "_load_forge_yaml", lambda: {
        "feishu": {
            "app_id": "cli_demo",
            "app_secret": "app-secret-value",
            "verification_token": "verification-secret-value",
            "encrypt_key": "encrypt-secret-value",
        }
    })
    monkeypatch.setattr(settings.feishu_runtime, "status", lambda: FeishuRuntimeStatus(
        True, True, True, True
    ))
    async def pi_status():
        return {"available": True, "ingress_configured": True, "identity_count": 1,
                "auto_binding_pending": False}
    monkeypatch.setattr(settings, "_get_pi_channel_status", pi_status)

    response = await client.get("/admin/settings")

    assert response.status_code == 200
    assert "app-secret-value" not in response.text
    assert "verification-secret-value" not in response.text
    assert "encrypt-secret-value" not in response.text


@pytest.mark.asyncio
async def test_feishu_settings_validate_enable_and_hot_start(client, monkeypatch):
    import web.routes.settings as settings

    saved: dict = {"feishu": {
        "app_secret": "existing-secret",
        "verification_token": "existing-verification",
        "encrypt_key": "existing-encrypt-key",
    }}
    writes: list[dict] = []
    reloads = 0

    async def valid(app_id: str, app_secret: str):
        assert app_id == "cli_valid"
        assert app_secret == "existing-secret"
        return True, ""

    def reload_runtime():
        nonlocal reloads
        reloads += 1
        return FeishuRuntimeStatus(True, True, True, True)

    monkeypatch.setattr(settings, "_load_forge_yaml", lambda: saved)
    monkeypatch.setattr(settings, "_save_forge_yaml", lambda value: writes.append(value))
    monkeypatch.setattr(settings, "_validate_feishu_credentials", valid)
    monkeypatch.setattr(settings.feishu_runtime, "reload", reload_runtime)

    response = await client.post(
        "/admin/settings/feishu",
        data={
            "app_id": "cli_valid",
            "app_secret": "********cret",
            "verification_token": "********tion",
            "encrypt_key": "********-key",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/settings?saved=feishu"
    assert writes[0]["feishu"]["pi_enabled"] is True
    assert writes[0]["feishu"]["app_secret"] == "existing-secret"
    assert writes[0]["feishu"]["verification_token"] == "existing-verification"
    assert writes[0]["feishu"]["encrypt_key"] == "existing-encrypt-key"
    assert reloads == 1


@pytest.mark.asyncio
async def test_feishu_settings_reject_invalid_credentials_without_writing(client, monkeypatch):
    import web.routes.settings as settings

    async def invalid(app_id: str, app_secret: str):
        return False, "invalid credentials"

    writes: list[dict] = []
    monkeypatch.setattr(settings, "_load_forge_yaml", lambda: {"feishu": {}})
    monkeypatch.setattr(settings, "_save_forge_yaml", lambda value: writes.append(value))
    monkeypatch.setattr(settings, "_validate_feishu_credentials", invalid)

    response = await client.post(
        "/admin/settings/feishu",
        data={"app_id": "bad", "app_secret": "bad"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert "error=invalid%20credentials" in response.headers["location"]
    assert writes == []
