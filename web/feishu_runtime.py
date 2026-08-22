"""Managed Feishu WebSocket runtime for the thin Pi channel adapter."""
from __future__ import annotations

import atexit
import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class FeishuRuntimeStatus:
    enabled: bool
    credentials_configured: bool
    channel_key_configured: bool
    process_running: bool
    last_error: str | None = None


class FeishuRuntimeSupervisor:
    def __init__(self, config_path: Path | None = None) -> None:
        self._config_path = config_path or Path(__file__).resolve().parents[1] / "forge.yaml"
        self._process: subprocess.Popen[bytes] | None = None
        self._lock = threading.RLock()
        self._last_error: str | None = None

    def status(self) -> FeishuRuntimeStatus:
        with self._lock:
            settings = self._settings()
            running = self._process is not None and self._process.poll() is None
            return FeishuRuntimeStatus(
                enabled=settings["enabled"],
                credentials_configured=settings["credentials_configured"],
                channel_key_configured=settings["channel_key_configured"],
                process_running=running,
                last_error=self._last_error,
            )

    def reload(self) -> FeishuRuntimeStatus:
        with self._lock:
            self._stop_locked()
            settings = self._settings()
            if not settings["enabled"]:
                self._last_error = None
                return self.status()
            if not settings["credentials_configured"]:
                self._last_error = "飞书 App ID / App Secret 未配置"
                return self.status()
            if not settings["channel_key_configured"]:
                self._last_error = "Forge → Pi Channel Service Key 未配置"
                return self.status()
            try:
                allowed_environment = (
                    "HOME", "PATH", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR",
                    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "PI_ORCHESTRATOR_URL",
                    "PI_CHANNEL_SERVICE_KEY", "FEISHU_APP_ID", "FEISHU_APP_SECRET",
                    "FEISHU_VERIFICATION_TOKEN", "FEISHU_ENCRYPT_KEY",
                )
                environment = {
                    key: os.environ[key] for key in allowed_environment if os.environ.get(key)
                }
                environment["FEISHU_PI_ENABLED"] = "true"
                environment["PYTHONUNBUFFERED"] = "1"
                environment["FORGE_DISABLE_DOTENV"] = "true"
                self._process = subprocess.Popen(
                    [sys.executable, "-m", "web.feishu_pi"],
                    cwd=str(self._config_path.parent),
                    env=environment,
                    stdin=subprocess.DEVNULL,
                )
                self._last_error = None
            except OSError as exc:
                self._process = None
                self._last_error = f"飞书 Runtime 启动失败：{type(exc).__name__}"
            return self.status()

    def stop(self) -> None:
        with self._lock:
            self._stop_locked()

    def _stop_locked(self) -> None:
        process = self._process
        self._process = None
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)

    def _settings(self) -> dict[str, bool]:
        try:
            root = yaml.safe_load(self._config_path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError):
            root = {}
        feishu = root.get("feishu") if isinstance(root, dict) else None
        pi = root.get("pi_orchestrator") if isinstance(root, dict) else None
        feishu = feishu if isinstance(feishu, dict) else {}
        pi = pi if isinstance(pi, dict) else {}
        return {
            "enabled": feishu.get("pi_enabled") is True,
            "credentials_configured": bool(
                (os.getenv("FEISHU_APP_ID") or feishu.get("app_id"))
                and (os.getenv("FEISHU_APP_SECRET") or feishu.get("app_secret"))
            ),
            "channel_key_configured": bool(
                os.getenv("PI_CHANNEL_SERVICE_KEY") or pi.get("channel_service_key")
            ),
        }


feishu_runtime = FeishuRuntimeSupervisor()
atexit.register(feishu_runtime.stop)
