#!/usr/bin/env python3
"""
Claude Code Stop Hook — 对话结束时自动导出可读 Markdown 日志。

Hook payload 通过 stdin 传入（JSON），包含 transcript_path 和 session_id。
导出文件写入 docs/conversation-logs/YYYY-MM-DD_HH-MM_<session>.md
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
LOG_DIR = ROOT / "docs" / "conversation-logs"


def parse_transcript(jsonl_path: str) -> list[dict]:
    """解析 JSONL，提取 user / assistant 轮次，带时间戳。"""
    turns = []
    try:
        with open(jsonl_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except json.JSONDecodeError:
                    continue

                t = d.get("type")
                if t not in ("user", "assistant"):
                    continue

                ts = d.get("timestamp", "")[:19]
                msg = d.get("message", {})
                content = msg.get("content", "")

                if isinstance(content, list):
                    parts = []
                    for c in content:
                        if isinstance(c, dict):
                            if c.get("type") == "text":
                                parts.append(c.get("text", ""))
                            elif c.get("type") == "tool_use":
                                parts.append(f"[tool: {c.get('name','')}]")
                    text = "\n".join(p for p in parts if p).strip()
                else:
                    text = str(content).strip()

                # 跳过系统内部消息
                if "<command-message>" in text or not text:
                    continue

                turns.append({"role": t, "ts": ts, "text": text})

    except FileNotFoundError:
        pass
    return turns


def render_markdown(turns: list[dict], session_id: str) -> str:
    """渲染为 Markdown。"""
    if not turns:
        return ""

    start_ts = turns[0]["ts"] if turns else ""
    end_ts   = turns[-1]["ts"] if turns else ""

    lines = [
        f"# 对话记录",
        f"",
        f"**Session**: `{session_id}`  ",
        f"**开始**: {start_ts}  ",
        f"**结束**: {end_ts}  ",
        f"**轮次**: {len(turns)}",
        f"",
        f"---",
        f"",
    ]

    for turn in turns:
        role_label = "**User**" if turn["role"] == "user" else "**Claude**"
        ts = f"`{turn['ts']}`" if turn["ts"] else ""
        lines.append(f"### {role_label} {ts}")
        lines.append("")
        lines.append(turn["text"])
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    # 从 stdin 读取 hook payload
    try:
        payload = json.loads(sys.stdin.read())
    except (json.JSONDecodeError, ValueError):
        payload = {}

    transcript_path = payload.get("transcript_path", "")
    session_id      = payload.get("session_id", "unknown")[:8]

    if not transcript_path:
        sys.exit(0)

    turns = parse_transcript(transcript_path)
    if not turns:
        sys.exit(0)

    # 用对话开始时间命名文件
    start_ts = turns[0]["ts"].replace(":", "-").replace("T", "_") if turns else \
               datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    out_path = LOG_DIR / f"{start_ts[:16]}_{session_id}.md"

    md = render_markdown(turns, session_id)
    out_path.write_text(md, encoding="utf-8")

    print(f"[export_conversation] saved → {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
