"""
Staging → Registry 自动合并模块。

职责：
    读取 .forge/staging/ 目录中的歧义确认记录（JSON），
    将其合并入 registry/data/disambiguations.registry.yaml。

Staging 文件格式（每个文件一条记录）：
    {
      "key":                  "revenue_order_level",    # 唯一标识，会成为 YAML 的 key
      "label":                "销售额（订单级别）",
      "triggers":             ["销售额", "营收"],
      "context":              "...",                    # 注入 LLM 的说明文本
      "original_question":    "统计各品类销售额",
      "clarification_prompt": "您指的是订单金额还是商品明细金额？",
      "user_response":        "订单总额",
      "ambiguity_keys":       ["revenue"],              # 触发的原始规则 key
      "confirmed_at":         "2026-03-16T14:30:22",
      "requires_clarification": false,
      "confirmed_by_users":   true
    }

合并策略：
    - key 不存在 → 直接插入
    - key 已存在且 confirmed_by_users=false → 覆盖（用确认过的版本替换猜测版本）
    - key 已存在且 confirmed_by_users=true  → 跳过（已确认的记录不覆盖）
    - 合并成功后将 staging 文件移动到 .forge/staging/done/ 目录

调用方式：
    CLI：  forge sync-staging
    Python：from registry.staging_sync import promote_staged
            promote_staged(staging_dir, disambiguations_path)
"""
from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# staging 文件中允许写入 disambiguations.yaml 的字段白名单
_ALLOWED_FIELDS = {
    "label", "triggers", "context",
    "requires_clarification", "confirmed_by_users",
    "clarification_question",
}


def promote_staged(
    staging_dir: Path | str,
    disambiguations_path: Path | str,
) -> dict[str, int]:
    """
    扫描 staging_dir，将歧义确认记录合并入 disambiguations.registry.yaml。

    Args:
        staging_dir:           .forge/staging/ 路径
        disambiguations_path:  registry/data/disambiguations.registry.yaml 路径

    Returns:
        {"added": N, "updated": N, "skipped": N}  合并统计
    """
    staging_dir = Path(staging_dir)
    disambiguations_path = Path(disambiguations_path)

    # 读取现有歧义规则
    try:
        existing: dict = yaml.safe_load(disambiguations_path.read_text()) or {}
    except FileNotFoundError:
        existing = {}

    done_dir = staging_dir / "done"
    done_dir.mkdir(parents=True, exist_ok=True)

    stats = {"added": 0, "updated": 0, "skipped": 0}
    changed = False

    for fp in sorted(staging_dir.glob("*.json")):
        try:
            record: dict = json.loads(fp.read_text())
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping malformed staging file %s: %s", fp.name, exc)
            continue

        key = record.get("key", "").strip()
        if not key:
            continue

        # 判断是否应写入
        current = existing.get(key)
        if current is not None and current.get("confirmed_by_users", False):
            # 已经是用户确认过的条目，跳过
            stats["skipped"] += 1
        else:
            # 写入：只取白名单字段，并强制 confirmed_by_users=true
            entry = {k: v for k, v in record.items() if k in _ALLOWED_FIELDS}
            entry["confirmed_by_users"] = True
            entry.setdefault("requires_clarification", False)

            if current is None:
                stats["added"] += 1
            else:
                stats["updated"] += 1

            existing[key] = entry
            changed = True

        # 移动到 done 目录（无论成功或跳过）
        shutil.move(str(fp), done_dir / fp.name)

    if changed:
        disambiguations_path.parent.mkdir(parents=True, exist_ok=True)
        disambiguations_path.write_text(
            yaml.dump(existing, allow_unicode=True, sort_keys=False, default_flow_style=False)
        )

    return stats


def write_staging_record(
    staging_dir: Path | str,
    key: str,
    label: str,
    triggers: list[str],
    context: str,
    original_question: str,
    clarification_prompt: str,
    user_response: str,
    ambiguity_keys: list[str],
    requires_clarification: bool = False,
) -> Path:
    """
    将一条用户确认的歧义消除记录写入 staging 目录。

    由 agent.py 在澄清轮次成功完成后调用（pending_intent resolved + 查询成功）。

    Returns:
        写入的 staging 文件路径。
    """
    staging_dir = Path(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{ts}_{key[:40]}.json"

    record = {
        "key":                  key,
        "label":                label,
        "triggers":             triggers,
        "context":              context,
        "original_question":    original_question,
        "clarification_prompt": clarification_prompt,
        "user_response":        user_response,
        "ambiguity_keys":       ambiguity_keys,
        "confirmed_at":         datetime.now(tz=timezone.utc).isoformat(),
        "requires_clarification": requires_clarification,
        "confirmed_by_users":   True,
    }

    fp = staging_dir / filename
    fp.write_text(json.dumps(record, ensure_ascii=False, indent=2))
    return fp
