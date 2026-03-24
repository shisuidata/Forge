"""
业务上下文加载器 — 读取 business_context.yaml，格式化为 LLM 可读文本。

支持按类别选择性注入（thresholds / calendar / benchmarks / rules）。
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

try:
    from config import cfg
    _DEFAULT_PATH = cfg.BUSINESS_CONTEXT_PATH
except (ImportError, AttributeError):
    _DEFAULT_PATH = Path("registry/business_context.yaml")


def load(path: Path | None = None) -> dict:
    """加载业务上下文 YAML。"""
    p = path or _DEFAULT_PATH
    try:
        return yaml.safe_load(p.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.debug("Business context not available: %s", exc)
        return {}


def format_for_prompt(
    categories: list[str] | None = None,
    path: Path | None = None,
) -> str:
    """
    格式化为 LLM system prompt 注入文本。

    Args:
        categories: 要注入的类别列表，None = 全部。
                    可选值：thresholds / calendar / benchmarks / org_structure / rules
        path: 自定义文件路径
    """
    ctx = load(path)
    if not ctx:
        return ""

    parts = ["## 业务上下文"]

    # ── 阈值标准 ──────────────────────────────────────────────────────────
    if (categories is None or "thresholds" in categories) and ctx.get("thresholds"):
        parts.append("\n### 指标阈值")
        for key, t in ctx["thresholds"].items():
            label = t.get("label", key)
            note = t.get("note", "")
            nr = t.get("normal_range")
            if nr:
                parts.append(f"- **{label}**：正常区间 {nr[0]}~{nr[1]}。{note}")
            elif t.get("baseline"):
                parts.append(f"- **{label}**：基准 {t['baseline']}{t.get('unit','')}。{note}")

    # ── 日历事件 ──────────────────────────────────────────────────────────
    if (categories is None or "calendar" in categories) and ctx.get("calendar"):
        cal = ctx["calendar"]
        now = datetime.now()
        month_day = now.strftime("%m-%d")

        # 检测当前是否在某个事件期间
        active_events = []
        for promo in cal.get("promotions", []):
            if promo.get("start", "") <= month_day <= promo.get("end", ""):
                active_events.append(f"当前处于 **{promo['name']}** 大促期间（{promo['start']}~{promo['end']}），订单量预期提升 {promo.get('expected_lift', '?')} 倍")
        for season in cal.get("seasons", []):
            if season.get("start", "") <= month_day <= season.get("end", ""):
                active_events.append(f"当前处于 **{season['name']}** 期间，预计影响 {season.get('expected_impact', '?')}")

        if active_events:
            parts.append("\n### 当前日历事件")
            for e in active_events:
                parts.append(f"- {e}")

        parts.append("\n### 关键日历")
        for promo in cal.get("promotions", []):
            parts.append(f"- {promo['name']}：{promo.get('start','')}~{promo.get('end','')}（提升 {promo.get('expected_lift','?')}x）")

    # ── 行业基准 ──────────────────────────────────────────────────────────
    if (categories is None or "benchmarks" in categories) and ctx.get("benchmarks"):
        bm = ctx["benchmarks"]
        parts.append(f"\n### 行业基准（{bm.get('industry', '')}，{bm.get('updated', '')}）")
        for k, v in bm.items():
            if k not in ("industry", "source", "updated"):
                parts.append(f"- {k}: {v}")

    # ── 组织架构 ──────────────────────────────────────────────────────────
    if (categories is None or "org_structure" in categories) and ctx.get("org_structure"):
        org = ctx["org_structure"]
        if org.get("regions"):
            parts.append("\n### 区域划分")
            for region, cities in org["regions"].items():
                parts.append(f"- {region}：{', '.join(cities)}")

    # ── 业务规则 ──────────────────────────────────────────────────────────
    if (categories is None or "rules" in categories) and ctx.get("rules"):
        parts.append("\n### 业务规则")
        for r in ctx["rules"]:
            parts.append(f"- {r.get('rule', '')}")

    return "\n".join(parts) if len(parts) > 1 else ""
