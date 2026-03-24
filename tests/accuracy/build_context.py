"""
benchmark 专用的 registry_context 构建器。

解决问题：method 文件只加载 schema_context.md（静态表结构），
但 agent/llm.py 还会加载 field_conventions.registry.yaml 和
metrics.registry.yaml，benchmark 缺失这两层语义，导致模型
在退款率、客单价、ORDER BY 状态过滤等问题上持续犯错。

用法（在 method 文件里）：
    from build_context import build_registry_context
    REGISTRY_CONTEXT = build_registry_context(DATASETS_DIR / "large")
"""
from __future__ import annotations

from pathlib import Path

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False


def build_registry_context(dataset_dir: Path) -> str:
    """
    组装完整的 registry_context 字符串，包含：
    1. schema_context.md     — 表结构（表名、字段、枚举值、内联注解）
    2. metrics.registry.yaml — 衍生指标定义（退款率、客单价、复购率等）
    3. field_conventions.registry.yaml — 字段使用约定（ORDER BY、GROUP BY规则等）
    """
    lines: list[str] = []

    # ── 1. 表结构（主体）────────────────────────────────────────────────────
    schema_path = dataset_dir / "schema_context.md"
    if schema_path.exists():
        lines.append(schema_path.read_text(encoding="utf-8").strip())
    else:
        lines.append(f"[schema_context.md not found at {schema_path}]")

    if not _HAS_YAML:
        # yaml 不可用时退化到只有 schema_context
        return "\n\n".join(lines)

    # ── 2. 语义指标定义 ──────────────────────────────────────────────────────
    metrics_path = dataset_dir / "metrics.registry.yaml"
    if metrics_path.exists():
        try:
            metrics: dict = yaml.safe_load(metrics_path.read_text(encoding="utf-8")) or {}
            derivatives = {k: v for k, v in metrics.items()
                           if v.get("metric_class") == "derivative"}
            if derivatives:
                metric_lines = ["## 衍生指标定义（需要多步计算）"]
                for name, m in derivatives.items():
                    label = m.get("label", name)
                    desc = m.get("description", "")
                    num = m.get("numerator", "")
                    den = m.get("denominator", "")
                    metric_lines.append(f"- {name}（{label}）= {num} / {den}")
                    metric_lines.append(f"  含义：{desc}")
                    if m.get("notes"):
                        note = m["notes"].strip().replace("\n", " | ")
                        metric_lines.append(f"  注意：{note}")
                lines.append("\n".join(metric_lines))
        except Exception:
            pass

    # ── 3. 字段使用约定（全量注入，不做问题匹配过滤）────────────────────────
    conv_path = dataset_dir / "field_conventions.registry.yaml"
    if conv_path.exists():
        try:
            conventions: dict = yaml.safe_load(conv_path.read_text(encoding="utf-8")) or {}
            if conventions:
                conv_lines = ["## 字段使用约定（必须遵守）"]
                for key, rule in conventions.items():
                    label = rule.get("label", key)
                    convention = rule.get("convention", "").strip()
                    if convention:
                        conv_lines.append(f"【{label}】")
                        for line in convention.split("\n"):
                            line = line.strip()
                            if line:
                                conv_lines.append(f"  {line}")
                lines.append("\n".join(conv_lines))
        except Exception:
            pass

    return "\n\n".join(lines)
