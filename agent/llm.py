"""
LLM 客户端模块 — 支持 Anthropic 原生 SDK 和任意 OpenAI 兼容接口。

职责：
    1. _registry_context()：读取结构层 + 语义层注册表，格式化为 LLM 可理解的文本
    2. call()：统一调用入口，根据 cfg.LLM_PROVIDER 分发到对应的后端
    3. _call_anthropic() / _call_openai()：各自处理 tool_use 响应格式的差异

返回值格式（统一为 dict）：
    {"tool": "generate_forge_query", "input": {...}}   # LLM 调用了工具
    {"tool": "define_metric",        "input": {...}}   # LLM 定义了指标
    {"tool": None, "text": "..."}                      # LLM 直接文字回复

工具列表（_TOOLS）：
    generate_forge_query：Forge JSON Schema 作为参数 schema，确保 LLM 输出结构合法
    define_metric：指标定义字段 schema，LLM 从用户描述中提取并填写各字段
"""
from __future__ import annotations
import json
from pathlib import Path
from typing import Any

import yaml

from config import cfg
from agent.prompts import build_system
from forge.schema_builder import build_tool_schema
from forge.retriever import SchemaRetriever, make_embed_fn, make_query_embed_fn


# ── Schema 检索器（模块级单例，懒初始化）─────────────────────────────────────

_retriever: SchemaRetriever | None = None
_query_embed_fn = None
_retriever_initialized = False


def _get_retriever() -> tuple[SchemaRetriever | None, object | None]:
    """
    懒加载 SchemaRetriever 单例。

    首次调用时读取 registry 并尝试加载/构建向量索引。
    后续调用直接返回缓存的实例（registry 变更通过 forge sync 重建索引）。
    """
    global _retriever, _query_embed_fn, _retriever_initialized
    if _retriever_initialized:
        return _retriever, _query_embed_fn

    _retriever_initialized = True
    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text())
        cache_path = cfg.REGISTRY_PATH.parent / ".forge" / "schema_embeddings.pkl"
        r = SchemaRetriever(registry, cache_path=cache_path)

        embed_key  = getattr(cfg, "EMBED_API_KEY",  None) or getattr(cfg, "LLM_API_KEY", None)
        embed_url  = getattr(cfg, "EMBED_BASE_URL", None) or "https://api.minimaxi.com/v1"
        embed_model = getattr(cfg, "EMBED_MODEL",   None) or "embo-01"

        if embed_key:
            if not r.load_index():
                db_fn = make_embed_fn(embed_key, embed_url, embed_model, "db")
                r.build_index(db_fn)
            _query_embed_fn = make_query_embed_fn(embed_key, embed_url, embed_model)

        _retriever = r
    except Exception:
        pass  # retriever 不可用时静默降级，不影响 Agent 运行

    return _retriever, _query_embed_fn


# ── 注册表上下文格式化 ────────────────────────────────────────────────────────

def _registry_context(question: str | None = None) -> str:
    """
    读取结构层和语义层注册表，格式化为便于 LLM 理解的纯文本。

    输出结构（示例）：
        表结构：
          orders: id, user_id, status, total_amount, created_at
          users:  id, name, city, is_vip

        原子指标（直接可查）：
          order_amount（订单金额）[sum(orders.total_amount)]
            含义：已完成订单的成交金额
            必须过滤：orders.status = 'completed'
            时间字段：orders.created_at
            可用维度：users.city, users.is_vip

        衍生指标（组合计算）：
          repurchase_rate（复购率）= repurchase_users / ordered_users
            含义：有重复购买行为的用户占比
            时间字段：orders.created_at（统一应用于分子和分母）

    每次 LLM 调用前实时读取文件，保证 schema 变更后立即生效，无需重启服务。

    Returns:
        格式化后的注册表上下文字符串；若文件不存在则返回提示信息。
    """
    lines: list[str] = []

    # ── 结构层：表名和字段名（向量检索精简 or 全量）────────────────────────────
    try:
        schema = json.loads(cfg.REGISTRY_PATH.read_text())

        # 当有问题文本时，用检索器只取相关表；否则全量展示
        retriever, q_embed_fn = _get_retriever()
        if question and retriever:
            top_k = getattr(cfg, "RETRIEVAL_TOP_K", 5)
            selected_tables = retriever.retrieve(question, q_embed_fn, top_k=top_k)
            lines.append(f"表结构（与问题相关的 {len(selected_tables)} 张表）：")
            tables_info = schema.get("tables", schema)
            for table in selected_tables:
                info = tables_info.get(table, {})
                cols = info.get("columns", info) if isinstance(info, dict) else info
                if isinstance(cols, dict):
                    col_parts = []
                    for col_name, meta in cols.items():
                        if isinstance(meta, dict) and meta.get("enum"):
                            col_parts.append(f"{col_name}[{'/'.join(str(v) for v in meta['enum'])}]")
                        else:
                            col_parts.append(col_name)
                    lines.append(f"  {table}: {', '.join(col_parts)}")
                else:
                    lines.append(f"  {table}: {', '.join(cols)}")
        else:
            # 全量模式（无问题文本 / 检索器不可用）
            tables = schema.get("tables", schema)
            lines.append("表结构：")
            for table, info in tables.items():
                cols = info.get("columns", info) if isinstance(info, dict) else info
                if isinstance(cols, dict):
                    col_parts = []
                    for col_name, meta in cols.items():
                        if isinstance(meta, dict) and meta.get("enum"):
                            col_parts.append(f"{col_name}[{'/'.join(str(v) for v in meta['enum'])}]")
                        else:
                            col_parts.append(col_name)
                    lines.append(f"  {table}: {', '.join(col_parts)}")
                else:
                    lines.append(f"  {table}: {', '.join(cols)}")
    except Exception:
        lines.append("表结构：未找到，请先运行 forge sync。")

    # ── 语义层：原子指标和衍生指标 ────────────────────────────────────────────
    try:
        metrics: dict = yaml.safe_load(cfg.METRICS_PATH.read_text()) or {}
        atomics     = {k: v for k, v in metrics.items() if v.get("metric_class") == "atomic"}
        derivatives = {k: v for k, v in metrics.items() if v.get("metric_class") == "derivative"}

        if atomics:
            lines.append("\n原子指标（直接可查）：")
            for name, m in atomics.items():
                agg  = m.get("aggregation", "")
                msr  = m.get("measure", "")
                desc = m.get("description", "")
                # 格式：name（label）[agg(measure)]
                lines.append(f"  {name}（{m.get('label', name)}）[{agg}({msr})]")
                lines.append(f"    含义：{desc}")
                # qualifiers 是必须永远应用的业务过滤条件，提示 LLM 在生成查询时注入
                if m.get("qualifiers"):
                    lines.append(f"    必须过滤：{'; '.join(m['qualifiers'])}")
                if m.get("period_col"):
                    lines.append(f"    时间字段：{m['period_col']}")
                if m.get("dimensions"):
                    lines.append(f"    可用维度：{', '.join(m['dimensions'])}")

        if derivatives:
            lines.append("\n衍生指标（组合计算）：")
            for name, m in derivatives.items():
                num  = m.get("numerator", "")
                den  = m.get("denominator", "")
                desc = m.get("description", "")
                # 格式：name（label）= numerator / denominator
                lines.append(f"  {name}（{m.get('label', name)}）= {num} / {den}")
                lines.append(f"    含义：{desc}")
                if m.get("period_col"):
                    # 提示 LLM：time window 统一应用于分子和分母两个原子指标
                    lines.append(f"    时间字段：{m['period_col']}（统一应用于分子和分母）")
                if m.get("dimensions"):
                    lines.append(f"    可用维度：{', '.join(m['dimensions'])}")
                if m.get("notes"):
                    # 多行 notes 折叠为单行，用 | 分隔，避免破坏上下文格式
                    note = m["notes"].strip().replace("\n", " | ")
                    lines.append(f"    注意：{note}")
    except Exception:
        # metrics.registry.yaml 不存在或解析失败时静默忽略，不影响表结构上下文
        pass

    return "\n".join(lines)


# ── 工具定义 ──────────────────────────────────────────────────────────────────

_DEFINE_METRIC_TOOL = {
    # 静态工具：不依赖 schema，所有字段都是固定的业务语义结构
    "name": "define_metric",
    "description": "从用户描述中提取业务指标定义，保存到语义层 Registry。",
    "input_schema": {
        "type": "object",
        "required": ["name", "metric_class", "label", "description"],
        "properties": {
            "name": {
                "type": "string",
                "description": "指标标识符，snake_case，如 repurchase_rate",
            },
            "metric_class": {
                "type": "string",
                "enum": ["atomic", "derivative"],
                "description": "atomic=原子指标（直接聚合字段），derivative=衍生指标（两个原子指标的比率）",
            },
            "label": {
                "type": "string",
                "description": "中文显示名称，如「复购率」",
            },
            "description": {
                "type": "string",
                "description": "指标的完整自然语言定义",
            },
            "measure": {
                "type": "string",
                "description": "【atomic 必填】度量字段，格式 table.column，如 orders.total_amount",
            },
            "aggregation": {
                "type": "string",
                "enum": ["sum", "count", "count_distinct", "avg", "min", "max"],
                "description": "【atomic 必填】聚合方式",
            },
            "qualifiers": {
                "type": "array",
                "items": {"type": "string"},
                "description": "业务限定，永远生效，如 [\"orders.status = 'completed'\"]",
            },
            "period_col": {
                "type": "string",
                "description": "时间字段，用户指定统计周期时作用于此，如 orders.created_at",
            },
            "dimensions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "支持的分析维度，如 [\"users.city\", \"users.is_vip\"]",
            },
            "numerator": {
                "type": "string",
                "description": "【derivative 必填】分子，填写原子指标的 name",
            },
            "denominator": {
                "type": "string",
                "description": "【derivative 必填】分母，填写原子指标的 name",
            },
            "notes": {
                "type": "string",
                "description": "边界说明；当分子/分母 qualifiers 不一致时必填，解释原因",
            },
        },
    },
}


def _build_tools(registry: dict) -> list[dict]:
    """
    根据当前 schema registry 动态构建工具列表。

    generate_forge_query 的 input_schema 从 registry 中提取列名枚举，
    使 LLM 在 col 字段只能选择已注册的 table.col，不能自由创造列名。

    Args:
        registry: schema.registry.json 解析后的 dict

    Returns:
        [generate_forge_query_tool, define_metric_tool]
    """
    return [
        {
            "name": "generate_forge_query",
            "description": "根据自然语言查询需求生成 Forge JSON 查询结构。",
            "input_schema": build_tool_schema(registry),
        },
        _DEFINE_METRIC_TOOL,
    ]


# ── Anthropic 后端 ────────────────────────────────────────────────────────────

def _call_anthropic(messages: list[dict], system: str, tools: list[dict]) -> dict:
    """
    调用 Anthropic Messages API。

    Args:
        messages: 格式化后的对话历史 [{"role": ..., "content": ...}]。
        system:   包含注册表上下文的完整 system prompt。
        tools:    _build_tools() 动态生成的工具列表。

    Returns:
        统一格式的响应字典，参见模块文档。
    """
    import anthropic
    client = anthropic.Anthropic(api_key=cfg.LLM_API_KEY)
    response = client.messages.create(
        model=cfg.LLM_MODEL,
        max_tokens=2048,
        system=system,
        tools=tools,
        messages=messages,
    )
    for block in response.content:
        if block.type == "tool_use":
            return {"tool": block.name, "input": block.input}
    text = next((b.text for b in response.content if hasattr(b, "text")), "")
    return {"tool": None, "text": text}


# ── OpenAI 兼容后端 ───────────────────────────────────────────────────────────

def _call_openai(messages: list[dict], system: str, tools: list[dict]) -> dict:
    """
    调用任意 OpenAI 兼容接口（包括 OpenAI、DeepSeek、通义等）。

    Args:
        messages: 对话历史（不含 system），system 会被前置为首条消息。
        system:   完整 system prompt。
        tools:    _build_tools() 动态生成的工具列表。

    Returns:
        统一格式的响应字典，参见模块文档。
    """
    import httpx, json as _json

    base_url = cfg.LLM_BASE_URL or "https://api.openai.com/v1"
    headers = {
        "Authorization": f"Bearer {cfg.LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": cfg.LLM_MODEL,
        "messages": [{"role": "system", "content": system}] + messages,
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t["description"],
                    "parameters": t["input_schema"],  # OpenAI 叫 parameters
                },
            }
            for t in tools
        ],
        "tool_choice": "auto",
    }
    r = httpx.post(f"{base_url}/chat/completions", headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    choice = r.json()["choices"][0]["message"]
    if choice.get("tool_calls"):
        tc = choice["tool_calls"][0]
        return {
            "tool":  tc["function"]["name"],
            "input": _json.loads(tc["function"]["arguments"]),
        }
    return {"tool": None, "text": choice.get("content", "")}


# ── 公开调用接口 ──────────────────────────────────────────────────────────────

def call(history: list[Any]) -> dict:
    """
    LLM 统一调用入口。

    根据 cfg.LLM_PROVIDER 自动分发到 Anthropic 或 OpenAI 兼容后端。
    每次调用前实时读取注册表上下文，确保 LLM 始终使用最新的表结构和指标定义。

    Args:
        history: Session.recent() 返回的 Message 列表，包含最近 N 条对话记录。

    Returns:
        {"tool": str, "input": dict}  — LLM 调用了工具
        {"tool": None, "text": str}   — LLM 直接文字回复
    """
    # 每次调用前实时读取 registry，动态生成带列名枚举约束的 tool schema
    registry: dict = {}
    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text())
    except Exception:
        pass
    tools = _build_tools(registry)

    # 提取最新的用户问题，用于 schema 向量检索（精简 context）
    current_question: str | None = None
    for m in reversed(history):
        if m.role == "user":
            current_question = m.content
            break

    system = build_system(_registry_context(question=current_question))
    messages = [{"role": m.role, "content": m.content} for m in history]
    if cfg.LLM_PROVIDER == "anthropic":
        return _call_anthropic(messages, system, tools)
    else:
        return _call_openai(messages, system, tools)
