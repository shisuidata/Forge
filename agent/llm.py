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
        metrics_registry: dict = {}
        try:
            metrics_registry = yaml.safe_load(cfg.METRICS_PATH.read_text()) or {}
        except Exception:
            pass
        r = SchemaRetriever(registry, cache_path=cache_path, metrics_registry=metrics_registry)

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

    # ── 歧义消除规则（与当前问题相关的条目）─────────────────────────────────
    try:
        disambiguations: dict = yaml.safe_load(
            cfg.DISAMBIGUATIONS_PATH.read_text()
        ) or {}
        matched_dis: list[str] = []
        q_lower = (question or "").lower()
        for key, rule in disambiguations.items():
            triggers = rule.get("triggers", [])
            if any(str(t).lower() in q_lower for t in triggers):
                matched_dis.append(f"  【{rule.get('label', key)}】{rule.get('context', '').strip()}")
        if matched_dis:
            lines.append("\n业务歧义说明（根据问题自动匹配）：")
            lines.extend(matched_dis)
    except Exception:
        pass

    # ── 字段使用约定（与当前问题相关的条目）─────────────────────────────────
    try:
        conventions: dict = yaml.safe_load(cfg.CONVENTIONS_PATH.read_text()) or {}
        matched_conv: list[str] = []
        for key, rule in conventions.items():
            applies_to = rule.get("applies_to", [])
            # 当问题中出现字段名（去掉 table. 前缀后的列名）或表名时注入
            col_names = {a.split(".")[-1] for a in applies_to} | {a.split(".")[0] for a in applies_to}
            if any(c.lower() in q_lower for c in col_names if len(c) >= 3):
                matched_conv.append(f"  【{rule.get('label', key)}】{rule.get('convention', '').strip()}")
        if matched_conv:
            lines.append("\n字段使用约定（根据问题自动匹配）：")
            lines.extend(matched_conv)
    except Exception:
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


_PROPOSE_METRIC_TOOL = {
    # 当用户查询的指标在语义层中不存在时，模型根据业务理解主动提案，
    # 展示给用户确认后再由 confirm_metric_definition() 持久化入库。
    "name": "propose_metric_definition",
    "description": (
        "当用户查询的业务指标在 Registry 中尚未定义时，根据数据库字段和业务常识"
        "主动提出一个指标定义草案，供用户确认。确认后自动入库，无需用户手动填写。"
        "注意：此工具仅提案，不保存；只有在用户明确回复「确认」/「是」/「对」后才入库。"
    ),
    "input_schema": {
        "type": "object",
        "required": ["name", "metric_class", "label", "description", "proposal_summary"],
        "properties": {
            "name":             {"type": "string",  "description": "指标标识符，snake_case"},
            "metric_class":     {"type": "string",  "enum": ["atomic", "derivative"]},
            "label":            {"type": "string",  "description": "中文显示名，如「门店近7日营收」"},
            "description":      {"type": "string",  "description": "完整自然语言定义"},
            "proposal_summary": {"type": "string",  "description": "向用户展示的确认提示，简明说明分子/分母或度量字段的业务含义"},
            "measure":          {"type": "string",  "description": "【atomic】度量字段，table.column"},
            "aggregation":      {"type": "string",  "enum": ["sum", "count", "count_distinct", "avg", "min", "max"]},
            "qualifiers":       {"type": "array",   "items": {"type": "string"}},
            "period_col":       {"type": "string"},
            "dimensions":       {"type": "array",   "items": {"type": "string"}},
            "numerator":        {"type": "string",  "description": "【derivative】分子原子指标 name"},
            "denominator":      {"type": "string",  "description": "【derivative】分母原子指标 name"},
            "notes":            {"type": "string"},
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
            "input_schema": build_tool_schema(registry, strict=cfg.LLM_STRICT_TOOLS),
        },
        _DEFINE_METRIC_TOOL,
        _PROPOSE_METRIC_TOOL,
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
    kwargs: dict = {"api_key": cfg.LLM_API_KEY}
    if cfg.LLM_BASE_URL:
        kwargs["base_url"] = cfg.LLM_BASE_URL
    client = anthropic.Anthropic(**kwargs)
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

    # strict mode：切换到 DeepSeek beta 端点，并在 function 定义里加 "strict": true
    strict = cfg.LLM_STRICT_TOOLS
    base_url = cfg.LLM_BASE_URL or "https://api.openai.com/v1"
    if strict and "deepseek.com" in base_url:
        # beta 端点路径与正式相同，只是 host 变为 api.deepseek.com/beta
        base_url = base_url.replace("api.deepseek.com", "api.deepseek.com/beta").rstrip("/")
        if not base_url.endswith("/v1"):
            base_url = base_url + "/v1" if "/v1" not in base_url else base_url

    headers = {
        "Authorization": f"Bearer {cfg.LLM_API_KEY}",
        "Content-Type": "application/json",
    }

    def _tool_def(t: dict) -> dict:
        fn: dict = {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],  # OpenAI 叫 parameters
        }
        if strict:
            fn["strict"] = True
        return {"type": "function", "function": fn}

    payload = {
        "model": cfg.LLM_MODEL,
        "messages": [{"role": "system", "content": system}] + messages,
        "tools": [_tool_def(t) for t in tools],
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

def _extract_hint_tables(error: str, registry: dict) -> list[str]:
    """
    从编译错误信息中提取缺失的表名，用于二次召回。

    捕获的模式：
      - "unknown table: dim_brand"
      - "no such column: dwd_order_item_detail.actual_amount"
      - 报错路径里出现的 table.column 格式
    """
    all_tables = set(registry.get("tables", {}).keys())
    found: list[str] = []
    seen: set[str] = set()

    import re as _re
    # 匹配 table.column 格式，提取 table 部分
    for m in _re.finditer(r'\b([a-z][a-z0-9_]+)\.[a-z][a-z0-9_]+', error):
        t = m.group(1)
        if t in all_tables and t not in seen:
            seen.add(t)
            found.append(t)
    # 匹配独立的表名（unknown table: xxx）
    for m in _re.finditer(r'(?:table|表)[：:\s]+([a-z][a-z0-9_]+)', error, _re.IGNORECASE):
        t = m.group(1)
        if t in all_tables and t not in seen:
            seen.add(t)
            found.append(t)
    return found


def call(history: list[Any], extra_tables: list[str] | None = None) -> dict:
    """
    LLM 统一调用入口。

    根据 cfg.LLM_PROVIDER 自动分发到 Anthropic 或 OpenAI 兼容后端。
    每次调用前实时读取注册表上下文，确保 LLM 始终使用最新的表结构和指标定义。

    Args:
        history:      Session.recent() 返回的 Message 列表，包含最近 N 条对话记录。
        extra_tables: 强制追加到检索结果的表名列表（用于错误驱动二次召回）。

    Returns:
        {"tool": str, "input": dict}  — LLM 调用了工具
        {"tool": None, "text": str}   — LLM 直接文字回复
    """
    # 每次调用前实时读取 registry
    registry: dict = {}
    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text())
    except Exception:
        pass

    # 提取最新的用户问题，用于 schema 向量检索（精简 context）
    current_question: str | None = None
    for m in reversed(history):
        if m.role == "user":
            current_question = m.content
            break

    # 用检索到的相关表构建 tool schema（避免全量 200 张表撑爆 context window）
    filtered_registry = registry
    retriever, q_embed_fn = _get_retriever()
    if current_question and retriever:
        top_k = getattr(cfg, "RETRIEVAL_TOP_K", 10)
        selected_tables = retriever.retrieve(current_question, q_embed_fn, top_k=top_k)
        # 错误驱动二次召回：追加缺失表（去重，保持顺序）
        if extra_tables:
            seen = set(selected_tables)
            for t in extra_tables:
                if t not in seen:
                    selected_tables.append(t)
                    seen.add(t)
        tables_info = registry.get("tables", {})
        filtered_registry = {"tables": {t: tables_info[t] for t in selected_tables if t in tables_info}}

    tools = _build_tools(filtered_registry)
    system = build_system(_registry_context(question=current_question), question=current_question)
    messages = [{"role": m.role, "content": m.content} for m in history]
    if cfg.LLM_PROVIDER == "anthropic":
        return _call_anthropic(messages, system, tools)
    else:
        return _call_openai(messages, system, tools)


def call_with_retry(history: list[Any], compile_fn=None) -> tuple[dict, str | None]:
    """
    带编译错误重试的 LLM 调用。

    第一次调用 call()，若 compile_fn 返回编译错误，
    从错误中提取缺失表名，扩充 schema 后重试一次。

    Args:
        history:    对话历史。
        compile_fn: callable(forge_json) -> (sql, error_str | None)
                    传 None 则等同于普通 call()，不做重试。

    Returns:
        (llm_result, sql_or_none)
    """
    result = call(history)

    if compile_fn is None or result.get("tool") != "generate_forge_query":
        return result, None

    forge_json = result.get("input", {})
    sql, error = compile_fn(forge_json)
    if not error:
        return result, sql

    # 从错误里提取缺失的表，读取 registry
    try:
        registry = json.loads(cfg.REGISTRY_PATH.read_text())
    except Exception:
        return result, None

    hint_tables = _extract_hint_tables(error, registry)
    if not hint_tables:
        return result, None  # 无法提取提示，放弃重试

    # 二次调用，追加缺失表
    result2 = call(history, extra_tables=hint_tables)
    if result2.get("tool") != "generate_forge_query":
        return result, None

    sql2, error2 = compile_fn(result2.get("input", {}))
    if not error2:
        return result2, sql2

    return result, None  # 两次都失败，返回第一次结果供上层处理
