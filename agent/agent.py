"""
Forge Agent 核心调度模块。

对外提供三个入口：
    process(user_id, user_text) → AgentResponse   处理用户消息
    approve(user_id)            → AgentResponse   用户确认 SQL
    cancel(user_id)             → AgentResponse   用户取消 SQL

工作模式：
    查询模式：用户提出数据问题
        → LLM 调用 generate_forge_query 工具
        → compile_query() 编译 Forge JSON 为 SQL
        → 若编译失败，将错误反馈给 LLM，最多重试 MAX_RETRIES 次
        → 编译成功后存入 session.pending_sql，返回 sql_review 动作

    定义模式：用户描述业务指标
        → LLM 调用 define_metric 工具
        → _validate_and_save() 校验并持久化到 metrics.registry.yaml
        → 校验失败返回错误，校验通过返回 metric_saved 动作

    文字模式：问候、澄清、闲聊
        → LLM 直接返回文字，不调用任何工具
        → 直接透传 text 内容

AgentResponse.action 值说明：
    message              LLM 文字回复
    clarification        Agent 需要用户补充信息（pending_intent 已记录原始问题）
    sql_review           SQL 已生成，等待用户 approve/cancel
    approved             用户已确认 SQL（approve() 返回）
    cancelled            用户已取消（cancel() 返回）
    metric_saved         指标已保存
    metric_clarification 模型提出指标定义草案，等待用户确认（confirm_metric_definition）
    cache_verified       用户确认查询结果准确（cache_verify() 返回）
    cache_rejected       用户标记查询结果不准确（cache_reject() 返回）
    error                处理失败（含具体错误信息）
"""
from __future__ import annotations
import json
import logging
from datetime import date

import yaml

from config import cfg
from forge.compiler import compile_query
from forge.cache import cache
from registry.validator import validate_metric
from registry.staging_sync import write_staging_record
from agent.memory import memory
from agent import llm

logger = logging.getLogger(__name__)

# 编译失败后最多重试次数（不含首次尝试）
# 设为 2：首次失败 → 第 1 次重试 → 第 2 次重试 → 放弃
MAX_RETRIES = 2


# ── 响应类型 ──────────────────────────────────────────────────────────────────

class AgentResponse:
    """
    Agent 调度结果的统一封装。

    Attributes:
        text:       向用户展示的文字内容（错误信息、确认提示、指标保存通知等）
        sql:        编译后的 SQL 字符串；仅 action=sql_review 或 approved 时有值
        forge_json: 生成 sql 的 Forge JSON 字典；与 sql 同步存在
        action:     当前响应的语义类型，供上层（飞书机器人 / Web API）决定展示方式
    """

    def __init__(
        self,
        text:       str         = "",
        sql:        str | None  = None,
        forge_json: dict | None = None,
        action:     str         = "message",
    ):
        self.text       = text
        self.sql        = sql
        self.forge_json = forge_json
        self.action     = action


# ── 主入口 ────────────────────────────────────────────────────────────────────

def process(user_id: str, user_text: str) -> AgentResponse:
    """
    处理用户发送的一条消息，返回 AgentResponse。

    记忆流：
        EMS.record(user) → WMB.build(scene) → LLM → EMS.record(assistant)
        状态变更通过 memory.set_state / clear_state 管理
    """
    # ── 澄清轮次：若有待补充的 intent，将用户回复合并入原始问题后继续 ──────────
    pending_intent = memory.get_state(user_id, "pending_intent")
    if pending_intent is not None:
        memory.clear_state(user_id, "pending_intent")
        enriched = f"{pending_intent.get('original_question', '')}（补充说明：{user_text}）"
        memory.record(user_id, "user", enriched)
    else:
        # ── 首次消息：检测是否需要发起澄清 ──────────────────────────────────
        clarification = _check_clarification_needed(user_text)
        if clarification:
            memory.set_state(user_id, "pending_intent", {
                "original_question": user_text,
                "clarification_prompt": clarification["prompt"],
                "ambiguity_keys": clarification["keys"],
            })
            memory.record(user_id, "user", user_text)
            memory.record(user_id, "assistant", clarification["prompt"])
            return AgentResponse(text=clarification["prompt"], action="clarification")
        memory.record(user_id, "user", user_text)

    # ── 带编译重试的 agent loop ───────────────────────────────────────────────
    # 重试期间的临时消息用 retry_messages 维护，不写入 EMS（避免污染）
    retry_messages: list[dict] = []

    # 数据权限：查询该用户所属团队的可见表白名单
    from agent.tenant import tenants as _tenants
    _allowed_tables = _tenants.get_allowed_tables_for_user(user_id)

    for attempt in range(1 + MAX_RETRIES):
        # 从 WMB 构建基础消息 + 拼接重试上下文 + 上一轮用过的表
        messages, knowledge, extra_tables = memory.build("query", user_id, user_text)
        if retry_messages:
            messages = messages + retry_messages
        result = llm.call(
            messages, knowledge_context=knowledge,
            extra_tables=extra_tables, allowed_tables=_allowed_tables,
        )

        # ── 文字回复：无工具调用，直接透传 ───────────────────────────────────
        if result["tool"] is None:
            text = result.get("text", "")
            memory.record(user_id, "assistant", text)
            return AgentResponse(text=text)

        # ── 查询模式：生成 Forge JSON 并编译 ─────────────────────────────────
        if result["tool"] == "generate_forge_query":
            forge_json = result["input"]
            try:
                sql = compile_query(forge_json)
            except Exception as exc:
                if attempt < MAX_RETRIES:
                    # 重试消息不写 EMS，只在本次调用内传递
                    retry_messages.append(
                        {"role": "assistant", "content": json.dumps(forge_json, ensure_ascii=False)}
                    )
                    retry_messages.append(
                        {"role": "user", "content": f"编译错误（第 {attempt + 1} 次）：{exc}\n请修正。"}
                    )
                    continue
                else:
                    err = f"⚠ 查询生成失败（已重试 {MAX_RETRIES} 次）：{exc}"
                    memory.record(user_id, "assistant", err, action="error")
                    return AgentResponse(text=err, action="error")

            # 编译成功
            memory.record(user_id, "assistant", "",
                          tool_name="generate_forge_query",
                          tool_input=json.dumps(forge_json, ensure_ascii=False),
                          tool_output=sql,
                          action="sql_review")

            # 存入 pending 状态
            memory.set_state(user_id, "pending_sql", sql)
            memory.set_state(user_id, "pending_forge", forge_json)
            return AgentResponse(sql=sql, forge_json=forge_json, action="sql_review")

        # ── 提案模式：模型猜测指标定义，等待用户确认 ─────────────────────────
        if result["tool"] == "propose_metric_definition":
            proposal = result["input"]
            summary  = proposal.pop("proposal_summary", "")
            memory.set_state(user_id, "pending_metric_proposal", proposal)
            text = (
                f"📋 **指标定义提案**\n\n"
                f"我根据数据库结构，推测了以下定义：\n\n"
                f"{summary}\n\n"
                "如确认无误，请回复「确认」；如需调整，请直接描述修改意见。"
            )
            memory.record(user_id, "assistant", text, action="metric_clarification")
            return AgentResponse(text=text, action="metric_clarification")

        # ── 定义模式：提取指标并校验保存 ─────────────────────────────────────
        if result["tool"] == "define_metric":
            metric = result["input"]
            name   = metric.get("name", "")
            validation_err, validation_warn = _validate_and_save(metric, name)

            if validation_err:
                err_text = (
                    "⚠ 指标定义校验未通过，未保存：\n"
                    + "\n".join(f"• {e}" for e in validation_err)
                    + "\n\n请修正后重新定义。"
                )
                memory.record(user_id, "assistant", err_text, action="error")
                return AgentResponse(text=err_text, action="error")

            label = metric.get("label", metric.get("name", ""))
            text  = f"✅ 已保存指标「{label}」：{metric.get('description', '')}"
            if validation_warn:
                text += "\n\n" + "\n".join(f"⚠ {w}" for w in validation_warn)
            memory.record(user_id, "assistant", text, action="metric_saved")
            return AgentResponse(text=text, action="metric_saved")

        # ── 未知工具（防御性处理）─────────────────────────────────────────────
        return AgentResponse(text="未知操作", action="error")

    # 退出 loop（理论上不可达，for/continue 结构保证在 loop 内 return）
    return AgentResponse(text="查询生成失败，请换一种方式提问。", action="error")


# ── SQL 确认 / 取消 ───────────────────────────────────────────────────────────

def approve(user_id: str) -> AgentResponse:
    """
    用户确认 pending SQL，将其从 session 中取出并返回。

    同时将查询写入缓存（pending 状态），等待 Stage 2 用户对结果的准确性反馈。

    Args:
        user_id: 飞书 open_id。

    Returns:
        action=approved 并携带 sql；若无 pending SQL 则返回 action=error。
    """
    sql = memory.get_state(user_id, "pending_sql")
    forge_json = memory.get_state(user_id, "pending_forge")
    if not sql:
        return AgentResponse(text="没有待确认的 SQL。", action="error")

    memory.clear_state(user_id, "pending_sql")
    memory.clear_state(user_id, "pending_forge")

    # Stage 1 完成：写入缓存
    if forge_json:
        question = _last_user_question(user_id)
        cache_id = cache.add_pending(
            question=question, question_emb=None,
            forge_json=forge_json, sql=sql,
        )
        if cache_id:
            memory.set_state(user_id, "pending_cache_id", cache_id)

    memory.record(user_id, "assistant", "", action="approved")
    # SMP 实时提炼：记录查询模式
    question = _last_user_question(user_id)
    memory.extractor.on_approve(user_id, sql, forge_json or {}, question)
    memory.extractor.update_user_profile(user_id, question)
    return AgentResponse(text="✅ SQL 已确认，开始执行。", sql=sql, action="approved")


def _last_user_question(user_id: str) -> str:
    """从 EMS 中取出最后一条真实用户提问。"""
    messages = memory.ems.get_recent_messages(user_id, limit=5, roles=("user",))
    for msg in reversed(messages):
        content = msg.get("content", "")
        if content and not content.startswith("[系统]") and not content.startswith("编译错误"):
            return content
    return ""


def cancel(user_id: str) -> AgentResponse:
    """用户取消 pending SQL。"""
    cancelled_sql = memory.get_state(user_id, "pending_sql") or ""
    memory.clear_state(user_id, "pending_sql")
    memory.clear_state(user_id, "pending_forge")
    memory.record(user_id, "assistant", "已取消。", action="cancelled")
    # SMP 实时提炼：记录纠错候选
    memory.extractor.on_cancel(user_id, cancelled_sql)
    return AgentResponse(text="已取消。", action="cancelled")


# ── 查询结果准确性反馈（Stage 2）─────────────────────────────────────────────

def cache_verify(user_id: str) -> AgentResponse:
    """
    Stage 2 👍：用户确认查询结果准确，将缓存条目升级为 verified。

    verified 条目可在后续问题中通过 embedding 相似度命中，直接复用 SQL。
    高频 verified 条目还可作为语义层指标的候选定义（suggest_metrics）。
    """
    cache_id = memory.get_state(user_id, "pending_cache_id")
    if not cache_id:
        return AgentResponse(text="没有待反馈的查询缓存。", action="error")
    memory.clear_state(user_id, "pending_cache_id")
    cache.verify(cache_id)
    # SMP 实时提炼：提升置信度 + 写入 org
    memory.extractor.on_cache_verify(user_id)
    return AgentResponse(
        text="✅ 已记录，该查询已加入缓存，下次相似问题可直接复用。",
        action="cache_verified",
    )


def cache_reject(user_id: str) -> AgentResponse:
    """Stage 2 👎：用户标记查询结果不准确。"""
    cache_id = memory.get_state(user_id, "pending_cache_id")
    if not cache_id:
        return AgentResponse(text="没有待反馈的查询缓存。", action="error")
    memory.clear_state(user_id, "pending_cache_id")
    cache.reject(cache_id)
    memory.extractor.on_cache_reject(user_id)
    return AgentResponse(
        text="已记录，感谢反馈，该查询结果不会被缓存。",
        action="cache_rejected",
    )


# ── 指标提案确认 / 拒绝 ──────────────────────────────────────────────────────

def confirm_metric_definition(user_id: str) -> AgentResponse:
    """用户确认模型提出的指标定义草案。"""
    proposal = memory.get_state(user_id, "pending_metric_proposal")
    if not proposal:
        return AgentResponse(text="没有待确认的指标定义。", action="error")

    name = proposal.get("name", "")
    validation_err, validation_warn = _validate_and_save(proposal, name)
    memory.clear_state(user_id, "pending_metric_proposal")

    if validation_err:
        err_text = (
            "⚠ 指标定义校验未通过，未保存：\n"
            + "\n".join(f"• {e}" for e in validation_err)
            + "\n\n请修正后重新定义。"
        )
        memory.record(user_id, "assistant", err_text, action="error")
        return AgentResponse(text=err_text, action="error")

    label = proposal.get("label", name)
    text  = f"✅ 已保存指标「{label}」，下次查询时直接可用。"
    if validation_warn:
        text += "\n\n" + "\n".join(f"⚠ {w}" for w in validation_warn)
    memory.record(user_id, "assistant", text, action="metric_saved")
    return AgentResponse(text=text, action="metric_saved")


def reject_metric_proposal(user_id: str) -> AgentResponse:
    """用户拒绝指标定义草案。"""
    memory.clear_state(user_id, "pending_metric_proposal")
    return AgentResponse(text="已取消指标提案，请重新描述您的需求。", action="cancelled")


# ── Staging 写入 ─────────────────────────────────────────────────────────────

def _maybe_write_staging(user_id: str, user_text: str, forge_json: dict) -> None:
    """
    若本次查询是由澄清轮次驱动，将用户澄清记录写入 staging。
    从 EMS 中检测是否有澄清对话模式。
    """
    messages = memory.ems.get_recent_messages(
        user_id, limit=6, roles=("user", "assistant")
    )
    if len(messages) < 3:
        return

    # 找澄清提示
    clarification_prompt = ""
    for i in range(len(messages) - 1, 0, -1):
        if messages[i]["role"] == "user" and messages[i - 1]["role"] == "assistant":
            prev = messages[i - 1]["content"]
            if any(kw in prev for kw in ["请问", "补充", "指的是", "是否", "请确认"]):
                clarification_prompt = prev
                break

    if not clarification_prompt:
        return

    # 提取原始问题
    current_q = ""
    for msg in reversed(messages):
        if msg["role"] == "user" and "补充说明" in msg["content"]:
            parts = msg["content"].split("（补充说明：")
            current_q = parts[0].strip()
            break

    if not current_q:
        return

    try:
        import re as _re
        key = _re.sub(r'[^\w]', '_', current_q[:30]).strip('_').lower()
        write_staging_record(
            staging_dir=cfg.STAGING_DIR,
            key=f"user_confirmed_{key}",
            label=f"用户确认：{current_q[:20]}...",
            triggers=current_q.split()[:5],
            context=f"用户补充说明：{user_text}",
            original_question=current_q,
            clarification_prompt=clarification_prompt,
            user_response=user_text,
            ambiguity_keys=[],
        )
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Failed to write staging record: %s", exc)


# ── 澄清检测 ──────────────────────────────────────────────────────────────────

def _check_clarification_needed(question: str) -> dict | None:
    """
    检查用户问题是否触发了需要澄清的歧义规则（requires_clarification=true）。

    读取 disambiguations.registry.yaml，对 requires_clarification=true 的条目做
    触发词匹配。若命中，返回澄清问题文本和触发的规则 key；否则返回 None。

    Returns:
        {"prompt": "澄清问题文本", "keys": ["rule_key_1", ...]}
        或 None（无需澄清时）
    """
    try:
        disambiguations: dict = yaml.safe_load(cfg.DISAMBIGUATIONS_PATH.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.debug("Disambiguations not available for clarification check: %s", exc)
        return None

    q_lower = question.lower()
    matched_keys: list[str] = []
    clarification_parts: list[str] = []

    for key, rule in disambiguations.items():
        if not rule.get("requires_clarification", False):
            continue
        triggers = rule.get("triggers", [])
        if any(str(t).lower() in q_lower for t in triggers):
            matched_keys.append(key)
            clarification = rule.get("clarification_question", "")
            if clarification:
                clarification_parts.append(clarification)

    if not matched_keys:
        return None

    if clarification_parts:
        prompt = "\n".join(clarification_parts)
    else:
        prompt = "为了给您准确的结果，请补充以下信息：\n" + "\n".join(
            f"• {disambiguations[k].get('label', k)}：{disambiguations[k].get('context', '').splitlines()[0]}"
            for k in matched_keys
        )

    return {"prompt": prompt, "keys": matched_keys}


# ── 注册表辅助 ────────────────────────────────────────────────────────────────

def _validate_and_save(metric: dict, name: str) -> tuple[list[str], list[str]]:
    """
    校验指标定义，通过后持久化到 metrics.registry.yaml。

    校验步骤：
        1. 读取 schema.registry.json（结构层，用于字段合法性检查）
        2. 读取 metrics.registry.yaml（现有指标，用于衍生指标的引用检查）
        3. 调用 validate_metric()，获取 errors 和 warnings
        4. 若无 errors，将指标写入 YAML 文件（覆盖同名指标）

    写入时：
        - 移除 "name" 字段（name 是 YAML 的 key，不应冗余存储在 value 中）
        - 注入 updated_at 字段（ISO 日期，便于审计）
        - 过滤空值（None、""、[]、{}），保持 YAML 简洁

    Args:
        metric: LLM define_metric 工具返回的原始字典（含 name 字段）。
        name:   指标的唯一标识符（snake_case）。

    Returns:
        (errors, warnings) 元组；errors 非空时调用方不应信任写入已发生。
    """
    # 读取结构层（容错：文件不存在时使用空字典，跳过字段合法性检查）
    try:
        structural = json.loads(cfg.REGISTRY_PATH.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError) as exc:
        logger.debug("Schema registry not available for validation: %s", exc)
        structural = {}

    # 读取现有指标列表（用于衍生指标的引用检查）
    path = cfg.METRICS_PATH
    try:
        existing: dict = yaml.safe_load(path.read_text()) or {}
    except (FileNotFoundError, OSError, yaml.YAMLError) as exc:
        logger.debug("Metrics registry not available for validation: %s", exc)
        existing = {}

    result = validate_metric(metric, structural, metric_name=name, all_metrics=existing)
    if not result.valid:
        # 校验失败，不写文件
        return result.errors, result.warnings

    # 清理后写入
    metric.pop("name", None)                    # name 已作为 YAML key，不重复存储
    metric["updated_at"] = str(date.today())    # 记录最后更新日期
    # 过滤空值字段，使 YAML 文件更整洁
    entry = {k: v for k, v in metric.items() if v not in (None, "", [], {})}

    existing[name] = entry
    path.write_text(
        yaml.dump(existing, allow_unicode=True, sort_keys=False, default_flow_style=False)
    )
    return [], result.warnings
