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
from agent.session import store, IntentSpec
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

    流程：
        1. 将用户消息加入 session 历史
        2. 进入带重试的 agent loop：
           a. 调用 LLM，获取工具调用或文字回复
           b. 若为文字回复 → 直接返回
           c. 若为 generate_forge_query → 编译 Forge JSON
              - 编译成功 → 存入 pending_sql，返回 sql_review
              - 编译失败且未超重试上限 → 将错误注入 session，continue 重试
              - 超出重试上限 → 返回 error
           d. 若为 define_metric → 校验并保存指标

    Args:
        user_id:   飞书 open_id，用于从 SessionStore 获取会话状态。
        user_text: 用户发送的原始文本。

    Returns:
        AgentResponse，action 字段指示上层如何处理响应。
    """
    session = store.get(user_id)

    # ── 澄清轮次：若有待补充的 intent，将用户回复合并入原始问题后继续 ──────────
    if session.pending_intent is not None:
        intent = session.pending_intent
        session.pending_intent = None
        # 将用户的补充信息附加到原始问题，构建更完整的查询描述
        enriched = f"{intent.original_question}（补充说明：{user_text}）"
        session.add("user", enriched)
    else:
        # ── 首次消息：检测是否需要发起澄清 ──────────────────────────────────
        clarification = _check_clarification_needed(user_text)
        if clarification:
            # 保存待澄清的 intent，返回澄清问题给用户
            session.pending_intent = IntentSpec(
                original_question=user_text,
                clarification_prompt=clarification["prompt"],
                ambiguity_keys=clarification["keys"],
            )
            session.add("user", user_text)
            session.add("assistant", clarification["prompt"])
            return AgentResponse(text=clarification["prompt"], action="clarification")
        session.add("user", user_text)

    # ── 带编译重试的 agent loop ───────────────────────────────────────────────
    for attempt in range(1 + MAX_RETRIES):
        result = llm.call(session.recent())

        # ── 文字回复：无工具调用，直接透传 ───────────────────────────────────
        if result["tool"] is None:
            text = result.get("text", "")
            session.add("assistant", text)
            return AgentResponse(text=text)

        # ── 查询模式：生成 Forge JSON 并编译 ─────────────────────────────────
        if result["tool"] == "generate_forge_query":
            forge_json = result["input"]
            try:
                sql = compile_query(forge_json)
            except Exception as exc:
                if attempt < MAX_RETRIES:
                    # 将编译错误注入 session，让 LLM 在下一轮自我修正
                    # 先把 LLM 生成的（有问题的）Forge JSON 作为 assistant 消息记录
                    session.add("assistant", json.dumps(forge_json, ensure_ascii=False))
                    err_msg = (
                        f"[系统] 编译错误（第 {attempt + 1} 次尝试）：{exc}\n"
                        "请修正 Forge JSON 后重新生成。"
                    )
                    session.add("user", err_msg)
                    continue   # 回到 loop 顶部，再次调用 LLM
                else:
                    # 已达重试上限，向用户报错
                    err = f"⚠ 查询生成失败（已重试 {MAX_RETRIES} 次）：{exc}"
                    session.add("assistant", err)
                    return AgentResponse(text=err, action="error")

            # 编译成功：若本次查询是由澄清轮次触发的，将确认写入 staging
            _maybe_write_staging(session, user_text, forge_json)

            # 清除重试期间注入的系统消息和 Forge JSON（只保留原始用户提问）
            session.history = [
                m for m in session.history
                if not (m.role == "user" and m.content.startswith("[系统]"))
                and not (m.role == "assistant" and m.content.startswith("{"))
            ]

            # 存入 session 的 pending 槽位，等待用户 approve/cancel
            session.pending_sql   = sql
            session.pending_forge = forge_json
            return AgentResponse(sql=sql, forge_json=forge_json, action="sql_review")

        # ── 提案模式：模型猜测指标定义，等待用户确认 ─────────────────────────
        if result["tool"] == "propose_metric_definition":
            proposal = result["input"]
            summary  = proposal.pop("proposal_summary", "")
            session.pending_metric_proposal = proposal
            text = (
                f"📋 **指标定义提案**\n\n"
                f"我根据数据库结构，推测了以下定义：\n\n"
                f"{summary}\n\n"
                "如确认无误，请回复「确认」；如需调整，请直接描述修改意见。"
            )
            session.add("assistant", text)
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
                session.add("assistant", err_text)
                return AgentResponse(text=err_text, action="error")

            label = metric.get("label", metric.get("name", ""))
            text  = f"✅ 已保存指标「{label}」：{metric.get('description', '')}"
            if validation_warn:
                # 有警告但仍保存成功，将警告信息附在回复末尾
                text += "\n\n" + "\n".join(f"⚠ {w}" for w in validation_warn)
            session.add("assistant", text)
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
    session = store.get(user_id)
    sql        = session.pending_sql
    forge_json = session.pending_forge
    if not sql:
        return AgentResponse(text="没有待确认的 SQL。", action="error")

    # 清空 pending 状态，保证状态机的单向流转
    session.pending_sql   = None
    session.pending_forge = None

    # Stage 1 完成：写入缓存（pending），等待 Stage 2 结果反馈
    if forge_json:
        question = _last_user_question(session)
        cache_id = cache.add_pending(
            question=question,
            question_emb=None,   # 无 embedding 时退化为精确匹配
            forge_json=forge_json,
            sql=sql,
        )
        session.pending_cache_id = cache_id or None

    return AgentResponse(text="✅ SQL 已确认，开始执行。", sql=sql, action="approved")


def _last_user_question(session) -> str:
    """从 session 历史中取出最后一条真实用户提问（跳过系统注入消息）。"""
    for msg in reversed(session.history):
        if msg.role == "user" and not msg.content.startswith("[系统]"):
            return msg.content
    return ""


def cancel(user_id: str) -> AgentResponse:
    """
    用户取消 pending SQL，清空 session 的 pending 状态。

    Args:
        user_id: 飞书 open_id。

    Returns:
        action=cancelled；即使当前无 pending SQL 也安全返回。
    """
    session = store.get(user_id)
    session.pending_sql   = None
    session.pending_forge = None
    return AgentResponse(text="已取消。", action="cancelled")


# ── 查询结果准确性反馈（Stage 2）─────────────────────────────────────────────

def cache_verify(user_id: str) -> AgentResponse:
    """
    Stage 2 👍：用户确认查询结果准确，将缓存条目升级为 verified。

    verified 条目可在后续问题中通过 embedding 相似度命中，直接复用 SQL。
    高频 verified 条目还可作为语义层指标的候选定义（suggest_metrics）。
    """
    session = store.get(user_id)
    cache_id = session.pending_cache_id
    if not cache_id:
        return AgentResponse(text="没有待反馈的查询缓存。", action="error")
    session.pending_cache_id = None
    cache.verify(cache_id)
    return AgentResponse(
        text="✅ 已记录，该查询已加入缓存，下次相似问题可直接复用。",
        action="cache_verified",
    )


def cache_reject(user_id: str) -> AgentResponse:
    """
    Stage 2 👎：用户标记查询结果不准确，将缓存条目软删除为 rejected。
    """
    session = store.get(user_id)
    cache_id = session.pending_cache_id
    if not cache_id:
        return AgentResponse(text="没有待反馈的查询缓存。", action="error")
    session.pending_cache_id = None
    cache.reject(cache_id)
    return AgentResponse(
        text="已记录，感谢反馈，该查询结果不会被缓存。",
        action="cache_rejected",
    )


# ── 指标提案确认 / 拒绝 ──────────────────────────────────────────────────────

def confirm_metric_definition(user_id: str) -> AgentResponse:
    """
    用户确认模型提出的指标定义草案，校验后持久化入库。

    Args:
        user_id: 飞书 open_id。

    Returns:
        action=metric_saved 表示保存成功；action=error 表示校验失败或无待确认项。
    """
    session = store.get(user_id)
    proposal = session.pending_metric_proposal
    if not proposal:
        return AgentResponse(text="没有待确认的指标定义。", action="error")

    name = proposal.get("name", "")
    validation_err, validation_warn = _validate_and_save(proposal, name)
    session.pending_metric_proposal = None   # 清空，无论成功或失败

    if validation_err:
        err_text = (
            "⚠ 指标定义校验未通过，未保存：\n"
            + "\n".join(f"• {e}" for e in validation_err)
            + "\n\n请修正后重新定义。"
        )
        session.add("assistant", err_text)
        return AgentResponse(text=err_text, action="error")

    label = proposal.get("label", name)
    text  = f"✅ 已保存指标「{label}」，下次查询时直接可用。"
    if validation_warn:
        text += "\n\n" + "\n".join(f"⚠ {w}" for w in validation_warn)
    session.add("assistant", text)
    return AgentResponse(text=text, action="metric_saved")


def reject_metric_proposal(user_id: str) -> AgentResponse:
    """
    用户拒绝模型提出的指标定义草案，清空 pending 状态。

    Args:
        user_id: 飞书 open_id。
    """
    session = store.get(user_id)
    session.pending_metric_proposal = None
    return AgentResponse(text="已取消指标提案，请重新描述您的需求。", action="cancelled")


# ── Staging 写入 ─────────────────────────────────────────────────────────────

def _maybe_write_staging(session, user_text: str, forge_json: dict) -> None:
    """
    若本次查询是由澄清轮次驱动（session 中曾有 pending_intent），
    将用户的澄清回复 + 原始问题写入 staging，供后续 sync-staging 合并入 registry。

    在 process() 编译成功时调用。session.pending_intent 此时已被清空，
    因此额外传入 user_text 作为上下文补充。

    只在 session 历史中能检测到澄清对话模式时才写入（避免误触发）。
    """
    # 从 history 中检测：倒数找 action=clarification 的模式
    # 简单启发式：如果倒数第 3 条是 assistant（澄清问），倒数第 2 条是 user（用户回答）
    hist = session.history
    if len(hist) < 3:
        return

    # 找澄清提示（assistant）和用户回答（user）的对
    clarification_prompt = ""
    for i in range(len(hist) - 1, 0, -1):
        if hist[i].role == "user" and hist[i - 1].role == "assistant":
            prev_assistant = hist[i - 1].content
            # 判断是否是澄清提示（包含「补充」「请问」「说明」等词）
            if any(kw in prev_assistant for kw in ["请问", "补充", "指的是", "是否", "请确认"]):
                clarification_prompt = prev_assistant
                break

    if not clarification_prompt:
        return

    # 提取原始问题（enriched 格式：原始问题 + 补充说明：user_text）
    current_q = ""
    for msg in reversed(hist):
        if msg.role == "user" and "补充说明" in msg.content:
            # 格式："{original}（补充说明：{user_text}）"
            parts = msg.content.split("（补充说明：")
            current_q = parts[0].strip()
            break

    if not current_q:
        return

    # 写入 staging
    try:
        import re as _re
        key = _re.sub(r'[^\w]', '_', current_q[:30]).strip('_').lower()
        key = f"user_confirmed_{key}"
        write_staging_record(
            staging_dir=cfg.STAGING_DIR,
            key=key,
            label=f"用户确认：{current_q[:20]}...",
            triggers=current_q.split()[:5],      # 取前5个词作为触发词
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
