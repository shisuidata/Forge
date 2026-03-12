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
    message       LLM 文字回复
    sql_review    SQL 已生成，等待用户 approve/cancel
    approved      用户已确认 SQL（approve() 返回）
    cancelled     用户已取消（cancel() 返回）
    metric_saved  指标已保存
    error         处理失败（含具体错误信息）
"""
from __future__ import annotations
import json
from datetime import date

import yaml

from config import cfg
from forge.compiler import compile_query
from registry.validator import validate_metric
from agent.session import store
from agent import llm

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

            # 编译成功：存入 session 的 pending 槽位，等待用户 approve/cancel
            session.pending_sql   = sql
            session.pending_forge = forge_json
            session.add("assistant", f"[SQL ready for review]\n{sql}")
            return AgentResponse(sql=sql, forge_json=forge_json, action="sql_review")

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

    调用方负责实际的 SQL 执行（数据库驱动、权限控制等不在 Agent 范围内）。
    取出后立即清空 pending 状态，防止重复确认。

    Args:
        user_id: 飞书 open_id。

    Returns:
        action=approved 并携带 sql；若无 pending SQL 则返回 action=error。
    """
    session = store.get(user_id)
    sql = session.pending_sql
    if not sql:
        return AgentResponse(text="没有待确认的 SQL。", action="error")
    # 清空 pending 状态，保证状态机的单向流转
    session.pending_sql   = None
    session.pending_forge = None
    return AgentResponse(text="✅ SQL 已确认，开始执行。", sql=sql, action="approved")


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
    except Exception:
        structural = {}

    # 读取现有指标列表（用于衍生指标的引用检查）
    path = cfg.METRICS_PATH
    try:
        existing: dict = yaml.safe_load(path.read_text()) or {}
    except Exception:
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
