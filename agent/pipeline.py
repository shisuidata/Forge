"""
Pipeline 执行引擎 — 多 Agent 线性编排。

概念：
    Pipeline  = 有序的 Stage 列表
    Stage     = 一个独立的处理阶段（对应一个 Agent 角色）
    Artifact  = 阶段间传递的结构化数据

用法：
    from agent.pipeline import router, runner

    pipeline_name = router.route("分析各城市订单趋势")  # → "analyze"
    result = runner.run(pipeline_name, user_id, "分析各城市订单趋势")
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


# ── Artifact ──────────────────────────────────────────────────────────────────

@dataclass
class Artifact:
    """阶段间传递的结构化数据。Schema-on-Read，version 字段保证向后兼容。"""
    _version: int = 1
    _type: str = ""
    _stage: str = ""
    _created_at: str = ""

    def to_dict(self) -> dict:
        d = {k: v for k, v in self.__dict__.items()}
        if not d.get("_created_at"):
            d["_created_at"] = datetime.now(timezone.utc).isoformat()
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Artifact":
        obj = cls()
        for k, v in d.items():
            setattr(obj, k, v)
        return obj

    def to_prompt(self) -> str:
        """将 Artifact 格式化为 LLM 可读的文本（注入 messages）。"""
        parts = []
        d = self.to_dict()
        for k, v in d.items():
            if k.startswith("_"):
                continue
            if isinstance(v, (list, dict)):
                parts.append(f"{k}: {json.dumps(v, ensure_ascii=False)}")
            elif v is not None and v != "":
                parts.append(f"{k}: {v}")
        return "\n".join(parts)


@dataclass
class QueryResult(Artifact):
    _version: int = 1
    _type: str = "query_result"
    sql: str = ""
    columns: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    row_count: int = 0
    forge_json: dict = field(default_factory=dict)

    def to_prompt(self) -> str:
        # 给下游 Agent 看：SQL + 前 20 行数据
        lines = [f"SQL: {self.sql}", f"列: {', '.join(self.columns)}", f"总行数: {self.row_count}", ""]
        for row in self.rows[:20]:
            lines.append(" | ".join(str(v) for v in row))
        if self.row_count > 20:
            lines.append(f"... 共 {self.row_count} 行，仅展示前 20 行")
        return "\n".join(lines)


@dataclass
class AnalysisReport(Artifact):
    _version: int = 1
    _type: str = "analysis_report"
    status: str = "complete"
    summary: str = ""
    insights: list = field(default_factory=list)
    key_metrics: dict = field(default_factory=dict)
    trend_direction: str = ""
    anomalies: list = field(default_factory=list)
    recommendations: list = field(default_factory=list)
    needs: str | None = None
    suggested_query: str | None = None


@dataclass
class ChartSpec(Artifact):
    _version: int = 1
    _type: str = "chart_spec"
    chart_type: str = ""
    title: str = ""
    config: dict = field(default_factory=dict)
    annotations: list = field(default_factory=list)


# ── Stage ─────────────────────────────────────────────────────────────────────

@dataclass
class Stage:
    name: str
    agent: str
    scene: str = "query"
    requires_approval: bool = False
    timeout_sec: int = 60


# ── Pipeline 定义 ─────────────────────────────────────────────────────────────

PIPELINES: dict[str, list[Stage]] = {
    "query": [
        Stage("generate", agent="forge_query", scene="query", requires_approval=True),
    ],
    "analyze": [
        Stage("generate", agent="forge_query", scene="query", requires_approval=True),
        Stage("analyze",  agent="data_analyst", scene="analyze"),
    ],
    "visualize": [
        Stage("generate", agent="forge_query", scene="query", requires_approval=True),
        Stage("chart",    agent="viz_designer", scene="visualize"),
    ],
    "report": [
        Stage("generate", agent="forge_query", scene="query", requires_approval=True),
        Stage("analyze",  agent="data_analyst", scene="analyze"),
        Stage("chart",    agent="viz_designer", scene="visualize"),
        Stage("summary",  agent="report_writer", scene="admin"),
    ],
}


# ── Pipeline 执行结果 ─────────────────────────────────────────────────────────

@dataclass
class StageRun:
    stage: str
    agent: str
    status: str = "pending"         # pending / running / completed / failed / skipped
    started_at: str = ""
    ended_at: str = ""
    duration_ms: int = 0
    artifact: Artifact | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        d = {
            "stage": self.stage, "agent": self.agent, "status": self.status,
            "started_at": self.started_at, "ended_at": self.ended_at,
            "duration_ms": self.duration_ms, "error": self.error,
        }
        if self.artifact:
            d["artifact"] = self.artifact.to_dict()
        return d


@dataclass
class PipelineRun:
    run_id: str = ""
    pipeline: str = ""
    user_id: str = ""
    team_id: str = ""
    question: str = ""
    status: str = "running"         # running / completed / failed / pending_approval
    stages: list[StageRun] = field(default_factory=list)
    current_stage: int = 0
    started_at: str = ""
    ended_at: str = ""

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id, "pipeline": self.pipeline,
            "user_id": self.user_id, "team_id": self.team_id,
            "question": self.question, "status": self.status,
            "current_stage": self.current_stage,
            "started_at": self.started_at, "ended_at": self.ended_at,
            "stages": [s.to_dict() for s in self.stages],
        }


# ── 意图路由 ──────────────────────────────────────────────────────────────────

INTENT_KEYWORDS: dict[str, list[str]] = {
    "report":    ["分析报告", "出报告", "生成报告", "详细分析", "完整分析"],
    "analyze":   ["分析", "为什么", "原因", "趋势", "对比", "同比", "环比", "变化", "下滑", "增长", "异常"],
    "visualize": ["画图", "图表", "可视化", "柱状图", "折线图", "饼图", "热力图"],
}


class IntentRouter:
    """意图路由：判断用户问题应该走哪条 Pipeline。"""

    def route(self, question: str) -> str:
        """关键词匹配路由。返回 pipeline 名。"""
        q = question.lower()
        for pipeline, keywords in INTENT_KEYWORDS.items():
            if any(kw in q for kw in keywords):
                return pipeline
        return "query"

    def route_with_llm(self, question: str) -> str:
        """LLM 路由（更准确但多一次调用）。"""
        try:
            from agent import llm
            result = llm.call(
                [{"role": "user", "content": question}],
                system_override=(
                    "判断用户意图，只返回一个词：query / analyze / visualize / report\n"
                    "query=查数据, analyze=分析原因/趋势, visualize=画图, report=完整分析报告"
                ),
            )
            text = result.get("text", "query").strip().lower()
            if text in PIPELINES:
                return text
        except Exception:
            pass
        return self.route(question)


# ── Pipeline Runner ───────────────────────────────────────────────────────────

class PipelineRunner:
    """
    Pipeline 执行引擎。

    按顺序执行 Stage，在 requires_approval 阶段暂停。
    每步记录到 EMS，支持从断点恢复。
    """

    def __init__(self):
        from agent.memory import memory
        self._memory = memory

    def run(self, pipeline_name: str, user_id: str, question: str) -> PipelineRun:
        """
        启动 Pipeline。对于 query pipeline，直接代理给 agent.process()。
        """
        if pipeline_name not in PIPELINES:
            pipeline_name = "query"

        stages = PIPELINES[pipeline_name]
        now = datetime.now(timezone.utc).isoformat()
        run = PipelineRun(
            run_id=f"pr_{uuid.uuid4().hex[:12]}",
            pipeline=pipeline_name,
            user_id=user_id,
            question=question,
            started_at=now,
            stages=[StageRun(stage=s.name, agent=s.agent) for s in stages],
        )

        # 获取 team_id
        try:
            from agent.tenant import tenants
            run.team_id = tenants.get_team(user_id)
        except Exception:
            pass

        # 保存 pipeline context 到 EMS state
        self._memory.set_state(user_id, "pipeline_run", run.to_dict())
        logger.info("Pipeline started: %s/%s for user=%s", pipeline_name, run.run_id, user_id)

        return self._execute_from(run, stages, start_idx=0)

    def resume(self, user_id: str) -> PipelineRun | None:
        """从断点恢复 Pipeline（用户 approve 后调用）。"""
        run_data = self._memory.get_state(user_id, "pipeline_run")
        if not run_data:
            return None

        run = PipelineRun(**{k: v for k, v in run_data.items() if k != "stages"})
        run.stages = [StageRun(**s) for s in run_data.get("stages", [])]

        if run.pipeline not in PIPELINES:
            return None

        stages = PIPELINES[run.pipeline]
        return self._execute_from(run, stages, start_idx=run.current_stage)

    def _execute_from(self, run: PipelineRun, stages: list[Stage], start_idx: int) -> PipelineRun:
        """从指定阶段开始执行。"""
        last_artifact: Artifact | None = None

        # 恢复之前阶段的 artifact
        for i in range(start_idx):
            sr = run.stages[i] if i < len(run.stages) else None
            if sr and sr.artifact:
                last_artifact = Artifact.from_dict(sr.artifact) if isinstance(sr.artifact, dict) else sr.artifact

        for idx in range(start_idx, len(stages)):
            stage = stages[idx]
            sr = run.stages[idx]
            sr.status = "running"
            sr.started_at = datetime.now(timezone.utc).isoformat()
            run.current_stage = idx
            t0 = time.time()

            try:
                artifact = self._execute_stage(stage, run, last_artifact)
                sr.status = "completed"
                sr.artifact = artifact
                last_artifact = artifact

                # 检查是否需要用户确认
                if stage.requires_approval:
                    run.status = "pending_approval"
                    run.current_stage = idx + 1   # 下次从下一阶段恢复
                    self._memory.set_state(run.user_id, "pipeline_run", run.to_dict())
                    sr.ended_at = datetime.now(timezone.utc).isoformat()
                    sr.duration_ms = int((time.time() - t0) * 1000)
                    return run

                # 检查分析是否 incomplete（1 次自动补查机制）
                if (isinstance(artifact, AnalysisReport)
                        and artifact.status == "incomplete"
                        and artifact.suggested_query):
                    run.status = "incomplete"
                    self._memory.set_state(run.user_id, "pipeline_run", run.to_dict())
                    sr.ended_at = datetime.now(timezone.utc).isoformat()
                    sr.duration_ms = int((time.time() - t0) * 1000)
                    return run

            except Exception as exc:
                sr.status = "failed"
                sr.error = str(exc)
                run.status = "failed"
                logger.warning("Stage %s failed: %s", stage.name, exc)
                break
            finally:
                sr.ended_at = sr.ended_at or datetime.now(timezone.utc).isoformat()
                sr.duration_ms = sr.duration_ms or int((time.time() - t0) * 1000)

        if run.status == "running":
            run.status = "completed"
            run.ended_at = datetime.now(timezone.utc).isoformat()

        # 清理 pipeline state
        if run.status in ("completed", "failed"):
            self._memory.clear_state(run.user_id, "pipeline_run")

        # 记录到 EMS
        self._memory.record(
            run.user_id, "tool",
            tool_name="pipeline_complete",
            tool_input=json.dumps({"pipeline": run.pipeline, "run_id": run.run_id}),
            tool_output=json.dumps(run.to_dict(), ensure_ascii=False, default=str),
            action="pipeline_" + run.status,
        )

        return run

    def _execute_stage(self, stage: Stage, run: PipelineRun, input_artifact: Artifact | None) -> Artifact:
        """执行单个 Stage。"""
        from agent import agent as agent_module

        if stage.agent == "forge_query":
            # 复用现有 agent.process()
            resp = agent_module.process(run.user_id, run.question)
            if resp.action == "sql_review" and resp.sql:
                return QueryResult(
                    _stage=stage.name,
                    sql=resp.sql,
                    forge_json=resp.forge_json or {},
                )
            elif resp.action == "error":
                raise RuntimeError(resp.text)
            else:
                # 文字回复（没有生成 SQL）
                return Artifact(_stage=stage.name, _type="text_response")

        elif stage.agent == "data_analyst":
            return self._run_analysis(run, input_artifact)

        elif stage.agent == "viz_designer":
            return self._run_visualization(run, input_artifact)

        elif stage.agent == "report_writer":
            return self._run_report(run, input_artifact)

        else:
            raise ValueError(f"Unknown agent: {stage.agent}")

    def _run_analysis(self, run: PipelineRun, input_artifact: Artifact | None) -> AnalysisReport:
        """执行分析阶段。"""
        from agent import llm

        if not input_artifact:
            return AnalysisReport(status="incomplete", summary="无输入数据", needs="需要先查询数据")

        # WMB 构建（analyze scene 自动注入业务上下文 + SMP 知识）
        messages, knowledge = self._memory.build("analyze", run.user_id, run.question)

        # 注入查询结果
        messages.append({
            "role": "user",
            "content": f"请分析以下查询结果：\n\n{input_artifact.to_prompt()}\n\n用户问题：{run.question}",
        })

        system = """你是一位资深数据分析师。根据查询结果和业务上下文，生成结构化分析报告。

分析要求：
1. 先给出一句话核心发现（summary）
2. 列出 3-5 条关键洞察（insights），每条一句话，用数据支撑
3. 提取关键指标（key_metrics），如增长率、占比、排名变化等
4. 判断趋势方向（trend_direction）：up / down / stable / mixed
5. 标注异常数据点（anomalies），说明为什么异常
6. 给出 1-3 条可行建议（recommendations）

重要：
- 结合业务上下文判断数值好坏（参考阈值标准、日历事件、行业基准）
- 不要只描述数据，要给出业务含义和判断
- 如果数据粒度不够（如只有年度汇总无法分析月度趋势），标注 status=incomplete 并给出 suggested_query
- 用 JSON 格式回复（不要加代码块标记）

JSON 格式：
{"status": "complete", "summary": "...", "insights": ["..."], "key_metrics": {"指标名": 值}, "trend_direction": "up|down|stable|mixed", "anomalies": ["..."], "recommendations": ["..."], "needs": null, "suggested_query": null}"""

        if knowledge:
            system += "\n\n" + knowledge

        result = llm.call(messages, system_override=system)
        text = result.get("text", "")

        # 尝试解析为结构化输出
        try:
            import re
            m = re.search(r'\{[\s\S]+\}', text)
            if m:
                data = json.loads(m.group())
                return AnalysisReport(
                    _stage="analyze",
                    status=data.get("status", "complete"),
                    summary=data.get("summary", ""),
                    insights=data.get("insights", []),
                    key_metrics=data.get("key_metrics", {}),
                    trend_direction=data.get("trend_direction", ""),
                    anomalies=data.get("anomalies", []),
                    recommendations=data.get("recommendations", []),
                    needs=data.get("needs"),
                    suggested_query=data.get("suggested_query"),
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        return AnalysisReport(_stage="analyze", summary=text)

    def _run_visualization(self, run: PipelineRun, input_artifact: Artifact | None) -> ChartSpec:
        """执行可视化阶段：LLM 生成 ChartSpec，包含图表类型、标注、高亮。"""
        from agent import llm

        # 收集可用信息
        data_prompt = ""
        analysis_context = ""
        if isinstance(input_artifact, QueryResult):
            data_prompt = input_artifact.to_prompt()
        elif isinstance(input_artifact, AnalysisReport):
            analysis_context = (
                f"分析结论：{input_artifact.summary}\n"
                f"趋势：{input_artifact.trend_direction}\n"
                f"异常点：{', '.join(input_artifact.anomalies)}\n"
            )
            # 往前找 QueryResult
            for sr in run.stages:
                if isinstance(sr.artifact, QueryResult):
                    data_prompt = sr.artifact.to_prompt()
                    break
                elif isinstance(sr.artifact, dict) and sr.artifact.get("_type") == "query_result":
                    qr = QueryResult.from_dict(sr.artifact)
                    data_prompt = qr.to_prompt()
                    break

        if not data_prompt:
            from forge.chart import _recommend
            return ChartSpec(_stage="chart", chart_type="bar", title=run.question[:40])

        messages = [{
            "role": "user",
            "content": (
                f"为以下数据设计最佳可视化方案：\n\n"
                f"{data_prompt}\n\n"
                f"用户问题：{run.question}\n"
                f"{analysis_context}"
            ),
        }]

        system = """你是一位数据可视化设计师。根据数据和分析结论，选择最佳图表类型并设计标注。

规则：
- 时间序列数据 → 折线图（line）
- 分类对比 → 柱状图（bar），≤8 个类别且是占比 → 饼图（pie）
- 相关性 → 散点图（scatter）
- 如果有异常数据点或分析标记的重点城市/指标，添加到 annotations
- annotations 格式：[{"name": "北京", "note": "环比下滑23%", "highlight": true}]

用 JSON 格式回复（不要代码块标记）：
{"chart_type": "bar|line|pie|scatter", "title": "图表标题", "annotations": [...], "config_hints": {"sort": "desc", "show_avg_line": true}}"""

        result = llm.call(messages, system_override=system)
        text = result.get("text", "")

        try:
            import re
            m = re.search(r'\{[\s\S]+\}', text)
            if m:
                data = json.loads(m.group())
                return ChartSpec(
                    _stage="chart",
                    chart_type=data.get("chart_type", "bar"),
                    title=data.get("title", run.question[:40]),
                    annotations=data.get("annotations", []),
                    config=data.get("config_hints", {}),
                )
        except (json.JSONDecodeError, AttributeError):
            pass

        # 降级：规则推荐
        from forge.chart import _recommend
        if isinstance(input_artifact, QueryResult):
            ct = _recommend(input_artifact.columns, input_artifact.rows)
        else:
            ct = "bar"
        return ChartSpec(_stage="chart", chart_type=ct, title=run.question[:40])

    def _run_report(self, run: PipelineRun, input_artifact: Artifact | None) -> Artifact:
        """执行报告生成阶段：汇总所有 Artifact 生成文字报告。"""
        from agent import llm

        # 收集所有阶段的输出
        parts = [f"用户问题：{run.question}\n"]
        for sr in run.stages:
            if sr.artifact and sr.stage != "summary":
                art = sr.artifact if isinstance(sr.artifact, Artifact) else Artifact.from_dict(sr.artifact)
                parts.append(f"--- {sr.stage} 阶段输出 ---\n{art.to_prompt()}\n")

        messages = [{"role": "user", "content": "\n".join(parts)}]
        system = """你是一位数据分析报告撰写者。根据查询结果和分析输出，撰写一份简洁的分析报告。

格式要求：
- 开头一句话核心结论
- 2-3 段正文，每段有小标题
- 用具体数字支撑每个论点
- 最后给出 1-2 条可行建议
- 语言风格：专业、简洁、直接
- 用 Markdown 格式"""

        result = llm.call(messages, system_override=system)
        text = result.get("text", "")

        report = Artifact(_stage="summary", _type="report_text")
        report.content = text  # type: ignore[attr-defined]
        return report


# ── 全局单例 ──────────────────────────────────────────────────────────────────

router = IntentRouter()
runner = PipelineRunner()
