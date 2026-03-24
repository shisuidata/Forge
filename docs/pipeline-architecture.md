# Forge Agent Pipeline 架构设计

> 让 Forge 从"SQL 查询工具"进化为"数据分析助手"。
> 本文档持续记录设计讨论过程。

---

## 1. 问题分析

### 1.1 当前能力边界

```
用户提问 → SQL 生成 → 审核 → 执行 → 展示结果
```

用户拿到的是**原始数据**，需要自己判断数据含义。Forge 目前不回答"为什么"和"怎么看"。

### 1.2 目标能力

```
用户提问 → 查询 → 分析 → 可视化 → 洞察报告
```

| 能力 | 输入 | 输出 | 示例 |
|---|---|---|---|
| 查询 | 自然语言 | 结构化数据 | "各城市的订单数" → SQL → rows |
| 分析 | 结构化数据 | 文字洞察 + 关键指标 | "北京订单下滑 23%，是唯一负增长城市" |
| 可视化 | 数据 + 洞察 | 交互图表 + 标注 | 折线图，北京线标红，标注拐点 |

---

## 2. 核心概念

### 2.1 为什么是 Pipeline 不是 Graph

| 模式 | 适用场景 | Forge 情况 |
|---|---|---|
| Pipeline（流水线） | 固定阶段、线性流转、前一步输出是后一步输入 | 查询→分析→可视化，天然线性 |
| Graph（有向图） | 动态分支、循环、多路由、Agent 间对话 | 不需要 Agent 之间来回交互 |

Pipeline 的特点：
- 每个阶段职责单一
- 阶段间通过结构化数据传递（不是自然语言）
- 用户可以在任意阶段确认或中断
- 失败时可以从断点重跑

### 2.2 三个核心概念

```
Pipeline  = 有序的阶段列表
Stage     = 一个独立的处理阶段（对应一个 Agent 角色）
Artifact  = 阶段间传递的结构化数据
```

---

## 3. Pipeline 定义

### 3.1 预置 Pipeline

```python
PIPELINES = {
    # 纯查询（现有能力，默认）
    "query": [
        Stage("generate", agent="forge_query"),
    ],

    # 查询 + 分析
    "analyze": [
        Stage("generate", agent="forge_query"),
        Stage("analyze",  agent="data_analyst"),
    ],

    # 查询 + 可视化
    "visualize": [
        Stage("generate", agent="forge_query"),
        Stage("chart",    agent="viz_designer"),
    ],

    # 完整分析报告
    "report": [
        Stage("generate", agent="forge_query"),
        Stage("analyze",  agent="data_analyst"),
        Stage("chart",    agent="viz_designer"),
        Stage("summary",  agent="report_writer"),
    ],
}
```

### 3.2 Stage 定义

```python
@dataclass
class Stage:
    name: str                    # 阶段名，唯一标识
    agent: str                   # Agent 角色名
    requires_approval: bool = False  # 是否需要用户确认才能进入下一阶段
    timeout_sec: int = 60        # 超时时间
```

### 3.3 Agent 角色

每个 Agent 角色定义了：
- WMB scene（记忆裁剪策略）
- LLM system prompt
- 可用的 tools
- 输入/输出的 Artifact 类型

```python
AGENTS = {
    "forge_query": {
        "scene": "query",
        "tools": ["generate_forge_query"],       # Structured Output
        "input_artifact": None,                   # 首阶段无输入
        "output_artifact": "query_result",        # cols + rows + sql
        "requires_approval": True,                # SQL 需要审核
    },
    "data_analyst": {
        "scene": "analyze",
        "tools": ["generate_analysis"],           # 结构化分析输出
        "input_artifact": "query_result",
        "output_artifact": "analysis_report",     # insights + key_metrics + recommendations
        "requires_approval": False,               # 分析结果直接展示
    },
    "viz_designer": {
        "scene": "visualize",
        "tools": ["generate_chart_spec"],         # 图表规格（不是直接生成图片）
        "input_artifact": "query_result",
        "output_artifact": "chart_spec",          # chart_type + config + annotations
        "requires_approval": False,
    },
    "report_writer": {
        "scene": "admin",                         # 复用 admin scene 的知识注入
        "tools": [],                              # 纯文字输出
        "input_artifact": "analysis_report",
        "output_artifact": "report_text",         # 完整的分析报告文本
        "requires_approval": False,
    },
}
```

---

## 4. Artifact（阶段间数据）

Artifact 是阶段间传递的结构化数据，不是自然语言。

### 4.1 Artifact 类型

```python
@dataclass
class QueryResult:
    """查询阶段输出。"""
    sql: str
    columns: list[str]
    rows: list[tuple]
    row_count: int
    forge_json: dict

@dataclass
class AnalysisReport:
    """分析阶段输出。"""
    summary: str                       # 一句话核心发现
    insights: list[str]                # 关键洞察列表
    key_metrics: dict[str, Any]        # 关键指标 {"增长率": -23%, "最大城市": "上海"}
    recommendations: list[str]         # 建议
    data_ref: QueryResult              # 关联的原始数据

@dataclass
class ChartSpec:
    """可视化阶段输出。"""
    chart_type: str                    # bar / line / pie / scatter / heatmap
    title: str
    config: dict                       # ECharts option 或 pyecharts 参数
    annotations: list[dict]            # 标注点 [{"city": "北京", "note": "下滑23%"}]
    data_ref: QueryResult

@dataclass
class ReportText:
    """报告阶段输出。"""
    title: str
    sections: list[dict]               # [{"heading": "...", "body": "..."}]
    charts: list[ChartSpec]
    data_tables: list[QueryResult]
```

### 4.2 Artifact 在 EMS 中的存储

每个 Artifact 作为一条 EMS 记录，role=tool，tool_output 存 JSON：

```python
memory.record(user_id, "tool",
    tool_name="stage_output",
    tool_input=json.dumps({"stage": "analyze", "pipeline": "report"}),
    tool_output=json.dumps(artifact.__dict__),
    action="stage_complete",
)
```

---

## 5. 执行引擎

### 5.1 PipelineRunner

```python
class PipelineRunner:
    """
    Pipeline 执行引擎。

    职责：
    1. 按顺序执行 Stage
    2. 在 Stage 之间传递 Artifact
    3. 在 requires_approval 阶段暂停等待用户确认
    4. 记录每步到 EMS
    5. 支持从断点恢复
    """

    def run(self, pipeline_name: str, user_id: str, question: str) -> PipelineResult:
        pipeline = PIPELINES[pipeline_name]
        context = PipelineContext(user_id=user_id, question=question)

        for stage in pipeline:
            # 构建当前阶段的 WMB 上下文
            agent_cfg = AGENTS[stage.agent]
            messages, knowledge = memory.build(
                agent_cfg["scene"], user_id, question
            )

            # 注入前一阶段的 Artifact
            if context.last_artifact:
                messages.append({
                    "role": "user",
                    "content": self._format_artifact_prompt(context.last_artifact),
                })

            # 调用 LLM
            result = llm.call(messages, knowledge_context=knowledge, ...)

            # 解析输出为 Artifact
            artifact = self._parse_artifact(result, agent_cfg["output_artifact"])
            context.artifacts[stage.name] = artifact
            context.last_artifact = artifact

            # 记录到 EMS
            memory.record(user_id, "tool", ...)

            # 需要用户确认？
            if stage.requires_approval:
                return PipelineResult(
                    status="pending_approval",
                    stage=stage.name,
                    artifact=artifact,
                    context=context,
                )

        return PipelineResult(status="complete", artifacts=context.artifacts)
```

### 5.2 用户确认与断点恢复

```
用户："分析各城市订单趋势"
    ↓
Pipeline "analyze" 启动
    ↓
Stage 1: forge_query → 生成 SQL
    ↓ requires_approval=True
返回给用户：SQL 预览 + [确认执行] [取消]
    ↓ 用户点击确认
Pipeline 从 Stage 2 恢复
    ↓
Stage 2: data_analyst → 分析数据
    ↓
返回：分析结论 + 数据表格
```

断点恢复的关键：`PipelineContext` 存入 EMS 的状态事件中。

```python
memory.set_state(user_id, "pipeline_context", {
    "pipeline": "analyze",
    "current_stage": 1,
    "artifacts": {...},
})
```

用户 approve 后，从状态中恢复 context 继续执行。

---

## 6. 意图路由

用户不会说"请用 analyze pipeline"，需要自动判断该走哪条 pipeline。

### 6.1 路由规则

```python
INTENT_PATTERNS = {
    "report":    ["分析报告", "出报告", "生成报告", "详细分析"],
    "analyze":   ["分析", "为什么", "原因", "趋势", "对比", "同比", "环比", "变化"],
    "visualize": ["画图", "图表", "可视化", "柱状图", "折线图", "饼图"],
    "query":     [],  # 默认兜底
}

def route_intent(question: str) -> str:
    """根据用户问题判断走哪条 pipeline。"""
    q = question.lower()
    for pipeline, keywords in INTENT_PATTERNS.items():
        if any(kw in q for kw in keywords):
            return pipeline
    return "query"
```

### 6.2 也可以用 LLM 路由（更准确但多一次调用）

```python
def route_intent_llm(question: str) -> str:
    result = llm.call([{"role": "user", "content": question}],
        system_override="判断用户意图，只返回一个词：query / analyze / visualize / report")
    return result.get("text", "query").strip()
```

---

## 7. 与现有架构的关系

```
                      ┌──────────────┐
                      │ 意图路由      │
                      │ route_intent │
                      └──────┬───────┘
                             ↓
              ┌──────────────────────────────┐
              │     PipelineRunner            │
              │  ┌───────┐ ┌───────┐ ┌─────┐│
              │  │Stage 1│→│Stage 2│→│ ... ││
              │  └───┬───┘ └───┬───┘ └─────┘│
              └──────┼─────────┼────────────┘
                     ↓         ↓
              ┌──────────────────────────────┐
              │     现有 agent.py             │
              │  process() = Stage 1 执行者   │
              │  approve() = Stage 1 确认     │
              └──────────────────────────────┘
                     ↓
              ┌──────────────────────────────┐
              │     记忆系统 EMS/SMP/WMB      │
              │  每个 Stage 的输入输出都记录   │
              │  SMP 跨 pipeline 累积知识     │
              └──────────────────────────────┘
```

关键设计决策：
- **Stage 1（查询）复用现有 agent.py 的 process/approve**，不重写
- Pipeline 是 agent.py 之上的**编排层**，不替换它
- 新增的分析/可视化 Agent 是**新的 LLM 调用**，共享同一个 LLM 配置
- Artifact 通过 EMS 持久化，支持断点恢复

---

## 8. 分析 Agent 的 Structured Output

和 Forge Query 一样，分析 Agent 也应该用 Structured Output 而不是自由文本：

```python
ANALYSIS_TOOL = {
    "name": "generate_analysis",
    "description": "根据查询结果生成数据分析报告",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary":          {"type": "string", "description": "一句话核心发现"},
            "insights":         {"type": "array", "items": {"type": "string"}, "description": "关键洞察"},
            "key_metrics":      {"type": "object", "description": "关键指标名值对"},
            "trend_direction":  {"type": "string", "enum": ["up", "down", "stable", "mixed"]},
            "anomalies":        {"type": "array", "items": {"type": "string"}, "description": "异常数据点"},
            "recommendations":  {"type": "array", "items": {"type": "string"}, "description": "建议"},
        },
        "required": ["summary", "insights"],
    },
}
```

这样分析结果也是结构化的，可以被 Visualization Agent 精确消费，而不是从自由文本中解析。

---

## 9. 讨论记录

### 2026-03-24 首次讨论

**背景**：用户问是否需要引入 LangGraph。

**分析**：
- Forge 的多 Agent 协作是线性流水线（查询→分析→可视化），不是图
- LangGraph 解决的是动态分支/循环/并行汇总问题，Forge 不需要
- 引入 LangGraph 增加 ~200MB 依赖，版本碎片化风险高

**决策**：不引入 LangGraph，用 Pipeline 模式扩展现有架构。

**用户确认的产品方向**：
- 不仅查询，还要分析和可视化
- 未来 Forge = 数据查询 + 数据分析 + 数据可视化 的一站式 Agent

---

## 10. Agent 交互与边界设计

> 2026-03-24 讨论。用户核心关注：agent 之间如何交互、agent 与用户之间如何交互、责任和能力的边界在哪里。

### 10.1 交互模型总览

系统中有三类角色：

```
┌─────────────────────────────────────────────────────────┐
│                        用户（User）                       │
│  拥有最终决策权。AI 提议，用户确认。                         │
│  用户可以在任何时刻中断、修改、重来。                         │
└──────────┬──────────────────────────────────┬────────────┘
           │ 指令/确认/否决                      │ 查看/反馈
           ↓                                   ↓
┌─────────────────────┐           ┌─────────────────────┐
│  编排者（Orchestrator）│          │   展示层              │
│  route_intent()      │          │   飞书/Web/CLI        │
│  PipelineRunner      │          │   渲染 Artifact       │
│  管理 Stage 流转      │          │   收集用户反馈         │
└──────────┬───────────┘          └─────────────────────┘
           │ 分派任务 / 传递 Artifact
           ↓
┌──────────────────────────────────────────────────────────┐
│                    Agent 层                               │
│  ┌────────────┐  ┌────────────┐  ┌────────────────────┐ │
│  │ Query Agent │  │Analysis Agt│  │ Visualization Agt  │ │
│  │            │  │            │  │                    │ │
│  │ 能力：生成SQL│  │ 能力：推理  │  │ 能力：图表设计      │ │
│  │ 不能：执行  │  │ 不能：查数据│  │ 不能：改数据/改结论  │ │
│  └────────────┘  └────────────┘  └────────────────────┘ │
└──────────────────────────────────────────────────────────┘
           │ 读写
           ↓
┌──────────────────────────────────────────────────────────┐
│                    记忆系统 EMS / SMP / WMB               │
└──────────────────────────────────────────────────────────┘
```

### 10.2 Agent 之间的交互原则

**原则 1：Agent 不直接对话，只通过 Artifact 传递数据**

```
错误模式：
  Query Agent: "我查到了数据，你来分析一下"
  Analysis Agent: "好的，让我看看..."

正确模式：
  Query Agent → 输出 QueryResult{sql, columns, rows}
  Orchestrator 将 QueryResult 注入 Analysis Agent 的输入
  Analysis Agent → 输出 AnalysisReport{summary, insights}
```

理由：
- Agent 之间不需要"协商"，数据是确定的
- Artifact 是结构化的，不会产生理解歧义
- 每个 Agent 只看到自己需要的输入，不被上下游的推理过程干扰
- 方便 debug：每个 Artifact 都记录在 EMS 中，可以独立回放

**原则 2：Agent 不知道 Pipeline 的存在**

每个 Agent 只看到：
- 自己的 system prompt
- 当前用户问题
- 输入 Artifact（如果有）
- WMB 注入的知识

它不知道自己是 Pipeline 的第几步，不知道下游是谁。这保证了 Agent 的**可组合性**——同一个 Analysis Agent 可以被不同 Pipeline 复用。

**原则 3：Orchestrator 不做业务判断**

Orchestrator 只负责：
- 选择 Pipeline
- 按顺序执行 Stage
- 传递 Artifact
- 在需要确认的节点暂停

它不判断"分析结论是否正确"或"图表是否合适"——这些判断交给用户。

### 10.3 Agent 与用户的交互原则

**信任梯度模型**：

不同操作有不同的信任级别，信任级别决定了 AI 的自主程度：

| 信任级别 | AI 行为 | 用户行为 | 示例 |
|---|---|---|---|
| **L0 禁止** | 不允许执行 | - | 删除数据库表、修改生产数据 |
| **L1 审核** | 生成提议，等待确认 | 审核后确认/否决 | SQL 执行、指标定义入库 |
| **L2 展示** | 执行并展示结果 | 查看，可选反馈 | 数据分析、图表生成 |
| **L3 静默** | 自动执行，不打扰用户 | 可事后查看 | EMS 记录、SMP 提炼、用户画像更新 |

每个 Agent 的每个操作都标注了信任级别：

```python
TRUST_LEVELS = {
    # Query Agent
    "generate_sql":       "L1",   # 必须审核
    "execute_sql":        "L1",   # 必须确认
    "compile_forge_json": "L3",   # 静默（编译是确定性的）

    # Analysis Agent
    "generate_analysis":  "L2",   # 展示结果，用户可选反馈
    "compute_metrics":    "L3",   # 计算是确定性的

    # Visualization Agent
    "recommend_chart":    "L2",   # 展示推荐，用户可选换
    "generate_chart":     "L3",   # 生成是确定性的

    # Registry 操作
    "save_metric":        "L1",   # 必须确认
    "save_disambiguation":"L1",   # 必须确认
    "update_user_profile":"L3",   # 静默

    # SMP 提炼
    "extract_knowledge":  "L3",   # 静默
    "promote_to_org":     "L1",   # 必须确认（影响所有用户）
}
```

**交互流程示例**：

```
用户："分析各城市订单趋势，找出问题"
    ↓
意图路由 → "analyze" pipeline

Stage 1: Query Agent
    [L3] 编译 Forge JSON                    ← 静默
    [L1] SQL: SELECT city, month, SUM(amt)  ← 展示给用户
    用户：[确认执行]
    [L1] 执行 SQL，返回 38 行               ← 展示数据表格
    ↓ Artifact: QueryResult

Stage 2: Analysis Agent
    [L3] 读取 QueryResult                   ← 静默
    [L2] 生成分析报告                        ← 展示给用户
         "北京 3 月环比下滑 23%，是唯一负增长城市"
         "上海持续增长，已超过北京成为第一"
    用户可选：[有用👍] [不准确👎] [换个角度分析]
```

### 10.4 用户的控制能力

用户在任何时刻可以：

| 操作 | 触发方式 | 效果 |
|---|---|---|
| **中断** | 发送"取消"或"停" | 立即终止 Pipeline，保留已完成阶段的 Artifact |
| **回退** | "重新查询"或"换个 SQL" | 回到指定 Stage 重新执行 |
| **跳过** | "不需要分析，直接画图" | 跳过 Analysis Stage，直接进入 Visualization |
| **追问** | "为什么北京下滑？" | 在当前 Stage 上追加一轮对话（不进入下一 Stage） |
| **修改** | "把时间范围改成近半年" | 带着修改重新执行当前 Pipeline |
| **分叉** | "同时按品类也分析一下" | 启动一个并行 Pipeline |

这些操作通过 Orchestrator 统一处理，Agent 不需要感知。

### 10.5 能力边界（每个 Agent 能做和不能做的）

```
┌─ Query Agent ────────────────────────────────────────────┐
│ ✅ 能做：                                                 │
│   - 理解用户的自然语言查询意图                              │
│   - 生成 Forge JSON（Structured Output）                  │
│   - 编译为 SQL                                           │
│   - 重试编译错误（最多 2 次）                               │
│                                                          │
│ ❌ 不能做：                                               │
│   - 直接执行 SQL（必须用户确认）                            │
│   - 修改数据库                                            │
│   - 定义业务指标（那是用户通过定义模式做的）                   │
│   - 判断数据结果是否合理（那是 Analysis Agent 的事）          │
└──────────────────────────────────────────────────────────┘

┌─ Analysis Agent ─────────────────────────────────────────┐
│ ✅ 能做：                                                 │
│   - 对 QueryResult 做统计分析（环比/同比/排名/异常检测）      │
│   - 生成文字洞察                                          │
│   - 提出数据驱动的建议                                     │
│                                                          │
│ ❌ 不能做：                                               │
│   - 自己查数据（必须由 Query Agent 提供）                   │
│   - 修改原始数据                                          │
│   - 做因果推断（只能做相关性分析，因果需要用户判断）           │
│   - 做业务决策（只提供数据支持，决策权在用户）                │
└──────────────────────────────────────────────────────────┘

┌─ Visualization Agent ────────────────────────────────────┐
│ ✅ 能做：                                                 │
│   - 根据数据特征推荐图表类型                                │
│   - 生成 ECharts 配置                                     │
│   - 添加标注和高亮                                        │
│                                                          │
│ ❌ 不能做：                                               │
│   - 改变数据（只能改变呈现方式）                            │
│   - 改变分析结论                                          │
│   - 生成误导性可视化（如截断 Y 轴）                         │
└──────────────────────────────────────────────────────────┘
```

### 10.6 错误处理与责任归属

| 错误类型 | 责任方 | 处理方式 |
|---|---|---|
| SQL 语法错误 | Query Agent | 自动重试，超限后向用户报错 |
| 查询结果为空 | Query Agent | 告知用户，建议调整查询条件 |
| 分析结论不准确 | Analysis Agent | 用户标记"不准确"→ SMP 记录纠错 |
| 图表选型不佳 | Viz Agent | 用户可选"换个图表类型" |
| 数据本身有问题 | **数据源（非 Agent 责任）** | Agent 应标注异常值，但不修改 |
| 业务逻辑理解错误 | **Registry 不完善** | 通过 SMP 积累正确定义 |

---

## 11. 反向通信问题（2026-03-24 讨论）

### 11.1 决策：Agent 之间禁止反向通信

**问题**：Analysis Agent 发现数据不够（比如只有年度汇总，无法分析月度趋势），能否反向触发 Query Agent 补查？

**风险**：
- 无限循环："数据不够" → 补查 → "还不够" → 再补查...
- 用户失去控制：Agent 在后台自行发起多轮查询
- 成本不可控：每次补查消耗 LLM token + 数据库查询

**设计决策**：Agent 之间不直接通信。如果某个 Agent 发现输入不足，它的输出 Artifact 中标注 `status=incomplete`，附带 `suggested_query`，由用户决定是否发起新的 Pipeline。

```python
@dataclass
class AnalysisReport:
    status: str                    # "complete" | "incomplete"
    summary: str
    insights: list[str]
    needs: str | None = None       # 不完整时：缺少什么
    suggested_query: str | None = None  # 建议用户发起的查询
```

**交互流程**：

```
Analysis Agent 输出 incomplete：
  "当前数据是年度汇总，无法判断趋势方向。"
  "建议补充查询：各城市按月的订单数"
  [一键补查] [跳过分析]

用户点击 [一键补查]
  → Orchestrator 自动启动新的 analyze pipeline
  → 预填 suggested_query 为用户输入
  → 走正常的 SQL审核 → 执行 → 分析 流程
```

**中间方案（2026-03-24 确认）**：允许一次自动补查，但必须告知用户。

```
Analysis Agent 发现数据不足
    ↓
Orchestrator 检查：本轮是否已经补查过？
    ↓
没有 → 自动执行 suggested_query（跳过用户确认 SQL 的步骤）
       但在 UI 上展示："数据粒度不够，已自动补查月度数据..."
       然后将补查结果交给 Analysis Agent 重新分析
    ↓
已经补查过 1 次 → 不再自动补查
       展示 incomplete 结果 + suggested_query 按钮
       由用户决定是否继续
```

这样保证了：
- 简单场景（缺月度数据）一次自动搞定，用户不多操作
- 复杂场景不会死循环（最多 1 次自动补查）
- 用户始终知道发生了什么（UI 上展示补查提示）
- 用户始终是 hub，Agent 是 spoke。不存在 spoke-to-spoke 的链路。

---

## 12. 业务上下文的收集与管理（2026-03-24 讨论）

### 12.1 问题

Analysis Agent 说"北京下滑 23%"——但 23% 在该行业是正常波动还是严重问题？Agent 缺少业务上下文。

业务上下文 = 让 AI 的分析从"数据描述"升级为"业务洞察"的关键知识。

### 12.2 业务上下文的四层来源

```
┌─ 第 4 层：推断知识（AI 从数据中发现的规律）──────────────┐
│  "过去 6 个月，北京订单的季节性波动范围是 ±15%"           │
│  "每年 Q4 订单量平均增长 30-40%"                         │
│  来源：定期分析历史数据，AI 提炼，用户确认                  │
│  信任级别：L1（需用户确认后才能作为分析依据）                │
├─────────────────────────────────────────────────────────┤
│  第 3 层：积累知识（从对话中沉淀的业务规则）                │
│  "用户确认过：复购率的分母是'至少下过1单的用户'"            │
│  "上次分析时用户说：退款率超过 5% 就需要关注"              │
│  来源：SMP 语义记忆（已有）                               │
│  信任级别：L3（已被用户确认过的事实）                       │
├─────────────────────────────────────────────────────────┤
│  第 2 层：定义知识（用户主动录入的业务规则）                │
│  "正常退款率区间：3-5%"                                  │
│  "大促期间定义：618、双11 前后各 7 天"                    │
│  "团队 A 负责华北，团队 B 负责华南"                       │
│  来源：Web UI 管理 / 对话定义 / 文档导入                   │
│  信任级别：L3（用户主动录入，完全可信）                     │
├─────────────────────────────────────────────────────────┤
│  第 1 层：结构知识（数据库本身携带的信息）                  │
│  "orders.status 的枚举值：已完成/已取消/退款中"            │
│  "dim_city 表有 14 个城市"                               │
│  来源：Registry 结构层（已有，forge sync 自动生成）         │
│  信任级别：L3（直接来自数据库，完全可信）                   │
└─────────────────────────────────────────────────────────┘
```

### 12.3 与现有系统的映射

| 上下文层 | 存储位置 | 现有/新增 | 收集方式 |
|---|---|---|---|
| 结构知识 | Registry schema.json | 已有 | `forge sync` 自动 |
| 定义知识 | Registry metrics/disambiguations + **新增 business_context** | 部分已有 | Web UI / 对话 / 导入 |
| 积累知识 | SMP semantic memory | 已有 | 自动提炼 |
| 推断知识 | SMP（新类别） | 新增 | 定期分析 + 用户确认 |

### 12.4 新增：Business Context Registry

现有的 Registry 管理"数据的定义"（表结构、指标公式、歧义消除）。

需要新增一层"**业务规则**"，专门给 Analysis Agent 用：

```yaml
# business_context.registry.yaml

thresholds:
  refund_rate:
    label: "退款率健康区间"
    normal_range: [0.03, 0.05]
    warning: 0.08
    critical: 0.10
    note: "超过 5% 需关注，超过 10% 需告警"

  order_growth:
    label: "订单环比增长预期"
    normal_range: [-0.15, 0.30]
    note: "±15% 为正常季节性波动"

calendar:
  promotions:
    - name: "618"
      period: ["06-10", "06-20"]
      expected_lift: 2.0
    - name: "双11"
      period: ["11-01", "11-15"]
      expected_lift: 3.0

  seasons:
    - name: "春节"
      period: ["01-20", "02-10"]
      expected_impact: -0.3
      note: "订单量通常下降 30%"

org_structure:
  regions:
    华北: ["北京", "天津", "河北"]
    华东: ["上海", "江苏", "浙江"]
    华南: ["广州", "深圳", "东莞"]

benchmarks:
  industry: "电商零售"
  avg_order_value: 150
  avg_repurchase_rate: 0.25
  note: "行业平均值，来源：2025 年中国电商报告"
```

### 12.5 业务上下文的收集方式

| 方式 | 适用场景 | 实现 |
|---|---|---|
| **Web UI 管理** | 管理员录入阈值、日历、组织架构 | Settings 页面新增"业务规则"区块 |
| **对话式录入** | 用户在分析过程中补充 | AI 助手："退款率超过多少算异常？" → 用户回答 → 存入 |
| **SMP 自动积累** | 从历史对话中提炼 | 用户说"这个退款率太高了" → SMP 记录"退款率 8% 被用户认为偏高" |
| **数据推断** | 从历史数据统计 | 定期任务：计算各指标的均值和标准差作为基准 |
| **文档导入** | 导入行业报告/内部文档 | 上传 PDF/Markdown → LLM 提取规则 → 用户确认 |

### 12.6 业务上下文如何注入 Agent

通过 WMB 场景配置，Analysis Agent 的 scene 自动注入业务上下文：

```python
SCENE_CONFIGS = {
    "analyze": SceneConfig(
        ems_limit=6,
        smp_max_items=5,
        ems_token_budget=3000,
        # 新增：业务上下文注入
        inject_business_context=True,   # 读取 business_context.registry.yaml
        inject_categories=[
            "thresholds",               # 注入阈值判断标准
            "calendar",                 # 注入日历（判断是否大促期间）
            "benchmarks",               # 注入行业基准
        ],
    ),
}
```

Analysis Agent 的 system prompt 中会看到：

```
## 业务上下文
- 退款率正常区间：3-5%，超过 8% 需告警
- 当前处于 618 大促期间（06-10 至 06-20），订单量预期翻倍
- 行业平均客单价：150 元
- 华北区域包含：北京、天津、河北
```

---

## 13. 讨论记录

### 2026-03-24 第五轮：交互边界与业务上下文

**讨论主题**：

1. Agent 之间的反向通信问题
2. 业务上下文的收集与管理

**用户核心关注**：
- Agent 之间做不好容易无限循环
- 需要更多的业务上下文，如何收集和管理

**设计决策**：

| 问题 | 决策 |
|---|---|
| Agent 反向通信 | **禁止**。Agent 输出 `incomplete` + `suggested_query`，由用户决定是否补查 |
| 业务上下文来源 | 四层：结构知识 → 定义知识 → 积累知识 → 推断知识 |
| 存储方式 | 新增 `business_context.registry.yaml`，通过 WMB 注入 Analysis Agent |
| 收集方式 | Web UI 管理 + 对话录入 + SMP 积累 + 数据推断 + 文档导入 |

---

### 2026-03-24 第六轮：反向通信 + 业务上下文收集

**反向通信**：
- 完全禁止太保守（用户操作步骤多），完全放开有死循环风险
- 中间方案：**允许 1 次自动补查，必须告知用户**
- 超过 1 次 → 展示 incomplete + suggested_query，由用户决定
- 用户确认

**业务上下文收集**：
- 用户认为三种收集方式**同等重要**：
  1. Web UI 管理（管理员填阈值/日历/组织架构）
  2. 对话式录入（分析过程中用户随口说的规则 → SMP 提炼）
  3. 文档导入（上传行业报告/内部文档 → LLM 提取 → 用户确认）
- 不分优先级，三者并行实现

---

## 14. 实现路线图

### Phase 1: Pipeline 基础设施
- [ ] `agent/pipeline.py`：PipelineRunner + Stage + Artifact 数据结构
- [ ] 意图路由：关键词 + LLM 混合
- [ ] agent.py 中 process() 接入 Pipeline（query pipeline = 现有逻辑不变）
- [ ] 断点恢复：PipelineContext 存入 EMS state

### Phase 2: Analysis Agent
- [ ] `agent/agents/analyst.py`：分析 Agent（system prompt + generate_analysis tool）
- [ ] `business_context.registry.yaml`：业务规则文件 + 加载逻辑
- [ ] WMB analyze scene 注入业务上下文
- [ ] analyze pipeline 端到端跑通

### Phase 3: 业务上下文收集
- [ ] Web UI：Settings 页面新增"业务规则"管理区块
- [ ] 对话式录入：Analysis Agent 对话中提取规则 → SMP → 用户确认
- [ ] 文档导入：上传接口 + LLM 提取 + 预览确认

### Phase 4: Visualization Agent 升级
- [ ] `agent/agents/visualizer.py`：生成 ChartSpec（不直接生成图片）
- [ ] ChartSpec → ECharts config 渲染（升级现有 chart.py）
- [ ] 标注和高亮能力（Analysis Agent 标记的异常点 → 图表标注）

### Phase 5: Report Pipeline
- [ ] report_writer Agent
- [ ] 完整 report pipeline：query → analyze → visualize → summary
- [ ] 输出格式：Web 页面 / Markdown / 飞书长文
