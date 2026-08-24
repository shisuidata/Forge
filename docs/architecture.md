# Forge 架构总览

> 本页是稳定入口。完整教材见 [`docs/architecture-course/`](architecture-course/index.md)。

Forge 是面向数据团队的**可信 AI 问数中间层与 Agent**。它把自然语言查询拆成可治理的链路，而不是让 LLM 直接连接数据库：

```text
Web / 飞书 / 外部 API
  → Agent（澄清、调度、重试）
  → Registry + Schema RAG + WMB
  → LLM Structured Output
  → Forge JSON
  → Lint + JSON Schema + 确定性 Compiler
  → 待审核 SQL
  → 用户确认
  → 只读 Executor（timeout / row cap）
  → Result + Audit + Feedback + Memory
```

![Forge 当前系统上下文](architecture-course/assets/01-system-context.svg)

## 核心架构原则

1. **意图与执行分离**：LLM 表达 Forge JSON，Compiler 负责 SQL 语法。
2. **组织语义外置**：指标、歧义、字段约定和业务上下文进入 Registry。
3. **高风险动作由人决定**：SQL 与组织事实默认审核后执行/入库。
4. **数据库权限是最终边界**：应用层只读检查不能替代只读账号。
5. **兼容声明按证据分层**：compile、sync、execute、smoke、production 分开描述。
6. **诚实标注算法边界**：DSL 能稳定编译已表达意图，不能替模型发明正确算法。

## 主要组件

| 层 | 模块 | 职责 |
|---|---|---|
| 入口 | `web/`, `agent/feishu.py` | Chat/Admin/API/飞书、认证与交互 |
| 调度 | `agent/agent.py` | Query/Define、澄清、重试、review state |
| 上下文 | `agent/llm.py`, `forge/retriever.py` | Registry、RAG、ACL 与 Provider 调用 |
| 中间表示 | `forge/schema.json`, `forge/schema_builder.py` | 静态 DSL 与动态 Tool Schema |
| 确定性核心 | `forge/compiler.py`, `forge/lint.py` | 校验、coerce、方言编译和规则检查 |
| 执行 | `forge/executor.py` | 只读校验、超时、结果上限与查询执行 |
| 知识 | `registry/`, `agent/memory/` | 组织知识、EMS/SMP/WMB 和记忆提炼 |
| 编排 | `agent/pipeline.py` | Query/Analyze/Visualize/Report Artifact 流 |
| 治理 | `agent/tenant.py`, `agent/audit.py` | team ACL、审计、反馈与回放 |
| 交付 | `forge/readiness.py`, `forge/poc.py` | dev/PoC/prod 门禁和交付证据 |

## 当前状态与目标状态

- **已实现并有测试的主线**：查询/定义、DSL/Compiler、Registry、Retriever、审核执行、Audit/Feedback、Web/Admin、认证、Memory、team ACL 和 readiness。
- **已有代码路径、仍需客户验收**：分析/可视化/报告 Pipeline、知识收集、多租户治理。
- **兼容层级**：SQLite/PostgreSQL/MySQL 有 smoke 证据；BigQuery/Snowflake 主要是编译路径，不能等同完整执行支持。
- **正式规模交付前重点**：客户域 accuracy suite、规则租户化、Registry 版本/回滚、企业权限和运维 runbook。

## 教材入口

- [导读与阅读路线](architecture-course/index.md)
- [核心技术优势](architecture-course/03-core-advantages.md)
- [一次查询的完整生命周期](architecture-course/04-query-lifecycle.md)
- [完整实战课程](architecture-course/12-labs.md)
- [目标架构与路线图](architecture-course/13-roadmap.md)

## 深入参考

- [工作原理与 DSL 能力](how-it-works.md)
- [DSL 形式化语义](dsl-semantics.md)
- [Registry](registry.md)
- [兼容性矩阵](compatibility-matrix.md)
- [生产部署](production-deployment.md)
- [基准测试](benchmarks.md)
