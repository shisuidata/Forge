---
title: 附录｜术语、源码、测试与授课建议
summary: 教材配套索引、架构决策与教师版课程设计
---

# 附录

## A. 术语表

| 术语 | 含义 |
|---|---|
| Forge JSON / DSL / IR | LLM 生成、Compiler 消费的结构化查询中间表示。 |
| Structured Output | Provider 按 tool/JSON Schema 约束模型输出的能力；严格程度因 Provider 而异。 |
| Registry | 结构、指标、歧义、字段约定和业务上下文的组织知识系统。 |
| EA | Execution Accuracy，执行结果与参考结果等价的比例。 |
| Lint | 在编译/执行前检查业务、字段、粒度和输出契约。 |
| EMS | 完整情景/事件记忆。 |
| SMP | 从轨迹提炼的结构化语义记忆。 |
| WMB | 当前场景实时构建的工作上下文。 |
| Artifact | Pipeline Stage 之间传递的版本化结构化数据。 |
| Readiness | dev/PoC/prod 的配置门禁，不是安全认证。 |
| Golden question | 客户确认的高价值问题及 reference SQL/结果。 |

## B. 常见误解

| 误解 | 正确理解 |
|---|---|
| “用了 JSON 就不会错” | JSON 只缩小结构空间，业务语义、算法和透传表达式仍可能错。 |
| “编译成功等于答案正确” | 只说明 DSL 可翻译，仍需执行和业务验收。 |
| “120/120 等于所有客户 100%” | 只对当次模型、数据集、Registry 和版本成立。 |
| “支持 BigQuery” | 当前主要是编译路径；sync、execute、dry-run 和生产证据要分开。 |
| “ACL 已经是完整多租户” | 当前是 team 表可见性基础，企业 IAM/行列权限仍需完善。 |
| “人工审核就绝对安全” | 审核必须叠加只读账号、超时、资源限制和审计。 |

## C. 组件—源码—测试映射

| 能力 | 源码 | 主要证据 |
|---|---|---|
| DSL/Compiler | `forge/schema.json`, `forge/compiler.py` | `test_compiler*.py` |
| 动态 Tool Schema | `forge/schema_builder.py`, `agent/llm.py` | `test_llm_openai_compat.py`, `test_registry_context.py` |
| Retriever | `forge/retriever.py` | Registry/context tests、accuracy runner |
| Registry | `registry/*` | `test_sync.py`, `test_metric_validator.py`, `test_staging_sync.py` |
| Executor | `forge/executor.py` | `test_executor.py`, compatibility tests |
| Audit/Feedback | `agent/audit.py`, `agent/feedback.py` | `test_audit.py`, `test_feedback.py` |
| Memory | `agent/memory/*` | agent runtime/API tests |
| Pipeline | `agent/pipeline.py` | API/runtime tests与 PoC 验收 |
| Tenant/ACL | `agent/tenant.py` | agent runtime/API tests |
| Auth/Web | `web/auth.py`, `web/router.py` | `test_auth.py`, `test_api.py`, E2E |
| Readiness/PoC | `forge/readiness.py`, `forge/poc.py` | `test_commercial_readiness.py` |

## D. API、CLI 与配置索引

- API：[`web/router.py`](https://github.com/shisuidata/Forge/blob/main/web/router.py)
- CLI：[`forge/cli.py`](https://github.com/shisuidata/Forge/blob/main/forge/cli.py)
- 配置：[`config.py`](https://github.com/shisuidata/Forge/blob/main/config.py) 与 [`forge.yaml.example`](https://github.com/shisuidata/Forge/blob/main/forge.yaml.example)
- 部署：[production-deployment.md](https://github.com/shisuidata/Forge/blob/main/docs/production-deployment.md)
- 外部 Agent：[agent-integration.md](https://github.com/shisuidata/Forge/blob/main/docs/agent-integration.md)

## E. 架构事实矩阵

| 领域 | 当前事实 | 证据等级 | 主要缺口 |
|---|---|---|---|
| Query/Define | 已实现 | 自动化测试 + 基准 | 客户域长期证据 |
| Compiler SQLite/PG/MySQL | 已实现 | smoke_verified | 更多真实查询差异 |
| BigQuery/Snowflake | 编译路径 | implemented | sync/execute/dry-run/资源限制 |
| Registry | 文件与 Admin 基础已实现 | 自动化测试 | 全规则租户化、历史、回滚 |
| Memory | EMS/SMP/WMB 已实现 | 代码/运行测试 | 治理与规模验证 |
| Pipeline | 四类流程代码已实现 | implemented | 客户验收、可观测性、稳健 Structured Output |
| Tenant | user/team + table ACL | implemented/测试 | org、API scope、行列权限 |
| Security | auth、review、readonly、timeout、row cap | 自动化测试 | 企业 IAM、安全评估 |
| Deployment | Docker/compose/doctor/smoke | implemented/smoke | 标准备份升级回滚监控 |

## F. 架构决策摘要

1. **JSON IR**：优先 LLM 友好与 Schema 约束，而非发明文本语法。
2. **确定性编译**：将概率意图理解与执行语法分离。
3. **Registry 外置语义**：组织知识不能依赖模型参数和单个 prompt。
4. **Pipeline 而非 Graph**：当前任务线性，避免无必要编排复杂度。
5. **EMS/SMP/WMB**：原始轨迹、长期知识和工作上下文分层。
6. **Human-in-the-loop**：SQL 执行和组织事实入库属于 L1 审核动作。

## G. 教师版建议

建议 12 课时，每课 60—90 分钟：

| 课时 | 内容 | 产出 |
|---|---|---|
| 1—2 | 错误分类、产品边界、总体架构 | 能画出可信问数主链 |
| 3—4 | DSL、Compiler、Lint | 完成 anti join 编译实验 |
| 5—6 | Registry、RAG、上下文 | 建 5 个指标与检索评估 |
| 7—8 | Agent、Memory、Pipeline | 解释 Artifact 与审核断点 |
| 9—10 | 安全、权限、部署 | 完成威胁模型与 prod doctor |
| 11 | 基准与证据 | 设计客户 accuracy suite |
| 12 | 项目答辩 | 展示架构、证据和诚实边界 |

讨论题：

1. 当强模型裸 SQL 已足够好，Forge 应把资源投向 DSL 还是 Registry？
2. 哪些 lint 应进入通用 Compiler，哪些必须留在租户规则？
3. 一次用户确认能否把知识提升到组织级？
4. 哪些查询可以降低审核等级，依据是什么？

课程项目评分建议：架构正确 25%、Registry 质量 20%、安全边界 20%、测试证据 20%、边界诚实性 15%。
