---
title: 02｜产品边界与总体架构
summary: 认识 Forge 的系统上下文、组件职责和信任边界
---

# 产品边界与总体架构

## 1. 产品定位

Forge 是可私有化部署的**可信 AI 问数中间层与 Agent**：

- 可独立提供 Web Chat、Admin 和飞书入口；
- 可通过 `/api/prepare-query` 为外部 Agent 准备待审核 SQL；
- 由客户配置 LLM、Embedding、Registry 和数据库连接；
- 核心流程默认是“生成 → 审核 → 执行”，不是无人值守自动跑库。

弱模型是重要场景，但不是唯一价值来源。即使使用强模型，组织语义、权限、审核和审计仍需要系统承担。

![当前系统上下文图](assets/01-system-context.svg)

**文字替代说明**：用户从 Web、飞书或外部 API 进入 FastAPI；Agent 读取 Registry 和记忆，调用 LLM 生成 Forge JSON；Compiler/Lint 产出待审核 SQL；审批后 Executor 以只读约束访问数据库，并把过程写入 Audit/Feedback。

## 2. 当前架构主链

```text
用户/外部 Agent
  → Web、飞书、API
  → Agent 调度与澄清
  → Registry + SchemaRetriever + WMB 构建上下文
  → LLM Tool Calling / Structured Output
  → Forge JSON
  → Lint + JSON Schema + Compiler
  → 待审核 SQL
  → 用户确认
  → Executor 只读执行
  → 结果、Audit、Feedback、Memory
```

## 3. 组件职责

| 组件 | 责任 | 不负责 |
|---|---|---|
| `agent/agent.py` | 查询/定义调度、重试、澄清、待审核状态 | SQL 方言细节、数据库权限最终强制 |
| `agent/llm.py` | Provider 调用、Registry/RAG 上下文、Tool Schema | 决定性 SQL 生成 |
| `forge/schema_builder.py` | 按当前 Registry 动态收窄 Tool Schema | 证明业务口径正确 |
| `forge/compiler.py` | 校验、coerce、确定性编译、方言适配 | 猜测用户未表达的算法 |
| `forge/lint.py` | 发现已知业务/字段/结果契约问题并触发重试 | 替代租户化 Registry |
| `forge/executor.py` | 只读检查、超时、结果上限、执行 | 替代数据库只读账号 |
| `forge/retriever.py` | 向量/BM25 召回、缓存和上下文精简 | 保证任意 Schema 100% 召回 |
| `agent/memory/*` | EMS/SMP/WMB 记忆分层 | 代替 Registry 的组织治理 |
| `agent/pipeline.py` | Query/Analyze/Visualize/Report 线性编排 | 多 Agent 自由循环协商 |
| `agent/tenant.py` | 用户—团队映射和团队表 ACL | 完整企业 IAM 与行列级权限 |
| `agent/audit.py` / feedback | 查询证据、状态和反馈 | 自动证明分析结论正确 |
| `forge/readiness.py` | dev/PoC/prod 配置门禁 | 代替客户环境验收 |

## 4. 依赖方向

Forge 有意保持单向依赖：

```text
入口层 → 编排层 → 语义/生成层 → 编译/执行层 → 数据源
                    ↘ 记忆/审计/反馈 ↗
```

LLM 不直接连接数据库；Compiler 不调用 LLM；Executor 只接收已经生成并审核的 SQL。这种分离让每段责任可单测、可替换、可审计。

## 5. 信任边界

- **不可信输入**：用户自然语言、外部文档、LLM 输出。
- **受约束中间态**：动态 Tool Schema 下的 Forge JSON；仍需 lint、schema validation 和编译。
- **人工决策点**：SQL 执行、指标/规则入库、组织级知识提升。
- **最终强制边界**：数据库只读账号、网络隔离和数据库权限。
- **证据系统**：Audit、EMS、测试报告和 smoke 结果；证据说明“在哪些条件下验证过”，不是无限承诺。

## 6. 当前与目标状态

| 领域 | 当前 | 目标 |
|---|---|---|
| 查询主链 | **已实现/有自动化测试** | 客户域稳定性持续验证 |
| Registry 管理 | 文件 + Admin CRUD 基础 | 完整作用域、变更历史和回滚 |
| 多租户 | team 映射和表 ACL **已实现** | org/team/user 全面规则与审计隔离 |
| Pipeline | 代码路径 **已实现** | 客户场景验收、可观测性和失败恢复增强 |
| 数据库 | SQLite/PG/MySQL 有 smoke；BQ/Snowflake 主要是编译路径 | Adapter、dry-run、资源治理和生产证据 |
| 运维 | compose、readiness、doctor、smoke 脚本 | 标准备份、升级、回滚、监控 runbook |

## 7. 源码地图

```text
forge/      DSL、Compiler、Retriever、Executor、Lint、CLI、Readiness
agent/      Agent loop、LLM、Memory、Pipeline、Tenant、Knowledge
registry/   Schema sync、规则校验、staging、业务上下文
web/        FastAPI 路由、认证、Jinja2 Chat/Admin
scripts/    Demo、provider/database/production smoke、性能检查
tests/      单元、API、兼容、E2E、accuracy 和 Spider2
```

下一章会把这些组件重新组织成七项核心技术优势。
