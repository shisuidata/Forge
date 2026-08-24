---
title: Forge 架构教材
summary: 从可信问数原理到生产落地的完整架构课程
---

# Forge 完整架构教材

> Forge 是面向数据团队的**可信 AI 问数中间层与 Agent**。它不把正确性押在一次 prompt 上，而是把业务口径、结构化生成、确定性编译、人工审核、只读执行和反馈证据串成闭环。

## 这套教材解决什么问题

读完后，你应该能回答四个问题：

1. Forge 与“让 LLM 直接写 SQL”有什么本质区别？
2. Registry、Forge DSL、Compiler、Lint 和 Audit 分别承担哪一段信任责任？
3. 当前代码已经实现了什么，哪些只完成了代码路径，哪些仍是目标架构？
4. 如何用 Demo 验证核心链路，并判断一个客户环境能否进入 PoC 或生产？

## 状态与证据标识

| 标识 | 含义 |
|---|---|
| **已实现** | 当前分支存在可运行代码路径，不自动等于可生产交付。 |
| **部分实现** | 主路径存在，但能力、隔离、异常处理或交付证据尚不完整。 |
| **已验证** | 有自动化测试、真实 smoke 或明确测试报告；正文会注明证据层级。 |
| **规划中** | 仅属于目标架构或路线图，不应被描述为现有能力。 |

兼容证据进一步分为 `implemented`、`smoke_verified` 和 `production_verified`。具体口径见[兼容性矩阵](https://github.com/shisuidata/Forge/blob/main/docs/compatibility-matrix.md)。

## 阅读路线

- **产品/业务读者**：01 → 02 → 03 → 06 → 11 → 13
- **数据从业者**：01 → 03 → 04 → 06 → 07 → 12
- **架构师**：02 → 03 → 08 → 09 → 10 → 11 → 13
- **开发者**：04 → 05 → 07 → 08 → 12 → 附录

## 目录

1. [为什么 AI 问数不可信](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/01-why-trust.md)
2. [产品边界与总体架构](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/02-overview.md)
3. [核心技术优势](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/03-core-advantages.md)
4. [一次查询的完整生命周期](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/04-query-lifecycle.md)
5. [Forge DSL 与确定性编译](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/05-dsl-compiler.md)
6. [Registry：组织的数据知识系统](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/06-registry.md)
7. [Schema 检索与上下文工程](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/07-retrieval-context.md)
8. [Agent、记忆与 Pipeline](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/08-agent-memory-pipeline.md)
9. [安全、权限与可信执行](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/09-security.md)
10. [部署、适配与可运维性](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/10-deployment.md)
11. [基准、证据与诚实边界](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/11-evidence.md)
12. [完整实战课程](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/12-labs.md)
13. [目标架构与演进路线](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/13-roadmap.md)
14. [附录：术语、源码、测试与授课建议](https://github.com/shisuidata/Forge/blob/main/docs/architecture-course/appendix.md)

## 贯穿案例

全书使用一个电商问题贯穿：

> “统计复购率；复购用户指下过至少 2 单的用户，分母是至少下过 1 单的用户。”

它同时包含业务口径、聚合粒度、多步计算和审核要求，适合解释 Forge 为什么不能只靠模型“猜”。

## 学习原则

- **先判断问题属于哪一层，再选择机制。** DSL 不能替代业务定义，Registry 也不能替代算法推理。
- **代码事实和交付证据分开。** 有实现不代表在所有数据库、Provider 和客户 Schema 上都验证过。
- **在边界内追求确定性。** Forge 的目标不是覆盖 SQL 的全部能力，而是在高频查询子集内建立可解释、可回放的可信链路。
