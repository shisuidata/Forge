---
title: 架构设计
description: Trust Runtime 的职责、当前 v2 执行边界与历史架构教材入口
---

## 当前系统职责

Forge 是面向企业 Data Agent 的开源**可信数据运行时**，不是必须由自带 Chat、模型或 Forge JSON 才能使用的问数 Agent。当前开发者路径是 **Evaluate → Enforce → Explain**：

```text
上游 Agent / 开发者
  → Direct SQL 或 Forge JSON 候选
  → Evaluate：不执行 SQL、不授予执行权
  → Enforce：主体、用途、任务、授权范围、Policy、Assurance、Registry
  → QueryRun：满足授权及适用审核条件才执行
  → 只读数据库连接、超时与结果限制
  → Explain：同一运行的 SQL、结果、Evidence、lineage 与限制
```

Evaluate 可以独立使用；评估通过不是执行授权。Direct SQL 不先转成 Forge JSON；Forge JSON 经确定性编译后仍需进入相同的执行控制，不因结构化生成而自动可信。

## 编排与执行的信任边界

- **Pi** 是集成路径中的主 Orchestrator 与 Task 真相源；Forge 保留校验、拒绝与失败关闭能力。
- **DATA Skills** 提供专业方法，不持有任务主状态，也不因此获得数据库执行权。
- **Web / 渠道** 是受信入口与投影，不创建第二套业务真相源。
- **Agent** 使用独立凭证；管理员授予单任务、单数据源及表范围的有界授权，并区分自主子集与指定人工审核资源，授权可随时撤销。
- **数据库权限** 是独立强制边界。应用层校验、Registry 与审核不能替代数据库只读角色、资源治理和网络隔离。

无需 Pi 或模型的[Quickstart](/guides/quickstart/)使用隔离合成身份和 SQLite，仅演示这条 Runtime 链路，不是完整自然语言 Agent 验收。

## 当前接口契约

| 状态 | 入口 | 边界 |
|---|---|---|
| 当前 | `POST /api/v1/evaluate`、`forge evaluate` | 不执行 SQL，不授予执行权 |
| 当前 | `/api/v2/governance/*`、`/api/v2/enforce/query-runs`、`/api/v2/explain/query-runs/{id}` | 独立主体、有界授权、撤销与指定审核人；不等于已验收真实企业身份与权限 |
| 当前 | `forge quickstart [--agent]` | 无模型、隔离合成数据；`--agent` 演示有界授权与撤销 |
| 已退休 | v1 Enforce 执行入口、原始 SQL 执行入口 | 返回 410；历史 v1 QueryRun 只保留管理员只读审计 |

不要把旧共享 API Key、pending/approve 或原始 SQL 入口当作安全生产接入方案。Direct SQL **候选格式**与无门禁原始 SQL 执行是两回事。

具体契约与版本迁移见[当前项目状态](https://github.com/shisuidata/Forge/blob/main/docs/current-project-state.md)、[治理契约](https://github.com/shisuidata/Forge/blob/main/docs/governance-contracts.md)及[仓库架构](https://github.com/shisuidata/Forge/blob/main/docs/architecture.md)。

## 证据与未关闭的边界

有界 Agent 执行切片已在公开 `main` 发布，并在隔离合成环境中验证；这不代表生产或外部采用门禁关闭。[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-22.md)记录了 2026-10-22 对提交 `e50b0a0` 的验证，2026-10-04 评估所列 Direct SQL 授权检查与 Pi 跨主体事件去重隔离两项 P0 发布阻断已修复并有回归测试。真实数据库只读角色、真实身份与独立安全审核仍未验证；本页不宣称可用于真实生产或共享多租户访问。

Explain 的 `verified` 是证据完整性状态，不是业务语义正确、源数据不可变快照或任何 SQL 都安全的证明。数据库和 Provider 的编译、连接、smoke 与生产验证也必须分开表述，见[基准与验证边界](/reference/benchmarks/)。

## 历史架构教材

[架构课程](/course/)保留了早期 Chat/Agent、Registry、Compiler、Memory 和 Pipeline 的设计、实验与配图。其旧 API、配置及“当前”成熟度属于历史教学快照，不覆盖本页的当前 Runtime 边界。先按[安装与使用边界](/guides/installation/)和[Quickstart](/guides/quickstart/)开始，再用教材理解演进过程。
