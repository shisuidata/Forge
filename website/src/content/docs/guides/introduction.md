---
title: 介绍
description: 面向企业 Data Agent 的开源 Trust Runtime：Evaluate → Enforce → Explain
---

## Forge 是什么

Forge 是面向企业 Data Agent 的开源 **Trust Runtime（可信数据运行时）**，位于上游 Agent 与数据库之间。它接收 Direct SQL 或受约束的 Forge JSON 候选，让评估、执行约束与证据解释进入同一条可检查的路径。

Forge 不是把模型生成的 SQL 直接交给数据库的聊天包装层。它关注的是：**这条候选经过了什么检查，谁授权了执行，实际执行了什么，以及结果有哪些尚未证明的边界。**

## Evaluate → Enforce → Explain

| 环节 | 做什么 | 不代表什么 |
|---|---|---|
| **Evaluate · 评估候选** | 在不执行 SQL 的前提下检查候选，记录 Policy、失败阶段、版本与评估证据；支持评估集、重放与回归门禁 | 评估通过不授予执行权限，也不证明业务答案正确 |
| **Enforce · 约束执行** | 将主体、用途、任务、授权范围、Registry 与 SQL 绑定到 QueryRun，按授权和审核条件决定是否允许执行 | 获得授权不等于查询表达了正确的业务意图 |
| **Explain · 解释证据** | 从同一 QueryRun 展示实际 SQL、结果、语义来源、审批、Evidence、lineage 与显式限制 | 证据完整性通过不等于语义正确，也不等于源数据有不可变快照 |

```text
上游 Agent / 开发者
  ↓ Direct SQL 或 Forge JSON 候选
Evaluate：非执行评估与可复现记录
  ↓ 候选仍需独立满足执行条件
Enforce：授权范围、运行时检查与适用的人工审核
  ↓ 满足条件才执行
QueryRun：实际 SQL、执行状态与结果
  ↓
Explain：同一运行的证据、来源和限制
```

普通 Quickstart 会展示实际 SQL，等待批准；`forge quickstart --agent` 演示 v2 有界授权：独立主体凭证、有界 Agent 自主子集、指定审核人与授权撤销。两条演示都只用隔离合成数据，都不代表生产安全验收。

## 为什么不只优化 Prompt

Forge 将不同责任拆开处理，而不是把所有问题都交给模型：

- **Registry 管理上下文**：表、字段、关系、指标与语义规则，为候选提供可追溯的业务定义。
- **Compiler 负责确定性翻译**：把支持范围内的 Forge JSON 编译为 SQL；Direct SQL 不必先转换为 Forge JSON。
- **运行时设置拒绝边界**：只读、资源范围、审核绑定、超时和结果行数限制，都是可检查的控制点，而不是 Prompt 中的请求。
- **Evidence 与 Audit 保留过程**：区分输入、检查失败、审批与执行结果，支持定位和复盘。

这些机制不能消灭所有错误。模型可能选错字段或算法，Registry 可能缺少业务定义，编译器与授权实现也需要测试和修复。**Direct SQL 与 Forge JSON 是两种候选格式，不是可信程度的排名。**

## 当前状态与使用边界

Forge 仍处于早期、持续开发阶段，适合先用隔离合成数据评估机制，不应当作已经通过生产安全验收或共享多租户验收的产品。

[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-22.md)记录了 2026-10-22 对公开 `main` 提交 `e50b0a0` 的验证：Python **1049 passed / 26 skipped**、Pi **226 passed**，远端 CI 通过。2026-10-04 评估所列两项 **P0 安全发布阻断**（Direct SQL 授权检查、Pi 跨主体事件去重隔离）已修复并有回归测试；真实数据库只读角色、真实身份与客户数据、独立安全审核仍未验证，这些测试结果不是安全认证，也不是当前部署的认证。

公开 BIRD 500 题的历史同候选 Compiler replay 为 Forge **313/500**、Direct SQL **314/500**，未证明总体准确率优势；具体成本与实验边界见[基准测试](/reference/benchmarks/)。生产部署、真实客户数据库和外部采用证据也不能由本地合成演示外推。更多版本与状态说明见[仓库当前状态](https://github.com/shisuidata/Forge/blob/main/docs/current-project-state.md)。

## 从哪里开始

从[五分钟上手](/guides/quickstart/)运行公开 Golden Path：无需 LLM、Pi、API Key 或客户数据库，先亲自观察 Evaluate → Enforce → Explain 如何串起一次受控查询。若要了解内部结构，可继续阅读[架构参考](/reference/architecture/)。
