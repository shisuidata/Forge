---
title: 13｜目标架构与演进路线
summary: 从封闭 Beta 候选走向标准化可信问数平台
---

# 目标架构与演进路线

![当前架构到目标架构](assets/09-current-to-target.svg)

**文字替代说明**：当前已有可信查询主链、基础 Registry、Memory/Pipeline、team ACL 和部署门禁；目标架构补齐客户 accuracy suite、规则租户化、企业 IAM、Adapter、可观测性与标准运维，长期围绕组织知识和证据飞轮演进。

## 1. 当前基线

Forge 已达到“受控生产落地 / 封闭 Beta 候选”：核心查询链、审计、安全开关、生产 compose、readiness/doctor 和自有基准已经存在。它还不是无需陪跑、可无约束规模铺开的标准产品。

## 2. 近期：让每个客户域可验证

### 客户 accuracy suite

- **动机**：项目内 40 题不能代表客户业务。
- **依赖**：客户 Schema、核心问题、reference SQL、结果判定。
- **验收**：P0 问题稳定达标，线上错误可回放并进入测试。
- **不做**：用公开 benchmark 数字替代客户验收。

### Registry 管理与规则租户化

- **动机**：YAML 手改和全局 lint 不适合规模交付。
- **依赖**：作用域、审批、版本、影响分析和回滚。
- **验收**：管理员可管理指标/歧义/约定；规则仅影响目标租户；变更触发回归。
- **不做**：继续把每个客户失败堆成全局 if/else。

### 权限与运维 runbook

- **动机**：表 ACL 和 compose 只是基础。
- **验收**：角色、API Key scope、审计隔离、备份恢复、升级回滚和告警演练完成。

## 3. 中期：收敛适配层和开放边界

### DatabaseAdapter

从“编译某方言”升级到显式能力契约：introspect、compile、dry-run、execute、timeout、row cap 和 capabilities。BigQuery/Snowflake 在 dry-run 与资源限制完成前保持 partial。

### LLMProviderAdapter

统一 tools、named tool choice、strict schema、JSON fallback、timeout 和 smoke 证据。Provider 兼容状态与账号/配额可用状态分开记录。

### 外部 Agent

第一阶段只开放 prepare/query planning，不开放隐式执行。执行必须带可验证的人工批准记录与最小权限 token。

## 4. 长期：组织知识与证据飞轮

```text
业务问题
  → 受控生成与执行
  → 用户反馈/线上失败
  → failure triage
  → Registry/租户规则/测试
  → 客户 accuracy suite
  → 更稳定的下一次查询
```

护城河不只押在 SQL 语法约束，而是：

- 组织可治理的指标和业务上下文；
- 跨模型、数据库和版本可比较的证据；
- 失败转规则、规则转测试的工程速度；
- 人类始终保有高风险动作的最终控制权。

## 5. 目标架构验收原则

1. **每项能力有状态**：implemented、smoke_verified、production_verified。
2. **每项规则有 scope**：org、team、user 或 dataset。
3. **每次执行有证据**：问题、上下文版本、Forge JSON、SQL、批准、结果摘要。
4. **每个 Adapter 有 capabilities**：不靠文档猜兼容。
5. **每个失败有归属**：检索、口径、生成、算法、权限、执行或数据质量。
6. **每个生产变更可回滚**：Registry、模型、Compiler 和配置均可追踪。

## 6. 明确不做

- 不把 Forge 变成可执行任意 DDL/DML 的数据库管理员；
- 不允许 Agent 无限自主补查和循环执行；
- 不用“支持 OpenAI API”替代 Provider 真实 smoke；
- 不用单一 benchmark 证明普遍准确；
- 不为了覆盖 SQL 全集牺牲边界内的可审计性；
- 不把数据库权限责任推给 prompt 或应用层正则。
