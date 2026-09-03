# Forge Governance Action Catalog

> 状态：M0 v1.2.0；Query prepare/approve/execute 已接入公开 Enforce PEP · Last updated: 2026-09-03
>
> 机器可读目录：[`../agent/contracts/governance-action-catalog.v1.json`](../agent/contracts/governance-action-catalog.v1.json)；Schema：[`../agent/contracts/governance-action-catalog-v1.schema.json`](../agent/contracts/governance-action-catalog-v1.schema.json)。

## 1. 目的

Action Catalog 同时回答两个不同问题，禁止混为一个“100% Governed”数字：

```text
Contract Coverage
= supported 且 contract_status=specified 的 Action 数
÷ support_status=supported 的 Action 总数

Runtime Governance Coverage
= supported 且 runtime_enforcement_status=enforced 的 Action 数
÷ support_status=supported 的 Action 总数
```

字段语义：

- `support_status`：产品是否支持该 Action，不表示 v1 Governance Contract 已接入。
- `contract_status`：Owner、Executor、Context、Truth Source 和失败策略是否已定义完整。
- `runtime_enforcement_status`：v1 Contract 是未接入、部分接入还是已由生产 PEP 完整执行并通过负向门禁。
- `required_context.mandate=conditional`：Human 直接 Action 不需要 DelegatedMandate；Service/Agent 代表 Principal 行动时必须提供。
- 未进入目录的高风险 Action 必须 `fail_closed`，不得被解释为已治理能力。

现有系统可能已有 SQL hash、ACL、审批等领域控制；`not_integrated` 仅表示新的 Principal/Mandate/Policy v1 Contract 尚未接入，不否认既有控制，也不把既有控制冒充新 Contract 已执行。

## 2. v1.2.0 支持范围

| Action | Owner | Executor | 风险 | Human Decision | Truth Source | v1 Runtime |
|---|---|---|---|---|---|---|
| `query.prepare` | Forge | Forge | medium | not required | Forge Query Store | enforced |
| `query.approve` | Forge | Forge | high | required | Forge Query Store | enforced |
| `query.execute` | Forge | Forge | high | required | Forge Query Store | enforced |
| `query.cancel` | Pi | Forge | medium | conditional | Forge Query Store | not integrated |
| `registry.publish` | Forge | Forge | high | required | Forge Registry Store | not integrated |
| `registry.rollback` | Forge | Forge | high | required | Forge Registry Store | not integrated |
| `model.activate` | Forge | Forge | high | required | Forge Model Control Store | not integrated |
| `model.rollback` | Forge | Forge | high | required | Forge Model Control Store | not integrated |
| `skill_policy.update` | Pi Governance | Pi | high | required | Pi Skill Policy Store | not integrated |
| `report.read` | Report Service | Report Service | medium | not required | Forge Report Store | not integrated |
| `report.share` | Report Service | Report Service | high | required | Forge Report Store | not integrated |
| `report.export` | Report Service | Report Service | high | required | Forge Report Store | not integrated |
| `memory_proposal.confirm` | Pi Governance | Forge | medium | required | Forge Memory Store | not integrated |
| `memory_proposal.forget` | Pi Governance | Forge | medium | required | Forge Memory Store | not integrated |

当前 14 个 supported Action 的 **Contract Coverage 为 100%**，其中 `query.prepare`、`query.approve`、`query.execute` 已由公开 Enforce PEP 执行 Principal/Mandate/Policy/Approval/Drift/Read-only 负向门禁，**v1 Runtime Governance Coverage 为 3/14（21.4%）**。其余 11 个 Action 仍是 `not_integrated`；不能从查询链覆盖率推断它们已治理，也不能把本地实现表述为生产部署已完成。

## 3. 变更规则

1. 新 Action 先以 `planned` 加入，不自动进入支持分母。
2. `contract_status=specified` 前必须完成 Owner、Executor、Context、Truth Source、失败策略和 Contract 评审。
3. `runtime_enforcement_status=enforced` 前必须完成生产 PEP、负向测试、审计、迁移和回滚门禁。
4. Action 风险、Owner、Executor、Truth Source 或 Required Context 变化时升级目录 revision。
5. 高风险 Action 不允许通过 `metadata`、自由文本或渠道 payload 临时扩权。
6. Catalog 不能成为 Task、Query、Registry、Model 或 Report 的业务状态真相源。
