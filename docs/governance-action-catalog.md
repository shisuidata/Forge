# Forge Governance Action Catalog

> 状态：M0 v1.0.0 Contract 基线；尚未接入运行时授权 · Last updated: 2026-08-24
>
> 机器可读目录：[`../agent/contracts/governance-action-catalog.v1.json`](../agent/contracts/governance-action-catalog.v1.json)；Schema：[`../agent/contracts/governance-action-catalog-v1.schema.json`](../agent/contracts/governance-action-catalog-v1.schema.json)。

## 1. 目的

Action Catalog 是 Forge 声明 `100% Governed` 时的可测分母，不是营销口号，也不表示开放世界所有动作均受支持。

```text
Governance Coverage
= supported 且 governed 的 Action 数
÷ support_status=supported 的 Action 总数
```

规则：

- 只有 `support_status=supported` 的 Action 进入分母。
- `governed=true` 表示该 Action 已明确 Owner、Executor、Principal/Mandate/Policy/Decision 要求、真相源和失败策略。
- 未进入目录的高风险 Action 必须 `fail_closed`，不得被解释为已治理能力。
- 目录是 Contract 基线；M0 不改变现有运行时授权行为。运行时接入从 M1A 开始，需单独评审。

## 2. v1.0.0 支持范围

| Action | Owner | Executor | 风险 | Human Decision | Truth Source | 失败策略 |
|---|---|---|---|---|---|---|
| `query.prepare` | Forge | Forge | medium | not required | Forge Query Store | deny |
| `query.approve` | Forge | Forge | high | required | Forge Query Store | deny |
| `query.execute` | Forge | Forge | high | required | Forge Query Store | deny |
| `query.cancel` | Pi | Forge | medium | conditional | Forge Query Store | deny |
| `registry.publish` | Forge | Forge | high | required | Forge Registry Store | deny |
| `registry.rollback` | Forge | Forge | high | required | Forge Registry Store | deny |
| `model.activate` | Forge | Forge | high | required | Forge Model Control Store | deny |
| `model.rollback` | Forge | Forge | high | required | Forge Model Control Store | deny |
| `skill_policy.update` | Pi Governance | Pi | high | required | Pi Skill Policy Store | deny |
| `report.read` | Report Service | Report Service | medium | not required | Forge Report Store | deny |
| `report.share` | Report Service | Report Service | high | required | Forge Report Store | deny |
| `report.export` | Report Service | Report Service | high | required | Forge Report Store | deny |
| `memory_proposal.confirm` | Pi Governance | Forge | medium | required | Forge Memory Store | deny |
| `memory_proposal.forget` | Pi Governance | Forge | medium | required | Forge Memory Store | deny |

当前分母为 14，机器可读目录中 14 个均为 `governed=true`，Contract Coverage 为 100%。这只表示目录字段完整，不表示 M1 运行时 enforcement 已完成。

## 3. 变更规则

1. 新 Action 先以 `planned` 加入，不自动进入对外覆盖分母。
2. 进入 `supported` 前必须完成运行时 PEP、负向测试、审计、迁移和回滚门禁。
3. Action 风险、Owner、Executor、Truth Source 或 Required Context 变化时升级目录 revision。
4. 高风险 Action 不允许通过 `metadata`、自由文本或渠道 payload 临时扩权。
5. Catalog 不能成为 Task、Query、Registry、Model 或 Report 的业务状态真相源。
