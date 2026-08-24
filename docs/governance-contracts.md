# Forge Governance Contract v1

> 状态：M0 Contract 基线；尚未接入运行时授权 · Last updated: 2026-08-24
>
> 权威 Schema 位于 [`../agent/contracts/`](../agent/contracts/)，TypeScript 对应定义位于 [`../services/pi-orchestrator/src/governance-contracts.ts`](../services/pi-orchestrator/src/governance-contracts.ts)。

## 1. Contract 边界

M0 只固定跨 Python/TypeScript 的治理语义，不修改 Task API、SQLite Schema、Forge QueryRun、数据库授权或 OAuth Runtime。

| Contract | Owner | Truth Source | 用途 |
|---|---|---|---|
| `PrincipalContext v1` | Pi Governance | Principal/Membership 与认证映射 | 固定 Actor、最终责任主体、Organization/Workspace、认证方法与 delegation snapshot |
| `AgentMandate v1` | Pi Governance | Mandate Store | 限制 Agent 的 Purpose、Task、能力、资源、预算引用、审批策略和有效期 |
| `PolicyDecision v1` | 作出资源裁决的 PEP | 对应 Policy/Audit Store | 固定 subject、Action、Resource、effect、reason、obligation、policy revision |
| `ResourceRef v1` | 资源 Owner | 对应领域 Store | 提供最小、稳定、租户有界的资源引用 |
| `DatasourceBinding v1` | Forge | Forge Datasource/Policy Store | 固定 Workspace 到 Datasource revision 和 Policy revision |
| `RegistryBinding v1` | Forge | Forge Registry/Policy Store | 固定 Workspace、Datasource Binding、Registry revision 和 Policy revision |

## 2. Actor 与最终责任

```text
actor_principal
· human / service / agent
· 表示实际发起或执行请求的 Actor

accountable_principal / decision_authority
· human / team / organization
· 表示最终组织责任与决策权
```

Schema 从类型上禁止 Agent 成为 `accountable_principal` 或 `decision_authority`。Agent 可以在有效 `AgentMandate` 下行动，但不能通过自然语言、Prompt 或自我委托获得最终权力。

## 3. 最小披露

Contract 可以携带：

- 稳定 ID、类型、Organization/Workspace scope。
- authentication method、assurance level、session hash 和有效期。
- Purpose、Task、capability、ResourceRef、Policy/Binding revision。
- Decision effect、reason code、obligation 和 decision authority。

Contract 禁止携带：

- access/refresh token、API Key、密码、数据库 URL 或 Secret。
- 完整数据库结果、Prompt、Tool transcript 或 hidden chain-of-thought。
- 用开放 `metadata` 表达权限、责任、预算、审批或资源范围。

## 4. 版本与兼容

- JSON Schema 是跨服务权威 Contract。
- TypeBox 定义消费同一有效/无效 fixture corpus，必须保持行为 parity。
- v1 文件不原地改变不兼容语义；破坏性变化新增版本文件和显式迁移。
- M0 fixture 只证明 Contract 形状和最小安全不变量，不代表运行时 authorization 已启用。
