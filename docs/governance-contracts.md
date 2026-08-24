# Forge Governance Contract v1

> 状态：M0 Contract Review 已通过；尚未接入运行时授权 · Last updated: 2026-08-24
>
> 权威 Schema 位于 [`../agent/contracts/`](../agent/contracts/)，TypeScript 对应定义位于 [`../services/pi-orchestrator/src/governance-contracts.ts`](../services/pi-orchestrator/src/governance-contracts.ts)。

## 1. Contract 边界

M0 只固定跨 Python/TypeScript 的治理语义，不修改 Task API、SQLite Schema、Forge QueryRun、数据库授权或 OAuth Runtime。跨 Contract 评审、Threat Model 与迁移结论见 [`governance-contract-review-2026-08-24.md`](governance-contract-review-2026-08-24.md)。

| Contract | Owner | Truth Source | 用途 |
|---|---|---|---|
| `PrincipalContext v1` | Pi Governance | Principal/Membership 与认证映射 | 固定 Actor、最终责任主体、Organization/Workspace、认证方法与 delegation snapshot |
| `DelegatedMandate v1` | Pi Governance | Mandate Store | 限制 Service/Agent 的 Audience、Purpose、Task、能力、资源、预算引用、审批策略和有效期 |
| `PolicyDecision v1` | 作出资源裁决的 PEP | 对应 Policy/Audit Store | 固定 subject、Action、Resource、effect、reason、obligation、policy revision |
| `ResourceRef v1` | 资源 Owner | 对应领域 Store | 提供最小、稳定、租户有界的资源引用 |
| `DatasourceBinding v1` | Forge | Forge Datasource/Policy Store | 固定 Workspace 到 Datasource revision 和 Policy revision |
| `RegistryBinding v1` | Forge | Forge Registry/Policy Store | 固定 Workspace、Datasource Binding、Registry revision 和 Policy revision |

## 2. Actor、Delegate 与最终责任

```text
actor_principal
· human / service / agent
· 表示当前安全边界实际发起请求的 Actor

DelegatedMandate.delegate_principal
· service / agent
· 表示代表 Principal 行动的受托方

accountable_principal / decision_authority
· human / team / organization
· 表示最终组织责任与决策权
```

Schema 从类型上禁止 Service/Agent 成为 `accountable_principal` 或 `decision_authority`。Service/Agent 可以在有效 `DelegatedMandate` 下行动，但不能通过自然语言、Prompt 或自我委托获得最终权力。

`DelegatedMandate v1` 必须绑定一个具体 Task、一个 Audience、能力和资源范围。v1 的 `can_delegate` 固定为 `false`；只有未来 Contract 能表达 `parent_mandate + authority subset + chain continuity` 时，才能讨论递归委托。

Human 直接 Action 不需要给自己签发 DelegatedMandate，而是依据 Membership/Role、Policy 和必要的 Decision。Service/Agent 代表 Human/Team/Organization 行动时，Mandate 才是必需。

## 3. 最小披露

Contract 可以携带：

- 稳定 ID、类型、Organization/Workspace scope。
- authentication method、assurance level、session hash 和有效期。
- Audience、Purpose、Task、capability、ResourceRef、Policy/Binding revision。
- Decision effect、reason code、obligation 和 decision authority。

Contract 禁止携带：

- access/refresh token、API Key、密码、数据库 URL 或 Secret。
- 完整数据库结果、Prompt、Tool transcript 或 hidden chain-of-thought。
- 用开放 `metadata` 表达权限、责任、预算、审批或资源范围。

## 4. 版本与兼容

- JSON Schema 是跨服务权威 Contract。
- TypeBox 定义消费同一有效/无效 fixture corpus，必须保持行为 parity。
- v1 文件不原地改变已进入运行时的不兼容语义；当前 M0 尚无生产调用方，评审通过前允许修订初版。
- M0 fixture 只证明 Contract 形状和最小安全不变量，不代表运行时 authorization 已启用。
- 跨对象 Organization/Workspace、时间顺序、撤销状态和 delegation continuity 在 M0.5 语义门禁中继续验证。
