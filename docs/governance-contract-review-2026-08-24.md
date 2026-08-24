# M0 Governance Contract 正式评审（2026-08-24）

> Verdict：**Approved for M1A proposal** · Runtime Governance Coverage：**0%**
>
> 该结论只批准 Governance Contract 作为下一阶段设计输入，不批准 M1A 实施、不宣称生产 PEP 已接入，也不改变现有授权行为。

## 1. 评审范围

本次评审覆盖：

- `PrincipalContext v1`
- `DelegatedMandate v1`
- `PolicyDecision v1`
- `ResourceRef v1`
- `DatasourceBinding v1`
- `RegistryBinding v1`
- Governance Action Catalog v1.1.0
- Web Human、Feishu Human、Agent 三条 review-only Query trace
- 跨 Contract 语义门禁、威胁模型、legacy 迁移与回滚边界

不在本次评审范围：

- Task API、TaskRun/QueryRun 数据库 Schema 和生产 PEP。
- Membership/Role/Policy Store 的最终存储实现。
- Coordination `DecisionRecord`、Economics、Context 或 OAuth Runtime。
- 多 Approver、复杂职责分离、SSO/SCIM 和 HA。

## 2. 证据

| 证据 | 位置 | 结论 |
|---|---|---|
| 权威 JSON Schema | `agent/contracts/*-v1.schema.json` | 对象形状、类型、最小披露和禁止最终 Agent authority |
| TypeBox parity | `services/pi-orchestrator/src/governance-contracts.ts` | Python/TypeScript 消费共享正反 fixture |
| 完整 review trace | `agent/contracts/governance-review-fixtures.v1.json` | Web、飞书和 Agent 请求映射到同一治理链 |
| Python 语义门禁 | `agent/contracts/governance_semantics.py` | 返回稳定、失败关闭的 reason code |
| TypeScript 语义门禁 | `validateGovernanceReviewTrace` | 对同一 mutation corpus 返回相同预期 reason code |
| Action Catalog | `governance-action-catalog.v1.json` | Contract Coverage 100%，Runtime Coverage 0% |

`approval_snapshot`、`query_lineage` 和 `request_binding` 是 M0.5 的 review composition records，用于证明连续性；它们不是冻结的生产 Contract，也不替代 M0.4/M3 的 `DecisionRecord` 或 M1 的请求 Envelope。

## 3. 已验证不变量

1. Actor 可为 Service/Agent，但最终 accountable principal 和 decision authority 只能是 Human/Team/Organization。
2. Service/Agent Actor 必须有一条与 PrincipalContext 匹配的 task-scoped DelegatedMandate；v1 不允许递归委托。
3. Principal、Mandate、DatasourceBinding、RegistryBinding、Action Resource 必须处于同一 Organization/Workspace。
4. Principal、delegation、Mandate、Policy、Binding、approval 和 request binding 必须在 `as_of` 时刻有效。
5. Mandate 必须覆盖准确 Task、Audience、Purpose、Capability 和 Resource revision。
6. 非空 `budget_ref` 在没有 Budget resolver 时失败关闭，不能把“已引用”误解为“已授权”。
7. Policy subject、Mandate、Action、Resource、effect 和 revision 必须与请求及 Binding 一致。
8. `query.execute` 必须有 approval obligation 和合法 Human/Team/Organization approval snapshot。
9. Registry 必须锚定准确 DatasourceBinding，Registry/Policy revision 不能漂移。
10. SQL hash、Assurance hash、Approver、QueryRun、Task 和 prepare→approve→execute 时间顺序必须连续。
11. 请求必须绑定 Task、Audience、短期有效期和 idempotency key hash。
12. Economics/Context 未定义时显式为 `null`，不得通过任意 metadata 暗示授权或证据。

共享 mutation corpus 覆盖跨租户、过期、撤销、跨 Task/Audience/Purpose、Capability/Resource 扩权、Policy/Binding 漂移、Agent 审批、SQL/Assurance 漂移、时间倒序、请求重用和隐式 Context 等失败路径。

## 4. Threat Model

| Threat | M0 Contract/Review 控制 | M1 实施义务 | 当前结论 |
|---|---|---|---|
| 请求体伪造 org/team/user | PrincipalContext 与服务端映射是授权输入，请求标签不构成授权 | 从认证上下文派生 Principal，忽略/拒绝不一致请求字段 | Contract 足够，Runtime 未实现 |
| Service Key 被重放 | Task/Audience/expiry/request ID/idempotency 形成最小绑定 | TLS、凭证轮换、短期 Envelope、持久 idempotency/replay store；高风险 Action 不自动重放 | **M1A 高优先义务** |
| 跨 Task/Audience 委托 | Mandate 和 request binding 同时绑定 Task/Audience | Pi/Forge 两侧都校验，任一漂移 403/404 | Contract 足够 |
| Capability/Resource 扩权 | 精确 capability 与 versioned ResourceRef scope | PEP 计算 subset，不接受自由文本/metadata 扩权 | Contract 足够 |
| 默认允许 ACL | active Binding 和 PolicyDecision 是执行前置 | 企业 profile deny-by-default；缺失 Binding/Policy 拒绝 | **M1A 阻断门禁** |
| 跨租户枚举 | 所有 ResourceRef 含 Organization/Workspace | Store 查询先加 scope，越权资源统一 404 | **M1A 阻断门禁** |
| 过期/撤销 Mandate | status 与时间语义失败关闭 | PEP 按服务端时钟及 Mandate Store revision 重验 | Contract 足够 |
| Context 跨 Purpose 泄露 | Mandate/Action 固定 Purpose；Context 扩展当前为空 | ContextBundle 出现前必须绑定 Purpose/Principal/Mandate/Task | 后续 Context 阶段义务 |
| 预算绕过 | 非空 budget_ref 无 resolver 时 review validator 拒绝 | Budget Runtime 出现前不得把 budget_ref 作为已结算证明 | 后续 Economics 阶段义务 |
| SQL/Binding 漂移复用旧审批 | SQL、Assurance、Registry、Policy、Binding lineage 必须相同 | Forge 在执行原子 claim 时重验 hash/revision/expiry | 现有部分控制可复用 |
| legacy 身份伪造 | Contract 不允许无来源的 Principal snapshot | 无法可靠映射的旧 Task 进入 needs_input/expired，不静默升级 | **M1A 迁移义务** |
| Agent 自批自执行 | Agent 不可成为 decision authority；approval authority 类型受限 | PEP 区分 Agent Action 与最终 Human/Team/Org Decision | Contract 足够 |

重要边界：PrincipalContext/Mandate 本身不提供密码学防重放。防重放需要认证传输、短期请求 Envelope、持久 idempotency/nonce 状态和副作用原子 claim 共同完成，不能仅靠增加一个 JSON 字段宣称解决。

## 5. 迁移与兼容设计

### 5.1 目标单一真相源

```text
External IdP / local identity
→ Pi Principal + Membership + Mandate Store
→ TaskRun v2 只保存 immutable ref/revision/hash
→ Forge PEP 重验 Principal/Mandate/Binding/Policy
→ Forge QueryRun 保存执行 lineage
```

不建立共享可写的“统一授权数据库”。Pi 持有 Task delegation；Forge 持有数据资源 Policy/Binding 和执行审计。

### 5.2 `legacy_single_user` 兼容

- 仅个人私有部署 profile 可显式启用，企业 profile 禁止。
- 初次迁移从现有 Web/Channel Identity Map 和 tenant mapping 生成候选 Principal/Membership，由管理员确认后写入新 Store。
- 旧 Task 不根据 `org_id/team_id/user_id` 字符串自动伪造高保证 PrincipalContext。
- 能可靠映射且没有在途高风险 Action 的旧 Task 可固定 legacy snapshot；不能映射的任务进入 `needs_input` 或 `expired`。
- 在途 QueryRun 保留现有 SQL/Assurance/approval hash，不自动获得新权限；需要继续时重新准备或重新审批。

### 5.3 TaskRun v2 最小演进

建议增加：

```text
principal_context_ref + principal_context_hash
mandate_ref + mandate_revision + mandate_hash（Human direct Action 可为空）
authz_contract_version
authz_mode = legacy_single_user | governed_v1
```

Task Store 不保存 token、Service Key、完整 OIDC claim 或可变 Membership/Policy 文档。

### 5.4 切换顺序

1. 先增加只读解析和 shadow evaluation，结果只进受限审计，不影响当前 Action。
2. 建立新 Principal/Membership/Mandate Store，并迁移已确认身份。
3. Pi Task creation 固定 v2 ref/hash；旧 Task 继续走显式 legacy mode。
4. Forge 在企业 profile 启用 PEP deny-by-default，先 `query.prepare`，再 `approve/execute`。
5. 每个 Action 通过生产负向门禁后，逐项把 Catalog runtime status 从 `not_integrated` 改为 `partial/enforced`。
6. legacy 路径达到退出门禁后只读保留，不进行长期双写。

### 5.5 回滚

- 新 Schema 使用 additive migration；回滚代码不得删除 v2 字段或新 Store。
- Feature flag 只允许个人部署回退到 `legacy_single_user`；企业 profile 失败时保持 deny，不回退默认允许。
- 在途 governed Task 回滚后进入 `needs_input/expired`，不把新 Mandate 静默翻译成旧 user 标签。
- Query/SQL 副作用不重放；已 claim 的执行按现有 lease/reconciliation 收口。
- 回滚前后保留 immutable Audit、QueryRun 和 Contract hash，以便解释授权路径。

## 6. Findings 与 Verdict

### Blocker

无 Contract 形状或跨对象连续性 blocker。

### M1A 实施义务（不改变本次 Approved 结论）

1. Service identity、短期请求 Envelope、idempotency/replay store 必须一起设计，不能只传 PrincipalContext JSON。
2. 企业 profile 缺少 ACL/Binding/Policy 时必须 deny-by-default。
3. TaskRun v2 必须采用 ref/revision/hash，不能复制凭证或把请求体身份作为授权真相。
4. 跨租户 Store 查询必须先 scope 后 lookup，并通过统一 404/403 负向测试。
5. 每个 Action 的 Runtime Governance Coverage 只能在生产 PEP 和迁移门禁通过后提升。

### 非阻塞后续项

- `DecisionRecord` 的正式 Contract 在多人审批/M3 前冻结；M1A 先复用现有 hash-bound QueryRun approval。
- Economics/Context Contract 按真实消费者 Just-in-Time 设计。
- M0 review validator 只用于 Contract 评审；M1 PEP 应实现领域化校验和审计，不直接把 test composition object 作为生产 API。

### 最终结论

**Approved for M1A proposal**。

该结论表示 M0 Governance 内核足以进入一个单独、可回滚的 M1A 方案评审。M1A 仍需用户明确批准；在其生产 PEP 完成前，Runtime Governance Coverage 保持 **0%**。
