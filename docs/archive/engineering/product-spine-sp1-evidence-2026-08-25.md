# Product Spine SP1 Evidence（2026-08-25）

> Requirement: `REQ-2026-08-25-017` · Scope: Pi read-only Product Projection · Verdict: PASS

## 1. 实施范围

SP1 只增加 Pi 只读查询、Projection Builder 和内部 Product API：

- `TaskStore.list` 增加可选 user scope；
- `TaskStore.listChildren`；
- `TaskStore.listConversations` / `getConversation`；
- Conversation list/detail opaque cursor；
- `ProductProjectionService`；
- `GET /v1/conversations`；
- `GET /v1/conversations/{conversation_id}`；
- `GET /v1/tasks/{task_run_id}/detail`。

未增加数据库表或 schema version，未修改 Task 状态机、Forge QueryRun、Web 页面或部署。

## 2. Truth Source 与恢复

Conversation 仍由以下真实 Task 数据只读重建：

```text
org_id + team_id + user_id + channel + channel_conversation_id
→ ordered TaskRuns
→ TaskEvent / Artifact / StageAttempt / ChannelPresentation
```

不存在可写 Conversation Store。SQLite reopen 测试证明 Conversation grouping、parent/child、Task order 和 scope 仍可恢复。

Task 创建与 transition 时间改为单 Store 内单调递增，避免同一毫秒内连续消息的 first/latest 顺序依赖随机 Task ID；未修改持久化字段或表结构。

## 3. Product Projection 行为

### Conversation list/detail

- list 最大 50 个 Conversation；
- detail 每页最大 100 个 Task entries；
- list/detail cursor 均为 opaque base64url；
- title 来自首个 Task 的原始问题；
- state 和 preview 来自最新更新 Task 的真实 Presentation；
- 超过 100 entries 返回 `partial + conversation_entries_truncated + next_cursor`；
- source revisions 使用 bounded SHA-256 aggregate，不因 Task 数量丢失覆盖。

### Task Detail

聚合：

- Task summary；
- latest ExecutionPlan；
- exact Query Review：SQL、`sql_hash`、`assurance_report_hash`、expiry、read-only；
- bounded Presentation；
- typed ActionCapability；
- 最后 200 条 Activity；
- 最后 100 个 Attempt/Artifact；
- Evidence refs；
- parent/child relation；
- source revisions、partial reasons 和 redactions。

Query review 缺少 SQL/hash/Assurance lineage 时返回 bounded conflict，不生成可审批假页面。

## 4. 安全边界

- 三个新 API 均要求 `X-Channel-Service-Key`；
- org/team/user/channel 全部服务端过滤；
- cross-scope Task/Conversation 返回 404/empty，不披露对象；
- 响应设置 `Cache-Control: no-store`；
- Task Detail/Conversation Detail 单对象序列化上限 2 MB；
- error、Secret-like 行和本地路径不进入业务 Projection；redaction reason 可见但不回显原值；
- Artifact/Presentation 引用不一致失败关闭；
- Action 不携带自由 endpoint 或模型生成执行 payload。

剩余边界：Channel Service Key 仍是受信服务身份，不绑定最终 Web Principal；SP2 Web BFF 必须继续执行已有 admin scope 和 channel gate。Runtime Governance Coverage 仍为 0%。

## 5. 10K Task 性能证据

基准：10,000 Task、1,000 Conversation、每个 10 Task，SQLite WAL，单事务准备数据，Conversation list `limit=20`，连续 10 次。

首版 SQL 使用每组相关子查询和 N+1 Task 查询，单次约 **19,040 ms**，判定不可接受。停止并修复 root cause，没有直接添加重复字段或新索引。

修正后使用单次 scoped CTE + window rank + grouped first/latest，并让 list query 不读取 detail Task rows：

- 数据准备：约 `121.2 ms`；
- 10 次 list：`26.3–27.9 ms`；
- 平均：约 **26.8 ms**。

因此 SP1 不新增 schema v5 列或索引；若真实 Atlas 数据和并发下出现回归，再基于证据迁移。

## 6. 自动验证

- Pi TypeScript：`114 passed`；
- TypeScript strict typecheck：PASS；
- Product Projection schema generation sync：PASS；
- SQLite restart/scope/conversation tests：PASS；
- API auth/scope/no-store/bad cursor tests：PASS；
- 101-entry Conversation pagination：PASS；
- broken Query Review lineage：fail closed；
- Python 全量：`569 passed / 24 skipped`，SP0 contract parity 保持通过；
- `git diff --check`：PASS。

## 7. Verdict 与 SP2 入口

SP1 PASS。Pi 已能从真实状态生成稳定 Conversation/Task Product Projection，但 Web 尚未获得统一 Product BFF，ReportStore 尚无 scope-aware list。

SP2 只允许：

1. `ReportStore.list(...)`；
2. authenticated `/api/product/*` BFF；
3. Workspace 聚合与 dependency partial/offline；
4. typed action 继续复用现有 endpoint；
5. Python semantic gate 与 BFF scope tests。

SP2 不开始 Product Shell、不复制 Pi Task/Report 状态、不开放 Agent Runtime。
