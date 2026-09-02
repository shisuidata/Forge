# Product Spine SP2 Evidence（2026-08-25）

> Requirement: `REQ-2026-08-25-017` · Scope: Product BFF + scoped Report Index · Verdict: PASS

## 1. 实施内容

### ReportStore

- 新增 `ReportStore.list(...)`；
- scope 固定为 org/team/user；
- 支持 status、limit、`updated_at + report_id` cursor；
- 新增 `idx_reports_scope_updated`；
- 返回仍来自原 ReportStore，不复制 Report 状态。

### Product BFF

新增 authenticated read-only API：

```text
GET /api/product/workspace
GET /api/product/conversations
GET /api/product/conversations/{conversation_id}
GET /api/product/tasks
GET /api/product/tasks/{task_run_id}
GET /api/product/reports
GET /api/product/data-summary
```

BFF 只调用 Pi Product API、Pi Task list、ReportStore 和 Registry 文件；没有 Product 数据库或可写状态。

### Contract 补充

SP2 在真正暴露 Task list 前补充同一 Product Projection v1 的 `TaskSummaryV1`。Task list 不再把 raw TaskRun metadata 暴露给前端；TypeBox、generated JSON Schema、Python semantic parity 和共享 fixtures 同版本更新。

## 2. Scope 与认证

- `/api/product/*` 使用已有 `require_api_auth`，支持登录 Session 或受控 API Key；
- org/team 必须属于 `PI_WEB_ADMIN_TASK_SCOPES`；
- 当前 Human user 固定为现有 Web identity `web_admin`，channel 固定为 `web`；
- BFF 不只信任 Pi 返回：每个 Conversation/Task Projection 再次检查 scope；scope mismatch 返回 bounded 502，不转交数据；
- Report SQL list 自身按 org/team/user 过滤；
- 所有正常 Product JSON 响应 `Cache-Control: no-store`。

该切片仍是单用户私有化 Human Control Plane。Channel Service Key 和 `web_admin` 尚不是 M1A Principal/Mandate Runtime，Runtime Governance Coverage 仍为 0%。

## 3. Workspace partial/offline

Workspace v1 聚合：

- needs input；
- waiting decision；
- running；
- failed；
- recent reports；
- dependency status。

任一依赖不可用时不会把整个工作台伪装为 ready：

- Pi offline：Task sections 缺失，但 Report/Data 仍可显示；
- ReportStore offline：Task 仍可显示；
- Registry 部分损坏：counts 不编造，显示 partial；
- Task 达到 100 或 Report 达到 20 的工作台读取上限：显示 bounded partial reason。

只有 Pi、Report、Registry 均不可用时 Workspace 才为 offline；否则按可用内容返回 partial。

## 4. Bounds、lineage 与去敏

- Conversation 和 Task 上游对象再次执行 Python Product Projection semantic gate；
- Task list 使用 `TaskSummaryV1`，不返回 metadata、correlation、raw error、model revision 或 Secret-like 行；
- Report Summary 不返回 `error`，只记录 `sensitive_error_redacted`；
- Report list 最大 100 并支持 cursor；
- Task list最大 100，当前明确返回 `bounded=true` 和 `truncated_possible`，不冒充全量；
- Registry `source_revision` 是 Schema + Metrics 内容的确定性 SHA-256，不使用请求时间冒充 revision；
- Pi invalid/non-object JSON 统一转为 bounded 502。

## 5. 验证

- Python 全量：`575 passed / 24 skipped`；
- SP2 定向：Product BFF、Reporting、Pi Web proxy、Auth 共 `34 passed`；
- Pi TypeScript：`114 passed`；
- strict TypeScript typecheck：PASS；
- npm audit：0 vulnerabilities；
- JSON Schema generation/parity：PASS；
- auth enabled 未认证请求：401；
- invalid Product Contract：502；
- upstream scope mismatch：502；
- unauthorized configured scope：404；
- Report cursor/scope：PASS；
- Pi offline + Report ready Workspace：partial，报告仍可见；
- missing Registry：partial，不伪造表数量；
- `git diff --check`：PASS。

## 6. 未做与剩余风险

- 未开始 Product Shell、CSS/JS 或页面；
- 未实现 Agent Runtime、M1A、通用 Decision Runtime；
- Task list v1 暂无 cursor，已明确 bounded/truncated；SP4 若真实使用超过 100 Task，必须先补 cursor，不能隐藏截断；
- BFF 依赖现有 `web_admin + configured scopes`，不能宣称多用户租户隔离；
- Product BFF 尚未在 Atlas 集成环境连接真实 Pi/Report DB；该门禁属于 SP5。

## 7. Verdict 与 Backend Gate

SP2 PASS。Backend Gate 已满足：

```text
Product Contract 固定
+ Conversation 可恢复
+ Task Detail 可聚合
+ Report 可 scope-list
+ Workspace 可 partial/offline
+ Auth/scope/lineage 负向测试通过
+ 无第二真相源
```

允许进入 SP3 Product Shell Foundation。SP3 只建立本地静态资产、模板 Shell、导航和统一页面状态，不提前实现完整业务页面或修改现有 Admin 真相源。
