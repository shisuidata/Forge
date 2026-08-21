# Forge Pi Orchestrator

受限的 Pi Agent Runtime，负责 Forge 平台的 TaskRun、Skill 调度和阶段推进。

## 职责边界

- Pi Orchestrator 拥有流程调度权。
- Forge Python 服务拥有查询规划、编译、审批验证和数据库执行权。
- 本服务不持有数据库凭证，不启用 Pi 内置 `bash/read/write/edit` 工具。
- 当前仅授权四个 MVP Skills 和 `forge_prepare_query`；个人全局 Skills、Extensions 和 AGENTS.md 不加载。
- `forge_prepare_query` 只能返回待审核 SQL，不能批准或执行。

详细设计见：

- `../../docs/platform-architecture.md`
- `../../docs/pi-forge-integration-plan.md`

## 本地验证

```bash
npm install
npm run typecheck
npm test
```

启动服务：

```bash
npm start
curl http://127.0.0.1:4310/health/readiness
```

Integration Spike Task API：

```text
POST /v1/tasks
GET  /v1/tasks/{task_run_id}
GET  /v1/tasks/{task_run_id}/events
POST /v1/tasks/{task_run_id}/clarify
POST /v1/tasks/{task_run_id}/review-metric
GET  /v1/tasks/{task_run_id}/artifacts
GET  /v1/tasks/{task_run_id}/attempts
GET  /v1/tasks/{task_run_id}/presentation
POST /v1/channel-events
POST /v1/tasks/{task_run_id}/prepare-query
POST /v1/tasks/{task_run_id}/approve-query
POST /v1/tasks/{task_run_id}/analyze
POST /v1/tasks/{task_run_id}/supplements
POST /v1/tasks/{task_run_id}/resume-analysis
POST /v1/tasks/{task_run_id}/render-report
```

所有长耗时 POST Stage 都支持请求体 `{"async": true}`。异步模式返回 `202 Accepted` 以及 Task/Event/Artifact/Attempt polling URL；不传时保留同步响应作为迁移期兼容。渠道应优先使用异步模式，不要依赖长 HTTP 连接。

`prepare-query` 创建持久化 Forge QueryRun；`approve-query` 只能提交与 ReviewRequest 完全匹配的 `query_run_id + sql_hash`。实际安全校验和只读执行仍由 Forge 完成。`incomplete` 分析最多允许从最新 `suggested_queries` 选择一次补查；补查创建 child TaskRun，仍需独立 SQL 审批，完成后父任务合并两个 QueryResult 重新分析。

环境变量：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `PI_ORCHESTRATOR_HOST` | `127.0.0.1` | 监听地址 |
| `PI_ORCHESTRATOR_PORT` | `4310` | 监听端口 |
| `SHISUI_DATA_SKILLS_DIR` | Forge 同级的 `拾穗 DATA` | 固定版本 Skills 包路径 |
| `PI_ORCHESTRATOR_AGENT_DIR` | 服务目录 `.runtime` | 隔离的 Pi 配置目录 |
| `PI_ORCHESTRATOR_STATE_DB` | `<agentDir>/state/orchestrator.sqlite3` | Task/Event/Artifact/StageAttempt/ChannelEvent SQLite 真相源；生产环境挂载持久卷 |
| `PI_CHANNEL_IDENTITY_MAP` | `<agentDir>/channel-identities.json` | 飞书/钉钉外部用户到组织身份的只读映射；修改后重启加载 |
| `PI_CHANNEL_SERVICE_KEYS` | 空 | Channel Adapter 调用 `/v1/channel-events` 的服务端密钥列表 |
| `PI_STAGE_TIMEOUT_MS` | `240000` | 单个模型或 Forge Stage 的执行超时 |
| `PI_STAGE_LEASE_MS` | `300000` | StageAttempt lease；必须大于 Stage timeout |
| `PI_RECONCILIATION_INTERVAL_MS` | `30000` | 回收过期 StageAttempt 的扫描间隔 |
| `FORGE_BASE_URL` | `http://127.0.0.1:8000` | Forge 可信执行层地址 |
| `FORGE_API_KEY` | 空 | 仅旧 `/api/prepare-query` 使用的普通 Forge API Key |
| `FORGE_PI_SERVICE_KEY` | 空 | 内部 QueryRun API 专用服务密钥；必须匹配 Forge `PI_SERVICE_API_KEYS` |
| `FORGE_REQUEST_TIMEOUT_MS` | `130000` | Forge 请求超时 |
| `PI_MODEL_PROVIDER` | 空 | 专用 Pi Runtime 的模型 Provider；必须和 `PI_MODEL_ID` 同时配置 |
| `PI_MODEL_ID` | 空 | 专用 Pi Runtime 的模型 ID；模型和凭证只从 `PI_ORCHESTRATOR_AGENT_DIR` 加载 |

火山方舟 Coding Plan 本地配置：

```bash
mkdir -p .runtime
cp models.coding-plan.example.json .runtime/models.json

PI_MODEL_PROVIDER=volcengine-coding-plan \
PI_MODEL_ID=ark-code-latest \
node --env-file=../../.env --import tsx src/server.ts
```

模板只引用 `$ARK_API_KEY`，不保存或回显 Key。生产环境应由服务编排器单独注入该变量，不要挂载包含其他业务凭证的完整 `.env`。`/health/readiness` 只有在专用模型可用时返回 `status: ok`，否则返回 `degraded`。

当前服务提供健康检查、Runtime 能力检查、SQLite Task/Event/Artifact/StageAttempt/ChannelEvent Store 和 Task API；内存 Store 仅用于单元测试和显式注入。正式状态库启用 WAL、foreign keys 和 5 秒 busy timeout，当前 schema version 为 3；遇到更高版本会拒绝启动，禁止用旧服务降级打开新数据库。备份应使用 SQLite 在线备份能力，或停服后复制数据库文件；不要只复制运行中的主文件而遗漏 WAL。等待审批、`incomplete`、`ready_for_analysis` 和 `ready_for_report` 等安全暂停状态可跨进程恢复。Analysis、Report 和 QueryRun approval 已绑定持久化 Attempt/Lease；过期 lease 只恢复到可重试状态并写审计事件，不自动重放模型或 SQL。

四个 MVP Skills 都使用隔离 Pi Session，并且只能通过终止型 Structured Output Tool 提交 Artifact。分析发现必须引用实际 QueryRun 行，报告只能复用 AnalysisArtifact finding 和证据；Markdown 由服务端确定性渲染。Pi 只负责调度批准动作；QueryRun、审批记录和查询结果由 Forge 持久化。

Forge Web 可在设置 `PI_ORCHESTRATOR_ENABLED=true` 后访问 `/tasks`，审核 hash 绑定的 SQL 并查看只读执行结果。

## Channel Adapter

复制 `channel-identities.example.json` 到 `PI_CHANNEL_IDENTITY_MAP` 指定位置，并为 Bot 单独注入与 `PI_CHANNEL_SERVICE_KEYS` 匹配的 `PI_CHANNEL_SERVICE_KEY`。`POST /v1/channel-events` 必须携带 `X-Channel-Service-Key`；未知飞书 `open_id` 或钉钉 `user_id` 会失败关闭。`(channel,event_id)` 在 SQLite 中唯一，平台重试不会创建第二个 TaskRun 或重复批准。

飞书迁移由 `FEISHU_PI_ENABLED=true` 开启。HTTP webhook 会加载无 Forge 执行层依赖的 `web/feishu_pi.py`；WebSocket 模式应运行 `python -m web.feishu_pi`。新消息和新 Pi 卡片只走 ChannelEvent → Pi Task API，旧卡片会提示失效。关闭开关可回滚旧 Bot；两条路径不能同时消费同一飞书应用事件。
