# Forge Pi Orchestrator

受限的 Pi Agent Runtime，负责 Forge 平台的 TaskRun、Skill 调度和阶段推进。

## 职责边界

- Pi Orchestrator 拥有流程调度权。
- Forge Python 服务拥有查询规划、编译、审批验证和数据库执行权。
- 本服务不持有数据库凭证，不启用 Pi 内置 `bash/read/write/edit` 工具。
- 生产 Skills 的 Stage 只能返回候选；SQL 必须经过已签发任务授权、Forge Assurance、QueryRun v2 claim 后由 Forge 执行。

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

受管 Task API（`/v1/tasks` 写请求须 Forge 签发的短时控制票据，且 Pi 从 Forge 回查主体、scope 与权限；`/v1/internal/task-scopes/{id}` 仅受信 Channel Service 可读取）：

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

耗时 Stage 推荐 `{"async": true}` 与 polling；Pi 只转送 Forge QueryRun v2 的审批与结果，不创建第二套执行授权。`prepare-query` 需要已绑定且仍有效的 Mandate，非自主表进入指定真人审核；`approve-query` 必须由指定审核人以完整 hashes 确认。补查 child TaskRun 须独立授权，不继承执行权。

环境变量：

| 变量 | 默认值 | 用途 |
|---|---|---|
| `PI_ORCHESTRATOR_HOST` | `127.0.0.1` | 监听地址 |
| `PI_ORCHESTRATOR_PORT` | `4310` | 监听端口 |
| `SHISUI_DATA_SKILLS_DIR` | Forge 同级的 `拾穗 DATA` | 固定版本 Skills 包路径 |
| `PI_ORCHESTRATOR_AGENT_DIR` | 服务目录 `.runtime` | 隔离的 Pi 配置目录 |
| `PI_ORCHESTRATOR_STATE_DB` | `<agentDir>/state/orchestrator.sqlite3` | Task/Event/Artifact/StageAttempt/ChannelEvent SQLite 真相源；生产环境挂载持久卷 |
| `PI_CHANNEL_SERVICE_KEYS` | 空 | Channel Adapter 与 Forge Task-scope lookup 的受信通道密钥；未知渠道用户必须先由管理员显式登记绑定 |
| `PI_STAGE_TIMEOUT_MS` | `240000` | 单个模型或 Forge Stage 的执行超时 |
| `PI_STAGE_LEASE_MS` | `300000` | StageAttempt lease；必须大于 Stage timeout |
| `PI_RECONCILIATION_INTERVAL_MS` | `30000` | 回收过期 StageAttempt 的扫描间隔 |
| `FORGE_BASE_URL` | `http://127.0.0.1:8000` | Forge 可信执行层地址 |
| `FORGE_PI_SERVICE_KEY` | 空 | Forge 中注册的 `pi_service` 凭证；仅用于服务间请求，不是人类管理员密钥 |
| `PI_EXECUTION_PRINCIPAL_ID` | 空 | Forge 中注册的 Pi service principal，必须与凭证身份及任务授权执行者一致 |
| `FORGE_REQUEST_TIMEOUT_MS` | `220000` | Forge 请求超时 |
| `PI_MODEL_PROVIDER` | 空 | 专用 Pi Runtime 的模型 Provider；必须和 `PI_MODEL_ID` 同时配置 |
| `PI_MODEL_ID` | 空 | 专用 Pi Runtime 的模型 ID；模型目录只从 `PI_ORCHESTRATOR_AGENT_DIR` 加载 |
| `PI_MODEL_SECRET_REF` | 空 | 可选 `file-env:/absolute/mode600.env#VARIABLE`；只读取指定变量并映射为 Runtime 的 `ARK_API_KEY`，不复制、不响应、不持久化其他变量 |

火山方舟 Coding Plan 本地配置：

```bash
mkdir -p .runtime
cp models.coding-plan.example.json .runtime/models.json

PI_MODEL_PROVIDER=volcengine-coding-plan \
PI_MODEL_ID=ark-code-latest \
node --import tsx src/server.ts
```

模板只引用 `$ARK_API_KEY`，不保存或回显 Key。本地单独注入模型凭证，或用受限 `PI_MODEL_SECRET_REF` 指向只包含该变量的 0600 文件；Pi→Forge 另需注册 `pi_service` 凭证和执行身份。不要挂载完整业务 `.env`。`/health/readiness` 的运行时/治理降级不能当作已具备授权查询能力。

当前服务提供健康检查、Runtime 能力检查、SQLite Task/Event/Artifact/StageAttempt/ChannelEvent Store 和 Task API；内存 Store 仅用于单元测试和显式注入。正式状态库启用 WAL、foreign keys 和 5 秒 busy timeout，当前 schema version 为 4；遇到更高版本会拒绝启动，禁止用旧服务降级打开新数据库。备份应使用 SQLite 在线备份能力，或停服后复制数据库文件；不要只复制运行中的主文件而遗漏 WAL。等待审批、`incomplete`、`ready_for_analysis` 和 `ready_for_report` 等安全暂停状态可跨进程恢复。Analysis、Report、Advisory Skill 和 QueryRun approval 已绑定持久化 Attempt/Lease；过期 lease 只恢复到可重试状态并写审计事件，不自动重放模型或 SQL。

Skills 以隔离 Pi Session 生成结构化 Artifact；只有已绑定任务授权的查询可进入 Forge 的 QueryRun v2。团队 Skill Policy 由真实管理员的受信控制票据管理；Pi 模型修订绑定 StageAttempt，报告仅复用已有 Evidence。Forge Web 的 `/tasks` 提供任务授权和指定真人审核，渠道是投影，不自行批准查询。

### State 与阶段恢复契约

- `OrchestratorApplication` 只接收一个 `OrchestratorState`：Task、Event、Artifact、Attempt、ChannelEvent、Skill Policy 与事务同属一个 adapter，禁止独立 Store 与空事务拼装。SQLite 使用嵌套 SAVEPOINT；显式内存 adapter 具有同等回滚语义。
- 默认重试创建新 Attempt 和下游 command；显式成功 key 回放已持久结果，失败、运行中或跨阶段 key 在迁移 Task 前拒绝。用户取消 abort 活跃 signal；超时或调用方中断恢复可重试状态，迟到结果不写 Artifact。Abort 只保证停止本地等待，不宣称撤销已发生的远端 SQL 或发布副作用。
- ExecutionPlan 是 Task 的显示进度，不是第二套调度器。失败、取消、过期恢复随 Task 在同一事务收口；不完整分析保持安全暂停，失败补查不消费子任务证据。

### Benchmark 生命周期与诊断

- Benchmark 与关联 Task 共用一个 SQLite 连接及事务：准入失败不留下半个 Task/Run；运行、完成、失败、停止和中断同步投影到 Task。case 评价、固定预算和禁止模型自动重放的规则不变。
- 当前是单机 SQLite、单活跃 Benchmark owner，不是 HA。owner 绑定进程 ID/主机；第二个存活 owner 只能读取，不能中断或接管；仅明确释放或确认旧进程已退出后恢复。未知主机/进程权限失败关闭。`Server.close` 等待在途 Benchmark 请求和 worker 结束后释放连接；进程意外退出通过下次启动收口，不自动续跑。
- context、evaluation 和准入 HTTP 请求统一使用 `FORGE_REQUEST_TIMEOUT_MS`；stop 将 abort 传播到挂起请求。重启把未完成的冻结运行标为 interrupted，不重放付费调用；历史未冻结运行只读。
- `X-Request-ID` 在 Web → Pi → Forge 链路传递，默认日志只记录安全 ID、状态与耗时，不记录 SQL、请求内容、结果行或凭证。客户端遇到响应丢失应回查原 Command/Task，再沿用同一 command ID；投递结果未知不等于请求未执行。
- Structured Artifact 由 `contracts.ts` / TypeBox 统一定义，`npm run export:structured-contracts` 导出 Python schema；共享语义语料同时覆盖行数、行宽、时间和内容边界。Stage 扩展使用明确 descriptor 与类型化执行方法，不自动获得新的审批或数据库执行权。


## Channel Adapter

飞书与钉钉用户须在 Forge 管理界面显式登记外部身份绑定。Adapter 使用 `PI_CHANNEL_SERVICE_KEY` 调用受信 ChannelEvent API；未知或撤销绑定失败关闭，不能首用户自动认领。渠道事件去重不创建第二个 TaskRun，不代替真实人的 QueryRun 审批。

渠道投递 receipt 独立于 Pi Task 真相。`GET /api/pi/tasks/{task_run_id}/deliveries` 按登录主体 scope 查看，`POST /api/pi/deliveries/{delivery_id}/retry` 只重送原 presentation，不重新执行 SQL。投递未知不冒充成功。
