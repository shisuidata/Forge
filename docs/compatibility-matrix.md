# Forge 兼容性矩阵

Forge 的兼容性分四层看：SQL 编译、数据库结构同步、SQL 执行、Agent/LLM 接入。不要把“能编译某方言 SQL”等同于“已经完整支持该数据平台生产交付”。

## 数据库 / 数据仓库

证据状态口径：

- **planned**：只进入路线图，不对外交付承诺。
- **implemented**：仓库已有代码路径，但还不能作为生产交付承诺。
- **smoke_verified**：有本地测试、CI smoke 或真实 provider/database smoke 证据。
- **production_verified**：有客户或生产环境交付证据，可作为商业交付承诺。

| 平台 | SQL 编译 | `forge sync` | 查询执行 | 交付状态 | 当前建议 |
|---|---|---|---|---|---|
| SQLite | smoke_verified | smoke_verified | smoke_verified | PoC 主线 | 本地 demo、测试、单机 PoC |
| PostgreSQL | smoke_verified | smoke_verified | smoke_verified | 生产首选 | 私有化部署首选关系型数据库 |
| MySQL / MariaDB | smoke_verified；不支持 `FILTER` 时显式报错 | smoke_verified | smoke_verified | PoC 可用 | 需保留真实库回归证据 |
| BigQuery | implemented | planned | planned | partial | 先补 dry-run 和资源上限 |
| Snowflake | implemented | planned | planned | partial | 先补 warehouse/resource guardrail |
| ClickHouse / Doris / StarRocks / DuckDB | planned | planned | planned | 不承诺 | 先建立方言差异清单 |

SQLite、PostgreSQL 16、MySQL 8 共用 `tests/test_database_compatibility.py`，覆盖 schema introspection、枚举采样、目标方言编译、只读查询、行数上限和写 SQL 拒绝。CI 的 `compatibility` job 分别连接三个真实数据库运行同一套 smoke；只有 CI 绿灯的平台才能对外声明对应层级已验证。

`forge sync` 通过 SQLAlchemy 正确引用保留字、大小写或带空格的表名和字段名。该能力只代表结构同步成功；Forge 编译器和目标数据库是否能执行这些非标准标识符，仍需单独验证，不能据此宣称完整查询支持。

数据库适配层契约已在 `forge/adapters.py` 固定为 `DatabaseAdapter`：`introspect`、`compile_dialect`、`dry_run`、`execute` 和 `capabilities`。新增数据库必须先声明 capabilities，再补 mock 单测和真实 smoke；BigQuery / Snowflake 在具备 dry-run 与资源上限前保持 partial。

## Agent 入口

| 入口 | 状态 | 说明 |
|---|---|---|
| Web Chat / Admin | smoke_verified | 适合数据团队内部 PoC 和 Registry 管理 |
| `POST /api/prepare-query` | implemented | 外部 Agent 只获取待审核 SQL，不执行数据库 |
| 飞书 Bot | implemented | 适合国内团队协同场景，真实交付前需重跑 smoke |
| Pipeline API | implemented | 支持分析、可视化、报告类流程，仍需客户域验收 |
| 钉钉 / 企业微信 / Slack | planned | 建议抽象消息入口适配层后再做 |
| MCP / OpenAI Agents / Claude Desktop | planned | 中期项；第一版只接 `prepare_query`，返回待审核 SQL，不执行数据库 |

## LLM / AI 服务

| 服务 | 配置方式 | 接口兼容 | 最近真实 smoke | 注意事项 |
|---|---|---|---|---|
| Anthropic SDK | `LLM_PROVIDER=anthropic` | implemented：原生 tools | 2026-07-20 本机配置返回 429 quota | 适合强模型质量基线；演示前必须重跑 smoke |
| OpenAI 兼容接口 | `LLM_PROVIDER=openai` + `LLM_BASE_URL` | smoke_verified：Chat Completions tools mock 契约 | 真实 provider 逐项记录 | DeepSeek、MiniMax、通义、火山方舟等按兼容接口接入 |
| 火山方舟 Ark | `LLM_PROVIDER=openai` + `LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/v3` | smoke_verified：官方 OpenAI 兼容请求形态 | 历史真实 smoke 通过 | `LLM_MODEL` 必须填具体模型/endpoint ID，API Key 建议放 `ARK_API_KEY` 或 `LLM_API_KEY` 环境变量 |
| 火山方舟 Coding Plan | `LLM_PROVIDER=openai` + `LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/coding/v3` | smoke_verified：OpenAI-compatible tools | 历史 Method AI 跑到 `120/120` | 使用 Coding Plan 专属 Key 与 `ark-code-latest`/套餐模型名；不要使用普通 Ark 网关，否则不会走套餐额度 |
| 本地 OpenAI 兼容模型 | `LLM_PROVIDER=openai` + 本地 `LLM_BASE_URL` | implemented | 需客户环境 smoke | 必须验证 tool calling / function calling 是否真正兼容 |

OpenAI 兼容接口可用 `LLM_TOOL_CHOICE=auto|required|named` 调整 function calling 形态。默认使用 `auto`；`named` 会强制第一个工具，只应用于单工具兼容测试或明确受控流程。
对首包延迟较高的私有化或聚合网关，可用 `LLM_TIMEOUT_SECONDS` 调整请求超时，默认 120 秒。

LLM 适配层契约已在 `forge/adapters.py` 固定为 `LLMProviderAdapter`：`tools`、`named_tool_choice`、`json_schema_strict`、`json_mode`、`plain_json_fallback` 和 `timeout_seconds`。Provider 对外支持状态必须由 `scripts/provider_smoke.py --json --out <path>` 记录，区分接口契约兼容和当前账号/套餐可用。

真实 provider 验证命令：

```bash
python scripts/provider_smoke.py --json --out .forge/provider-smoke.json
FORGE_PROFILE=poc bash scripts/production-smoke.sh
```

该命令只验证 tool call、Forge Schema 和确定性编译，不连接或执行客户数据库。输出不包含 API Key。

Provider 状态要同时记录两件事：接口契约是否兼容、当前账号/套餐/网关是否可用。2026-07-20 的本机 smoke 失败原因是 Token Plan 用量上限；这类失败不代表 Forge 编译链路坏了，但会阻断演示和交付验收。

## 中期适配边界

- `DatabaseAdapter` 的真实实现从 SQLite/PostgreSQL/MySQL 开始收敛，BigQuery / Snowflake 先接 dry-run。
- `LLMProviderAdapter` 下一步接入现有 Anthropic/OpenAI-compatible 调用，不改变当前 `agent/llm.py` 的行为。
- 外部 Agent 第一版只提供 `prepare_query`；执行接口默认关闭，并要求有效的人工批准记录。

## 本周交付原则

- 对外只承诺已经跑通的层级：编译支持、sync 支持、执行支持、生产交付支持要分开写。
- 每新增一个数据库或 LLM provider，至少补三类验证：配置示例、mock 单测、真实连通性 smoke。
- 客户 PoC 默认优先 PostgreSQL/MySQL/SQLite，BigQuery/Snowflake 进入前先补 dry-run 和资源限制策略。
- 对外演示前必须保存 `scripts/provider_smoke.py` 或 `scripts/production-smoke.sh` 的结果；不要只凭配置表宣称 provider 当前可用。
