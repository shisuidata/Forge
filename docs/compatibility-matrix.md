# Forge 兼容性矩阵

Forge 的兼容性分四层看：SQL 编译、数据库结构同步、SQL 执行、Agent/LLM 接入。不要把“能编译某方言 SQL”等同于“已经完整支持该数据平台生产交付”。

## 数据库 / 数据仓库

| 平台 | SQL 编译 | `forge sync` | 查询执行 | 当前建议 |
|---|---|---|---|---|
| SQLite | ✅ 完整测试覆盖 | ✅ SQLAlchemy introspection | ✅ 行数上限 + progress timeout | 本地 demo、测试、单机 PoC |
| PostgreSQL | ✅ 日期、聚合、JOIN 方言 | ✅ SQLAlchemy introspection | ✅ statement timeout | 生产首选关系型数据库 |
| MySQL / MariaDB | ✅ 主要方言覆盖；不支持 `FILTER` 时显式报错 | ✅ SQLAlchemy introspection | ✅ best-effort timeout | 可做 PoC，需补真实库回归 |
| BigQuery | ✅ 编译层覆盖日期、字符串聚合、部分 JOIN 限制 | ⚠️ 尚未产品化 sync | ⚠️ 尚未产品化执行 | 中期适配项，先从编译测试扩展到 dry-run |
| Snowflake | ✅ 编译层覆盖日期、`LISTAGG`、部分限制 | ⚠️ 尚未产品化 sync | ⚠️ 尚未产品化执行 | 中期适配项，需补 warehouse/resource guardrail |
| ClickHouse / Doris / StarRocks / DuckDB | ❌ 未声明支持 | ❌ 未声明支持 | ❌ 未声明支持 | 先建立方言差异清单，再决定是否进入路线图 |

SQLite、PostgreSQL 16、MySQL 8 共用 `tests/test_database_compatibility.py`，覆盖 schema introspection、枚举采样、目标方言编译、只读查询、行数上限和写 SQL 拒绝。CI 的 `compatibility` job 分别连接三个真实数据库运行同一套 smoke；只有 CI 绿灯的平台才能对外声明对应层级已验证。

`forge sync` 通过 SQLAlchemy 正确引用保留字、大小写或带空格的表名和字段名。该能力只代表结构同步成功；Forge 编译器和目标数据库是否能执行这些非标准标识符，仍需单独验证，不能据此宣称完整查询支持。

## Agent 入口

| 入口 | 状态 | 说明 |
|---|---|---|
| Web Chat / Admin | ✅ 已有 | 适合数据团队内部 PoC 和 Registry 管理 |
| 飞书 Bot | ✅ 已有 | 适合国内团队协同场景 |
| Pipeline API | ✅ 已有雏形 | 支持分析、可视化、报告类流程 |
| 钉钉 / 企业微信 / Slack | ❌ 未产品化 | 建议抽象消息入口适配层后再做 |
| MCP / OpenAI Agents / Claude Desktop | ❌ 未产品化 | 中期项，适合把 Forge 作为企业数据查询工具暴露给外部 Agent |

## LLM / AI 服务

| 服务 | 配置方式 | 当前状态 | 注意事项 |
|---|---|---|---|
| Anthropic | `LLM_PROVIDER=anthropic` | ✅ 原生 tools | 适合强模型质量基线 |
| OpenAI 兼容接口 | `LLM_PROVIDER=openai` + `LLM_BASE_URL` | ✅ Chat Completions tools | DeepSeek、MiniMax、通义、火山方舟等按兼容接口接入 |
| 火山方舟 Ark | `LLM_PROVIDER=openai` + `LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/v3` | ✅ 已按官方 OpenAI 兼容接口验证请求形态 | `LLM_MODEL` 必须填具体模型/endpoint ID，API Key 建议放 `ARK_API_KEY` 或 `LLM_API_KEY` 环境变量 |
| 火山方舟 Coding Plan | `LLM_PROVIDER=openai` + `LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/coding/v3` | ⚠️ 需真实 smoke | 使用 Coding Plan 专属 Key 与 `ark-code-latest`/套餐模型名；不要使用普通 Ark 网关，否则不会走套餐额度 |
| 本地 OpenAI 兼容模型 | `LLM_PROVIDER=openai` + 本地 `LLM_BASE_URL` | ⚠️ 可接入 | 必须验证 tool calling / function calling 是否真正兼容 |

OpenAI 兼容接口可用 `LLM_TOOL_CHOICE=auto|required|named` 调整 function calling 形态。默认使用 `auto`；`named` 会强制第一个工具，只应用于单工具兼容测试或明确受控流程。
对首包延迟较高的私有化或聚合网关，可用 `LLM_TIMEOUT_SECONDS` 调整请求超时，默认 120 秒。

真实 provider 验证命令：

```bash
python scripts/provider_smoke.py
```

该命令只验证 tool call、Forge Schema 和确定性编译，不连接或执行客户数据库。输出不包含 API Key。

## 中期适配边界

- `DatabaseAdapter`：统一 introspection、timeout、dry-run、execution 和 capability；BigQuery / Snowflake 在具备 dry-run 与资源上限前保持 partial。
- `LLMProviderAdapter`：显式描述 tools、tool choice、JSON Schema 和普通 JSON fallback 能力。
- 外部 Agent：先提供只生成待审核 SQL 的 `prepare_query`；执行接口默认关闭，并要求有效的人工批准记录。

## 本周交付原则

- 对外只承诺已经跑通的层级：编译支持、sync 支持、执行支持、生产交付支持要分开写。
- 每新增一个数据库或 LLM provider，至少补三类验证：配置示例、mock 单测、真实连通性 smoke。
- 客户 PoC 默认优先 PostgreSQL/MySQL/SQLite，BigQuery/Snowflake 进入前先补 dry-run 和资源限制策略。
