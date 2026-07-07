# 2026-07-07 商业化兼容性与交付闭环

## 背景

本轮目标是把 Forge 从“已有商业化复盘建议”继续推进到更可执行的工程状态：

- 补齐生产部署文档与 CLI 行为不一致的问题。
- 明确数据库、数据仓库、Agent、LLM 服务的兼容性边界。
- 让新开发者能一键建立本地验证环境。
- 把火山方舟 Ark 按 OpenAI 兼容接口纳入配置和测试路径。

## 主要改动

### 1. `forge sync --out`

生产部署文档需要把客户数据库结构同步到 `registry/data/schema.registry.json`，但 CLI 原本只有 `--db`，没有 `--out`。

本轮为 `forge sync` 增加：

```bash
forge sync --db "$DATABASE_URL" --out registry/data/schema.registry.json
```

同时 `registry.sync.run_sync()` 会自动创建输出目录，适合全新交付环境。

### 2. 兼容性矩阵

新增 `docs/compatibility-matrix.md`，按四层拆分兼容性：

- SQL 编译
- `forge sync`
- 查询执行
- 生产交付建议

这能避免把“编译器能生成 BigQuery SQL”和“BigQuery 已可生产交付”混成同一个结论。

### 3. 火山方舟 OpenAI 兼容接入

火山方舟 Ark 使用 OpenAI 兼容 Chat Completions API：

```env
LLM_PROVIDER=openai
LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/v3
LLM_MODEL=doubao-seed-2-1-pro-260628
LLM_API_KEY=...
```

本轮没有把真实 API Key 写入任何仓库文件。测试覆盖的是 OpenAI 兼容请求形态：base URL、Bearer 鉴权、`tools` / function calling payload。

### 4. 开发环境 bootstrap

新增：

```bash
bash scripts/bootstrap-dev.sh
```

脚本会创建 `.venv`，安装 `.[dev]`，并提示运行 `forge doctor` 和 `pytest -q`。

### 5. 测试路径修复

`tests/test_api.py` 中有旧机器路径 `/Volumes/MacData/Workspace/90_Dev/Forge`，导致当前工作区测试失败。本轮改为从测试文件位置推导仓库根目录。

## 验证

本轮验证项目：

- `forge doctor`
- `pytest -q`
- 火山方舟 Ark 真实 API 连通性 smoke

`forge doctor` 可能因为本地 `.env` 使用开发配置而返回 fail，例如默认密码、未确认只读数据库账号、Secure Cookie 未开启。这类 fail 是生产配置提醒，不代表代码不可运行。

## 后续

- 为 BigQuery / Snowflake 增加 dry-run 或 explain 级验证。
- 把客户 PoC 的 accuracy suite 模板固化到 `tests/customer-template/` 或 `docs/poc-playbook.md`。
- 把 Registry 规则加上 `tenant_id` / `dataset_id` / `version`，避免 benchmark 规则污染客户环境。
