# Forge 生产交付部署说明

## 交付目标

Forge 的生产部署必须满足四个条件：

- 使用客户数据库只读账号或只读副本。
- 开启认证和 API Key。
- 开启 SQL 执行上限、查询超时和审计。
- `/health/readiness` 返回 `ok`，或所有 `warn` 项都有明确接受理由。

## 快速部署

1. 复制生产环境变量模板：

```bash
cp .env.production.example .env.production
```

2. 修改 `.env.production`：

```text
AUTH_PASSWORD=...
AUTH_API_KEYS=...
LLM_API_KEY=...
DATABASE_URL=...
DATABASE_READONLY_CONFIRMED=true
```

3. 准备 Registry：

```bash
mkdir -p registry/data
forge sync --db "$DATABASE_URL" --out registry/data/schema.registry.json
```

并确认以下文件存在：

```text
registry/data/schema.registry.json
registry/data/metrics.registry.yaml
registry/data/disambiguations.registry.yaml
registry/data/field_conventions.registry.yaml
```

4. 启动：

```bash
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
```

5. 检查就绪状态：

```bash
curl http://localhost:8000/health/readiness
```

## 数据库权限要求

生产环境不能使用拥有写权限的数据库账号。

PostgreSQL 示例：

```sql
CREATE ROLE forge_readonly LOGIN PASSWORD 'replace-password';
GRANT CONNECT ON DATABASE warehouse TO forge_readonly;
GRANT USAGE ON SCHEMA public TO forge_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO forge_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO forge_readonly;
```

MySQL 示例：

```sql
CREATE USER 'forge_readonly'@'%' IDENTIFIED BY 'replace-password';
GRANT SELECT ON warehouse.* TO 'forge_readonly'@'%';
FLUSH PRIVILEGES;
```

应用层会拒绝明显非只读 SQL，但这只是第二道防线。真正的安全边界必须由数据库权限保证。

## 推荐生产配置

```text
AUTH_ENABLED=true
AUTH_COOKIE_SECURE=true
RAW_SQL_ENABLED=false
EXECUTION_MAX_ROWS=200
EXECUTION_DISPLAY_ROWS=50
EXECUTION_TIMEOUT_SECONDS=30
DATABASE_READONLY_CONFIRMED=true
```

## readiness 检查项

`GET /health/readiness` 会检查：

- 认证是否开启。
- 管理员密码是否为空或默认值。
- LLM API Key 是否配置。
- 数据库连接是否配置。
- 是否确认数据库只读账号。
- 手动 SQL 执行入口是否关闭。
- 最大返回行数是否过大。
- 查询超时是否配置。
- Registry 文件是否齐备。
- 审计目录是否可写。

生产交付前，`fail` 项必须清零。

## 运维建议

- `.forge` 目录需要持久化，用于审计、staging 和本地记忆库。
- `registry/data` 建议纳入客户私有 Git 仓库，所有业务规则变更可追踪。
- 每次 Registry 变更后运行客户 accuracy suite。
- 审计库和 Registry 文件需要纳入备份。
- 不建议在公网裸露服务，应放在公司 VPN、内网网关或反向代理之后。

## 备份与恢复

升级或修改 Registry 前至少备份两部分：

```bash
tar -czf forge-state-$(date +%Y%m%d-%H%M%S).tar.gz .forge registry/data
```

- `registry/data` 保存结构层和组织语义，推荐进入客户私有 Git 仓库并走 review。
- `.forge` 保存审计、staging、记忆库和运行状态，使用文件备份或持久卷快照。
- 恢复时先停止 Forge，恢复两个目录，再启动并运行 `forge doctor` 与 `/health/readiness`。
- 恢复后抽查核心指标、最近审计记录和一条只读查询，不只检查进程存活。

## 升级与回滚

1. 记录当前镜像标签或 Git commit，并完成备份。
2. 在测试环境使用客户 Registry 运行完整测试和核心问题集。
3. 构建新镜像并启动，确认 `forge doctor` 无 fail、readiness 无 fail。
4. 验证登录、指标 CRUD、SQL 审核、只读执行和审计记录后再切换流量。
5. 若出现 Registry 不兼容、查询回归或 readiness fail，立即切回旧镜像并恢复升级前状态。

生产环境不要使用浮动的 `latest` 作为唯一回滚依据；镜像标签应包含 Forge 版本或 commit。

## 日志与告警

- 持久化 `LOG_FILE`、审计库和反向代理访问日志。
- 对 readiness fail、LLM/provider 错误率、SQL 执行失败率、查询超时和审计目录不可写设置告警。
- 日志不得记录 API Key、数据库密码、完整认证 Cookie 或客户敏感结果集。
- 线上错误应能关联用户问题、Forge JSON、审核 SQL、执行状态和耗时。

## 反向代理示例

Forge 应位于 HTTPS 反向代理、VPN 或企业内网网关后。代理至少需要：

- 强制 HTTPS，并设置 `AUTH_COOKIE_SECURE=true`。
- 限制请求体大小和连接超时，保留客户端请求 ID。
- Web 管理入口和 API 分别配置访问控制；不要公开数据库或 `.forge` 目录。
- 长耗时查询的代理超时应略高于 `EXECUTION_TIMEOUT_SECONDS`，但不能取消数据库侧超时。

## 故障排查顺序

1. 运行 `forge doctor`，先清理所有 fail。
2. 检查 `/health/readiness`、容器日志和反向代理日志。
3. 验证 LLM provider smoke，区分模型接口与数据库故障。
4. 使用数据库只读账号执行最小 `SELECT 1`，再运行数据库 compatibility smoke。
5. 检查 Registry 文件能否解析、路径是否挂载正确、最近变更是否需要回滚。
6. 不在生产环境临时开启写权限或关闭 SQL 审核来绕过问题。

## 交付验收

- `forge doctor` 没有 fail，所有 warn 都有书面接受理由。
- 数据库账号已由客户 DBA 验证为只读，应用层校验不是唯一安全边界。
- Registry 与 `.forge` 已完成备份和恢复演练。
- SQL 仍在执行前展示并要求人工确认，审计记录可查询。
- 客户核心问题集达到约定准确率，失败样本已进入 Registry 或工程 backlog。
