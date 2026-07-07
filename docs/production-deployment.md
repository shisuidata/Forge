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
