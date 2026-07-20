---
title: 安装部署
description: 多种方式安装和部署 Forge
---

## 环境要求

- Python >= 3.11
- Node.js >= 18（仅开发时需要）
- Docker（推荐部署方式）

## Docker 部署（推荐）

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
cp .env.example .env
# 编辑 .env，填入 LLM_API_KEY 和 EMBED_API_KEY
docker compose up
```

自带 PostgreSQL 16，访问 `http://localhost:8000`。

## pip 安装

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
pip install -e .
cp .env.example .env
```

## 配置

### 必填配置

| 变量 | 说明 | 示例 |
|---|---|---|
| `LLM_API_KEY` | LLM 服务的 API Key | `sk-xxx` |
| `LLM_PROVIDER` | `anthropic` 或 `openai` | `anthropic` |
| `LLM_MODEL` | 模型 ID | `claude-sonnet-4-6` |

### 可选配置

| 变量 | 说明 | 默认值 |
|---|---|---|
| `DATABASE_URL` | 数据库连接 | SQLite 内存 |
| `EMBED_API_KEY` | 向量模型 API Key | （降级为全表模式） |
| `EMBED_MODEL` | 向量模型 | `BAAI/bge-m3` |
| `AUTH_ENABLED` | 启用认证 | `false` |
| `AUTH_ADMIN_PASSWORD` | 登录密码 | `123456` |

### 接入自己的数据库

```bash
# 设置 DATABASE_URL 后同步表结构到 Registry
forge sync --db postgresql://user:pass@host/db
```

## 生产交付

生产环境必须使用只读数据库账号或只读副本，并把 SQL 执行、行数上限、超时、审计和认证全部纳入门禁。

```bash
cp .env.production.example .env.production
# 编辑 .env.production，填入 AUTH_PASSWORD、AUTH_API_KEYS、LLM_API_KEY、DATABASE_URL
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
bash scripts/production-smoke.sh
```

`production-smoke` 会运行 `forge doctor`、数据库 `SELECT 1`、provider tool-call smoke 和可选 HTTP readiness；不会对客户数据库写入。
