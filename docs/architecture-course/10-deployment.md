---
title: 10｜部署、适配与可运维性
summary: 区分开发、PoC、生产与各层兼容证据
---

# 部署、适配与可运维性

![dev、PoC、prod 部署剖面](assets/08-deployment-profiles.svg)

**文字替代说明**：开发环境可使用 Demo SQLite 和宽松设置；PoC 连接客户只读数据库并建立专属 Registry/测试集；生产增加 HTTPS、认证、持久化、只读副本、备份、监控、回滚和严格 readiness 门禁。

## 1. 三种剖面

| 剖面 | 目的 | 最低要求 |
|---|---|---|
| dev | 本地开发、教材实验 | Demo 数据、测试 Provider 可选、`doctor dev` 可有 warn |
| PoC | 客户域验证 | 客户只读库、客户 Registry、golden questions、审计、超时 |
| prod | 受控生产 | 认证/HTTPS、只读账号、持久化、备份、监控、回滚、prod doctor 无 fail |

## 2. 部署组件

```text
Reverse Proxy / HTTPS
  → FastAPI (Web Chat + Admin + API + Feishu webhook)
  → LLM/Embedding Provider
  → Registry files (read-only mount recommended)
  → Memory/Audit DB (persistent volume)
  → Customer DB read-only replica/account
```

仓库提供 `Dockerfile`、开发 `docker-compose.yml`、`docker-compose.prod.yml` 和 `.env.production.example`。生产 compose 是起点，不是客户网络、证书和灾备方案的全部。

## 3. 兼容性必须分层说

| 层级 | 问题 |
|---|---|
| compile | Compiler 能否生成目标方言？ |
| sync | 能否 introspect schema 和枚举？ |
| execute | Executor 能否连接、超时、限制并只读执行？ |
| smoke_verified | 是否在真实/CI 数据库跑过共同测试？ |
| production_verified | 是否有客户/生产交付证据？ |

当前建议：SQLite 适合 Demo/测试；PostgreSQL 是生产首选；MySQL/MariaDB 可用于 PoC并保留真实回归；BigQuery/Snowflake 主要有方言编译路径，sync/execute/dry-run/资源治理不能据此视为完整支持。

## 4. Provider 适配

Anthropic 和 OpenAI-compatible 接口在工具格式、named tool choice、strict JSON、timeout、错误体和 token 上限上可能不同。接入新 Provider 至少验证：

1. tool call 是否真的返回结构化参数；
2. 动态 Schema 是否被执行而非忽略；
3. 编译失败能否正常重试；
4. 最大输出和超时是否足够；
5. API Key/套餐当前是否可用；
6. smoke 结果是否保存且不包含 secret。

```bash
python scripts/provider_smoke.py --json --out .forge/provider-smoke.json
```

## 5. Readiness 与 smoke

```bash
forge doctor --profile dev
forge doctor --profile poc --json
forge doctor --profile prod
FORGE_PROFILE=poc bash scripts/production-smoke.sh
```

`doctor` 检查配置；production smoke 串联 Provider、数据库、Registry 与服务路径。真实 Provider/数据库步骤受凭证和网络影响，文档不得把未运行写成通过。

## 6. 持久化与备份

至少保护：

- Registry 与变更历史；
- Memory/tenant 数据库；
- Audit/Feedback；
- 客户 accuracy cases 与结果；
- 生产配置模板（不含 secret）。

目标 runbook 应定义：RPO/RTO、备份频率、恢复演练、schema migration、版本升级、回滚触发条件和审计保留期。

## 7. 监控建议

- API/LLM/DB 延迟和错误率；
- 编译失败、lint retry、取消和 feedback 比例；
- 每租户 token 与数据库执行量；
- timeout、row cap 命中和空结果；
- Registry 变更与 accuracy 回归；
- Provider 配额、限流和兼容漂移。

这些运维能力目前并未全部产品化，应作为正式标准化交付前的目标架构，而不是现状宣称。

## 8. 权威参考

- [生产部署](https://github.com/shisuidata/Forge/blob/main/docs/production-deployment.md)
- [兼容性矩阵](https://github.com/shisuidata/Forge/blob/main/docs/compatibility-matrix.md)
- [商业化就绪](https://github.com/shisuidata/Forge/blob/main/docs/commercial-readiness.md)
