# Forge 交付前综合评估（2026-05-07）

## 结论

Forge 当前已经达到“受控生产落地 / 封闭 Beta”候选标准。模型效果、核心链路、审计、安全闸门、生产部署包和自动化测试已经具备交付基础。

但它还不应被定义为“无需陪跑的标准化规模交付产品”。真正标准化交付前，还需要真实客户业务域 accuracy suite、Registry 管理 UI、规则租户化、企业权限体系和客户级运维 runbook。

## 测试范围

本次评估覆盖：

- 业务板块：查询 Agent、定义模式、Registry、编译器、lint、执行器、审计、反馈、Admin、认证、部署、accuracy。
- 文档内容：README、benchmarks、商业化文档、生产部署文档、开发日志和公开 Markdown 链接。
- 目录结构：核心包、测试包、部署文件、运行态目录和忽略规则。
- 工作流程：自然语言到 SQL、用户审核执行、错误反馈、Registry 沉淀、生产 readiness。

## 分维度评估

### 方向层面

方向是成立的。Forge 没有把目标定成“让模型一次性写对所有 SQL”，而是把问题拆成：

- 生成错误：用 DSL、JSON Schema、compiler 和 lint 约束。
- 业务逻辑错误：用 Registry 语义层和字段/结果契约解决。
- 算法逻辑错误：明确标注为边界，不伪装成已解决。

从 AI 生成 SQL 的角度看，这是正确方向。随着模型变强，裸 SQL 的语法错误会下降，但私有指标定义、字段命名约定、组织业务口径不会自动进入模型参数。Forge 的长期价值更偏 Registry、审计和组织知识沉淀，而不只是 prompt。

### 架构层面

架构主线清晰：

```text
自然语言
  -> Registry/RAG 注入上下文
  -> LLM 生成 Forge JSON
  -> deterministic compiler
  -> SQL 审核
  -> 只读执行
  -> 审计/反馈/Registry 迭代
```

优点：

- JSON 中间层让 SQL 生成链路可审计、可复现。
- compiler 与 LLM 解耦，便于替换模型。
- Registry 把组织语义从 prompt 中剥离出来。
- lint 把高频错误从“模型注意力”转成系统约束。
- readiness/doctor 把生产配置风险前置暴露。

主要风险：

- 当前部分 high-confidence lint 规则仍偏 large schema benchmark，需要继续租户化。
- Registry 管理仍偏文件/YAML，客户自助维护成本偏高。
- 多用户/多租户已有基础，但还不是完整企业权限体系。

### 代码层面

当前代码质量整体可维护，测试覆盖面已经超过原型阶段。

本轮发现并修复的问题：

- README 和 benchmarks 文档仍停留在旧基线，已更新为 Method AF。
- `AUTH_ENABLED` 过去只读 `forge.yaml`，生产 `.env.production` 里配置后不会生效，已修复为支持环境变量。
- 缺少生产环境变量模板、生产 compose、docker ignore 和部署说明，已补齐。
- SQL 执行缺少查询超时兜底，已补充 SQLite/PostgreSQL/MySQL 的 best-effort timeout。
- readiness 缺少数据库只读账号确认和查询超时检查，已补齐。
- 文档链接没有自动化保护，已新增公开 Markdown 本地链接测试。
- 缺少轻量性能烟测入口，已新增 `scripts/performance_smoke.py`。
- `.gitignore` 未覆盖 `.venv`、`uv.lock`、生产 env、审计库、图表缓存和 website build 产物，已补齐。

### 功能性层面

已具备：

- Web Chat / API 查询。
- SQL 审核执行。
- Admin 设置和 Registry 管理基础。
- 审计与反馈队列。
- 查询只读校验、行数上限、查询超时。
- 生产 readiness 和 CLI doctor。
- large 40 题 accuracy 基准。
- 自动化测试和三轮稳定性验证。

仍需补齐：

- 客户业务域基准生成流程。
- Registry UI 的完整 CRUD、审核、回滚。
- 规则租户化。
- 企业权限：角色、团队、数据源、审计隔离。
- 备份、升级、回滚、告警 runbook。

## 三轮测试结果

### Round 1

```text
pytest: 303 passed, 23 skipped, 4 warnings
Method AF Case EA(any): 100.0% (40/40)
Method AF Case EA(all): 92.5% (37/40)
Method AF Run ACC: 97.5% (117/120)
performance: compile_p50=3.1220ms, compile_p95=3.7216ms, validate_p95=0.0210ms
```

### Round 2

```text
pytest: 303 passed, 23 skipped, 4 warnings
Method AF Case EA(any): 100.0% (40/40)
Method AF Case EA(all): 92.5% (37/40)
Method AF Run ACC: 97.5% (117/120)
performance: compile_p50=2.9574ms, compile_p95=3.2204ms, validate_p95=0.0122ms
```

### Round 3

```text
pytest: 303 passed, 23 skipped, 4 warnings
Method AF Case EA(any): 100.0% (40/40)
Method AF Case EA(all): 92.5% (37/40)
Method AF Run ACC: 97.5% (117/120)
performance: compile_p50=2.9659ms, compile_p95=3.3725ms, validate_p95=0.0146ms
```

### 额外检查

```text
公开 Markdown 本地链接检查：1 passed
scoped git diff --check：通过
docker compose config：未执行，本机无 docker 命令
forge doctor：本地配置 fail，原因是生产密码和数据库只读确认未配置
```

`forge doctor` 的 fail 是预期结果：当前本地开发配置不应被误判为可生产交付。生产交付前必须设置强密码、关闭或限制 raw SQL、确认只读数据库账号并开启 Secure Cookie。

## 实际交付优化方案

### 已完成

1. 生产部署标准化：
   - 新增 `.env.production.example`
   - 新增 `docker-compose.prod.yml`
   - 新增 `.dockerignore`
   - `Dockerfile` 默认不再使用 reload
   - 开发 compose 保留 reload

2. 生产安全闸门：
   - `EXECUTION_TIMEOUT_SECONDS`
   - `DATABASE_READONLY_CONFIRMED`
   - readiness 检查数据库只读确认、查询超时、Secure Cookie
   - `forge doctor` CLI 检查

3. 文档交付：
   - README 更新到 Method AF 当前基线
   - benchmarks 更新到 AF 指标
   - 新增生产部署说明
   - 新增本综合评估文档

4. 测试体系：
   - 新增公开 Markdown 链接测试
   - 新增性能烟测脚本
   - 完成三轮测试

### 下一步建议

1. 客户业务域 accuracy suite：
   - 每个客户 schema 建 30-100 条高频问题。
   - 核心指标、财务口径、经营看板单独标 P0。
   - 每次 Registry 变更必须跑客户 suite。

2. Registry 管理 UI：
   - 指标、字段约定、歧义规则 CRUD。
   - 规则启停、影响范围预览、变更历史、回滚。
   - 反馈样本一键生成候选规则。

3. 规则租户化：
   - lint 从全局规则逐步改成 dataset/tenant-scoped rules。
   - benchmark-specific 契约移入 Registry。
   - 规则变更触发相关测试。

4. 企业权限：
   - Admin / member / auditor 角色。
   - API Key scope。
   - 团队级 Registry、数据源和审计隔离。

5. 运维 runbook：
   - 备份、升级、回滚。
   - 日志采集、告警、审计保留策略。
   - HTTPS / 反向代理 / VPN / 内网网关部署样例。

## 可能 Bug 与修复方案

| 风险 | 影响 | 修复方案 |
|---|---|---|
| 客户 schema 与 large benchmark 差异大 | accuracy 下降 | 建客户专属 suite，按失败样本沉淀 Registry/lint |
| lint 全局规则过拟合 | 其他业务域被误约束 | 规则租户化，支持启停和作用域 |
| 数据库账号有写权限 | 极高安全风险 | 数据库层只读账号，`DATABASE_READONLY_CONFIRMED=true` 仅作为交付验收标记 |
| 慢查询拖垮数据库 | 资源风险 | 查询超时、只读副本、资源组、行数上限 |
| Registry YAML 手改出错 | 业务口径风险 | UI 管理、校验、版本历史、回滚 |
| 线上错误无法复现 | 质量闭环中断 | 审计保存 question/Forge JSON/SQL/result/error，反馈转测试 |
| Docker 生产配置漂移 | 部署不可复现 | 固定 `.env.production.example`、`docker-compose.prod.yml`、`forge doctor` |
| 文档链接腐化 | 交付体验下降 | `tests/test_docs_links.py` 持续检查 |

## 最终判断

Forge 已经可以进入设计型客户的受控生产落地。它的标准化产品交付还差最后一层：把当前针对项目内 large benchmark 的成功，复制到客户真实业务域，并把 Registry 与规则管理做成客户可自助维护的产品能力。
