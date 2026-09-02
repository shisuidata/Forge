# Forge

[![CI](https://github.com/shisuidata/Forge/actions/workflows/ci.yml/badge.svg)](https://github.com/shisuidata/Forge/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

> **面向 Data Agent 的开源可信数据运行时：让 AI 辅助的数据查询可约束、可审核、可执行、可追溯。**

Forge 位于上游 Agent 与数据库之间。它接收 Direct SQL 或受约束的 Forge JSON 候选，通过 Registry、确定性 SQL 编译、只读与权限校验、人工审核、Evidence 和 Audit 降低静默错误。它不是只把自然语言转成 SQL 的 wrapper，也不把“模型生成了 SQL”等同于“查询可信”。

[English README](README_EN.md) · [贡献指南](CONTRIBUTING.md) · [文档索引](docs/README.md)

## 30 秒理解 Forge

| 普通 Text-to-SQL 路径 | Forge 当前路径 |
|---|---|
| 模型直接生成并执行开放 SQL | 候选先进入受约束、可拒绝的保障流程 |
| Prompt 同时承担语义、语法和安全 | Registry 管语义，编译器管转换，运行时管校验与执行 |
| 失败通常只有“SQL 报错” | 保留输入类型、SQL hash、版本、失败阶段和 Evidence |
| 结果正确性依赖单次模型表现 | 用可复现 benchmark 和 regression 迭代，不承诺开放世界 100% 正确 |

**当前已有的核心能力：**

- Forge JSON 受约束中间表示与确定性 SQL 编译；
- Direct SQL / Forge JSON 统一候选、QueryRun 审核和执行链；
- Registry 结构与语义约束、关系和粒度校验；
- 只读 SQL、字段/表范围、审批 hash、超时和结果上限；
- SQLite、PostgreSQL、MySQL 的自动化兼容性检查；
- 可回放的准确率 Benchmark、Exact Result 比较和失败诊断。

> **项目状态：early-stage，actively iterating。** 当前适合评估、贡献和带人工审核/只读账号的受控部署，不代表功能完备、开放世界准确或大规模高可用。最新完整 500-case BIRD 运行中，Forge EA 为 **45.4%**，Direct SQL 为 **56.4%**；历史 Spider2-Lite SQLite 子集 EA 仍为 **9.2%**。这些结果受数据集、模型、Provider、Prompt、Registry 与运行配置约束，不能外推为普遍能力。详见 [当前项目状态](docs/current-project-state.md) 与 [Benchmark 边界](docs/benchmarks.md)。

---

## 快速开始

```bash
# 1. 克隆 & 安装
git clone https://github.com/shisuidata/Forge
cd Forge
bash scripts/bootstrap-dev.sh

# 2. 配置（填入 LLM_API_KEY + EMBED_API_KEY）
cp .env.example .env

# 3. 一键启动 Demo（生成 200 表数仓 + 同步 Registry + 跑通测试）
bash scripts/demo-setup.sh

# 4. 启动服务
uvicorn main:app --host 0.0.0.0 --port 8000  # Web UI + API
```

**Docker 开发方式（自带 PostgreSQL，热重载）：**

```bash
docker compose up
# 访问 http://localhost:8000/admin
```

**生产交付方式：**

```bash
cp .env.production.example .env.production
docker compose -f docker-compose.prod.yml --env-file .env.production up -d --build
forge doctor --profile prod
bash scripts/production-smoke.sh
```

生产部署必须使用数据库只读账号，并确认 `/health/readiness` 无 `fail` 项。详见 [生产交付部署说明](docs/production-deployment.md)。

**接入自己的数据库：**

```bash
# 修改 .env 中的 DATABASE_URL，然后同步 schema
forge sync --db postgresql://user:pass@host/db
# 或写入指定 Registry 路径
forge sync --db "$DATABASE_URL" --out registry/data/schema.registry.json
```

**使用火山方舟 Ark（OpenAI 兼容接口）：**

```env
LLM_PROVIDER=openai
LLM_BASE_URL=https://ark.cn-beijing.volces.com/api/v3
LLM_MODEL=doubao-seed-2-1-pro-260628
LLM_API_KEY=你的火山方舟 API Key
```

**运行测试：**

```bash
# 编译器 + API 测试（本地，无需服务运行）
pytest tests/ -v

# Playwright E2E 测试（需服务运行）
pip install playwright && playwright install chromium
FORGE_BASE_URL=http://localhost:8000 pytest tests/test_e2e.py -v
```

---

## 工作原理

```mermaid
flowchart LR
    NL["自然语言"]

    subgraph forge["Forge 管道"]
        direction TB
        REG["Registry<br/>结构层 + 语义层"]
        RETRIEVER["SchemaRetriever<br/>向量检索 / BM25 降级"]
        LLM["LLM<br/>Structured Output"]
        JSON["Forge JSON<br/>有约束的中间表示"]
        COMPILER["确定性编译器"]
        SQL["SQL"]
    end

    DB["数据库"]
    RESULT["结果集"]

    NL --> RETRIEVER
    REG --> RETRIEVER
    RETRIEVER -->|"top-k 相关表"| LLM
    LLM --> JSON
    JSON --> COMPILER
    COMPILER --> SQL
    SQL --> DB
    DB --> RESULT
```

自然语言经 Registry 语义注入后，由 LLM 生成结构化的 Forge JSON，再由确定性编译器翻译为 SQL。在 Provider 严格执行动态 JSON Schema 的字段上，非法候选可在生成阶段被阻止；表达式透传、兼容降级路径、业务口径和算法选择仍需校验、审核与测试。用户审核的 SQL 和执行的 SQL 是同一份。

从产品价值、核心原理到生产落地的系统讲解，见 [Forge 完整架构教材](docs/architecture-course/index.md)；快速参考见 [工作原理与 DSL 能力](docs/how-it-works.md)。

---

## 当前状态

| 信号 | 当前边界 |
|---|---|
| BIRD 500-case 完整运行 | Forge EA **45.4%**；Direct SQL EA **56.4%** |
| 历史自有 40 题回归集 | 用于域内回归，不作为陌生 Schema 泛化证明 |
| Python CI | 完整 `pytest`，并覆盖 SQLite / PostgreSQL / MySQL compatibility smoke |
| Pi Orchestrator | TypeScript typecheck 与 Node test 可在本地独立运行 |
| Spider2-Lite SQLite | 编译成功率 **97.6%**；EA **9.2%** |

不同数据集、模型、Provider、Prompt、Registry、重试策略和评价器的结果不可直接横向比较。详见 [基准测试详情](docs/benchmarks.md)。

### 已落地功能

| 功能 | 状态 |
|---|---|
| Web UI（Chat + 12 个 Admin 页面 + Dashboard 概览） | ✅ |
| SQL 审核编辑（生成后可修改 SQL 再执行） | ✅ |
| 查询结果导出（CSV / JSON，中文 BOM 兼容） | ✅ |
| 认证鉴权（Cookie session + API Key） | ✅ |
| 多租户基础（user → team 映射；org/team/user 仍在完善） | ✅ 基础能力 |
| 数据权限（team 级别表可见性 ACL + 无权限提示） | ✅ |
| PostgreSQL 支持（SQLite 零改动切换） | ✅ |
| 三层记忆系统（EMS / SMP / WMB） | ✅ |
| Pipeline 引擎（分析 / 可视化 / 报告） | ✅ 代码路径；需客户域验收 |
| 飞书 Bot（流式卡片 + 按钮回调） | ✅ |
| 五通道知识收集（RSS / URL / 文档 / 对话 / 手动） | ✅ |
| 文档导入（上传 .txt/.md → LLM 提取 → 确认入库） | ✅ |
| 自动化测试（API / compiler / executor / lint / docs / audit / feedback） | ✅ |
| 部署就绪检查（`/health/readiness` + `forge doctor`） | ✅ |
| 生产部署包（Dockerfile / compose.prod / env 模板 / 部署文档） | ✅ |

---

## 项目结构

```
forge/
  ├── schema.json          — Forge DSL 格式定义（JSON Schema）
  ├── compiler.py          — 确定性编译器：Forge JSON → SQL
  ├── retriever.py         — Schema 向量检索器（四层召回 + ACL 过滤）
  ├── executor.py          — SQL 执行器
  ├── lint.py              — 业务/字段/结果契约检查
  ├── cache.py             — 查询缓存（精确 + 模糊匹配）
  ├── chart.py             — 图表生成（ECharts）
  └── cli.py               — CLI 入口（compile / sync / doctor）

agent/
  ├── agent.py             — Agent 调度（查询 / 指标定义 / 缓存反馈）
  ├── llm.py               — LLM 客户端（RAG + ACL + 约定注入）
  ├── pipeline.py          — Pipeline 引擎（分析 / 可视化 / 报告）
  ├── db.py                — 数据库抽象层（SQLite / PostgreSQL）
  ├── tenant.py            — 多租户（org / team / user / ACL）
  ├── knowledge.py         — 五通道知识收集框架
  └── memory/
      ├── ems.py           — Episodic Memory Store（对话历史）
      ├── smp.py           — Semantic Memory Pool（业务知识）
      └── wmb.py           — Working Memory Buffer（当前上下文）

web/
  ├── router.py            — FastAPI 路由（Web UI + API + execute-raw）
  ├── auth.py              — HMAC-SHA256 Cookie + API Key 认证
  └── templates/           — Jinja2 模板（Chat + Dashboard + 11 个 Admin 页面）

registry/
  ├── sync.py              — forge sync：直连数据库生成结构层
  ├── staging_sync.py      — 用户确认规则合并入 Registry
  └── data/                — 生产 Registry 路径（schema / metrics / disambiguations / conventions）

scripts/
  └── seed_mock_data.py    — Mock 数据填充（团队/用户/审计/会话/知识）

tests/
  ├── conftest.py          — 共享 fixtures（app / client / auth_client）
  ├── test_compiler*.py    — 编译器单元测试（118 个用例）
  ├── test_api.py          — API 端点测试（26 个用例）
  ├── test_e2e.py          — Playwright E2E 测试（22 个用例）
  ├── test_docs_links.py   — 公开文档本地链接检查
  ├── accuracy/            — 自有 40 题基准（当前推荐 Method AI）
  └── spider2/             — Spider2-Lite SQLite 子集（123 题）
```

---

## 文档

| 文档 | 内容 |
|---|---|
| [当前项目状态](docs/current-project-state.md) | 当前产品定义、阶段、门禁、未关闭验收项与 OMP 继续开发入口 |
| [文档导航](docs/README.md) | 区分当前事实、稳定约束、主动计划与历史材料 |
| [完整架构教材](docs/architecture-course/index.md) | 从可信问数原理、核心技术优势到实战与生产架构 |
| [架构设计](docs/architecture.md) | 系统整体架构与模块职责的精简入口 |
| [产品北极星](docs/product-north-star.md) | Forge 为什么存在、服务谁，以及正确性、共识、数据事实与产品边界的长期指导 |
| [产品设计与阶段路线重建提案](docs/product-design-roadmap-2026-08-25.md) | Human Control Plane、Agent Data Runtime、产品对象、信息架构与长期阶段方向 |
| [短期 Product Spine 历史计划](docs/short-term-product-spine-plan-2026-08-25.md) | SP0–SP5 已完成实施与验证记录；仅作历史溯源，不是当前待办 |
| [Product Spine SP5 集成门禁证据](docs/product-spine-sp5-evidence-2026-08-25.md) | 真实 Pi/Forge/Report 三连 Golden Journey、restart/idempotency/offline、Atlas candidate 与失败关闭反证 |
| [Product Projection v1 Contract](docs/product-projection-contracts.md) | Conversation、Task、Action、Workspace、Report 的版本化只读边界、状态、bounds、redaction 与 SP1 入口 |
| [产品公理](docs/product-axioms.md) | 以第一性原理约束身份、证据、协同、记忆、成本与可信行动 |
| [AI Native 企业长期论证](docs/ai-native-enterprise-thesis.md) | Data Agent、组织协同、统一记忆、企业 AI Infra 的论证、反证与待验证假设 |
| [产品方向与架构复审](docs/product-direction-architecture-review-2026-08-24.md) | 按产品公理审核当前实现、四平面缺口、目标架构与分阶段建议 |
| [企业演进主动计划](docs/forge-enterprise-evolution-plan.md) | 唯一主动计划；当前阶段为 S0 Design Partner / Problem Baseline |
| [需求池](docs/requirements-pool.md) | 新需求的澄清、评估、接受、延期、拒绝、计划与验证记录 |
| [M0 Governance Contract 评审](docs/governance-contract-review-2026-08-24.md) | 跨 Contract 语义、Threat Model、迁移/回滚与 M1A 前置结论 |
| [工作原理与 DSL 能力](docs/how-it-works.md) | 执行流程详解、DSL 特性表、Schema RAG |
| [基准测试详情](docs/benchmarks.md) | 版本演化、跨模型 EA 对比、Spider2 结果 |
| [设计哲学与工程洞察](docs/philosophy.md) | 核心哲学、工程经验、开放问题 |
| [商业化就绪清单](docs/commercial-readiness.md) | 当前商业化差距、已补齐的安全/审计能力、PoC 到正式交付路线 |
| [商业化推进计划](docs/commercialization-plan.md) | P0/P1/P2 优先级、准确率闭环、PoC 到正式交付判定标准 |
| [兼容性矩阵](docs/compatibility-matrix.md) | 数据库、数据仓库、Agent 入口、LLM 服务的支持边界 |
| [客户 PoC 执行手册](docs/poc-playbook.md) | 客户域 golden questions、failure triage 和交付物 |
| [外部 Agent 集成边界](docs/agent-integration.md) | MCP / OpenAI Agents / Claude Desktop 等外部入口的 prepare-query 只读边界 |
| [交付前综合评估](docs/delivery-assessment-2026-05-07.md) | 业务板块、文档、目录、工作流、三轮测试和交付优化方案 |
| [生产交付部署说明](docs/production-deployment.md) | 生产 compose、env、只读数据库账号、readiness、运维建议 |
| [DSL 形式化语义](docs/dsl-semantics.md) | DSL 的形式化定义 |
| [构建你的语义库](docs/registry.md) | Registry 结构层 + 语义层三文件详解，从零构建指南 |
| [飞书 Bot 部署](docs/feishu-setup.md) | 飞书集成配置 |

---

## 开发日志

真实的建造记录，包括走错的路、自我怀疑的时刻，和偶尔出现的顿悟。

| 篇 | 日期 | 主题 |
|---|---|---|
| [Day 0 · 开发实录](docs/devlog/forge-dev-story.md) | 2026-03 | 为什么做这件事；错误分类；核心洞见的形成过程 |
| [Day 1 · 历史债 / 地面泥潭](docs/devlog/day1_2026-03-15.md) | 2026-03-15 | SQL 的设计哲学、四层召回演进、飞书 Bot 工程坑、SQL 缓存双阶段反馈 |
| [Day 2 · CROSS JOIN / HAVING 别名 / EA 95%](docs/devlog/day2_2026-03-16.md) | 2026-03-16 | CROSS JOIN 标量 CTE 模式、HAVING alias 展开修复、DeepSeek strict tool calling 实验、M/O/N 三组 EA 基准 |
| [Day 3 · 工程稳固 / 产品门面 / 连锁故障](docs/devlog/day3_2026-03-18.md) | 2026-03-18 | Session 持久化、编译器拆分、飞书 Bot 四层连锁故障、demo 向导、forge config CLI |
| [Day 5 · 先看自己错没错 / 三层系统优化](docs/devlog/day5_2026-03-19.md) | 2026-03-19 | 5 处设计缺陷修复、编译重试对齐、约定 lint 程序化验证、LAG 示例补全、M2.7 EA 72.5% |
| [Day 6 · 从原型到产品](docs/devlog/day6_2026-03-25.md) | 2026-03-25 | PostgreSQL 支持、HMAC 认证、数据权限 ACL、Pipeline E2E、Web Admin 完整落地、EA 70.0% |
| [Day 7 · 准确率回炉 / TopN lint](docs/devlog/day7_2026-05-05.md) | 2026-05-05 | 测试口径拆分、Z.AI GLM-5.1 接入受阻、DeepSeek single-run EA 55.0%→65.0%、TopN lint/prompt 优化 |
| [Day 8 · 把准确率问题重新工程化](docs/devlog/day8_2026-05-06.md) | 2026-05-06 | 面向博客发布的阶段总结：测试分层、GLM-5.1 接入受阻、DeepSeek EA 55.0%→65.0%、TopN 错误形式化为 lint |

---

## 参与维护

贡献入口与复现要求见 [`CONTRIBUTING.md`](CONTRIBUTING.md)。[`shisuidata/Forge`](https://github.com/shisuidata/Forge) 是项目主仓库；仓库历史中出现的 [`rockythink`](https://github.com/rockythink) 与 [`shisuidata`](https://github.com/shisuidata) 标识属于同一维护者/团队上下文。

## License

Forge 使用 [Apache License 2.0](LICENSE)。

## 官网

`website/` 是 Forge 的 Astro + Starlight 对外站点，用于承载快速开始、概念说明、基准测试和商业化 PoC 叙事：

```bash
cd website
npm install
npm run build
```
