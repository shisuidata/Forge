# Forge

> **面向数据团队的 AI 查询 Agent，让弱模型也能生成可信 SQL。**
> ⚠️ 早期阶段，持续迭代中。企业日常查询场景已优于直接 SQL 生成；学术 benchmark 上还有差距。

[English README](README_EN.md)

---

## 它解决什么问题

| 错误类型 | 举例 | Forge 的答案 |
|---|---|---|
| **生成错误**：推理对了，SQL 写错了 | `INNER JOIN` 替代 `LEFT JOIN`；`NOT IN` 遇 NULL 静默返错 | ✅ DSL 约束 + 编译器 |
| **业务逻辑错误**：指标定义歧义 | "复购率"的分母是谁？ | ✅ Registry 语义层 |
| **算法逻辑错误**：不知该用什么算法 | 日期序列填充、同比计算 | ❌ 诚实标注，超出能力边界 |

**核心主张**：生成错误和业务逻辑错误应该系统性消灭，而不是靠更好的 prompt 碰运气。

### Forge vs 直接 SQL 生成

40 题自有测试集，LLM Judge 评分（Claude Sonnet，每题 5 次均值）：

| 查询类型 | 直接 SQL | **Forge** | Δ |
|---|---|---|---|
| ANTI/SEMI JOIN | 7.80 | **8.60** | **+0.80** |
| 排名 & TopN | 8.36 | **9.00** | +0.64 |
| 时序导航 | 8.40 | **9.00** | +0.60 |
| **总体（8 类）** | **8.38** | **8.82** | **+0.44** |

所有分类均优于直接 SQL 生成，无退步。详见 [基准测试](docs/benchmarks.md)。

---

## 快速开始

```bash
# 1. 克隆 & 安装
git clone https://github.com/shisuidata/Forge
cd Forge
pip install -e .

# 2. 配置（填入 LLM_API_KEY + EMBED_API_KEY）
cp .env.example .env

# 3. 一键启动 Demo（生成 200 表数仓 + 同步 Registry + 跑通测试）
bash scripts/demo-setup.sh

# 4. 启动服务
PYTHONPATH=. python3 web/feishu.py          # 飞书 Bot
# 或
uvicorn main:app --host 0.0.0.0 --port 8000  # Web API
```

**Docker 方式：**

```bash
docker compose up
```

**接入自己的数据库：**

```bash
# 修改 .env 中的 DATABASE_URL，然后同步 schema
forge sync --db postgresql://user:pass@host/db
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

自然语言经 Registry 语义注入后，由 LLM 生成结构化的 Forge JSON（JSON Schema 在 token 级别约束合法输出），再由确定性编译器翻译为 SQL。用户审核的 SQL 和执行的 SQL 是同一份，无运行时变换。

详见 [工作原理与 DSL 能力](docs/how-it-works.md)。

---

## 当前状态

| 指标 | 值 |
|---|---|
| EA best（small schema, Claude/DeepSeek） | **95.0%** |
| EA（large schema, MiniMax M2.7, retry=2） | **72.5%** |
| EA（large schema, DeepSeek V3.2） | **65.0%** |
| EA（large schema, Claude Sonnet 4.6） | **57.5%** |
| 编译器测试用例 | **53** |
| Spider2-Lite 编译成功率 | **97.6%** |
| Spider2-Lite EA | **9.2%** |

详见 [基准测试详情](docs/benchmarks.md)。

---

## 项目结构

```
forge/
  ├── schema.json          — Forge DSL 格式定义（JSON Schema）
  ├── compiler.py          — 确定性编译器：Forge JSON → SQL
  ├── retriever.py         — Schema 向量检索器（四层召回）
  ├── executor.py          — SQL 执行器
  ├── cache.py             — 查询缓存（精确 + 模糊匹配）
  ├── chart.py             — 图表生成
  └── cli.py               — CLI 入口

agent/
  ├── agent.py             — Agent 调度（查询 / 指标定义 / 缓存反馈）
  ├── session.py           — 会话状态
  └── llm.py               — LLM 客户端（RAG 过滤 tool schema）

registry/
  └── sync.py              — forge sync：直连数据库生成 Registry

tests/
  ├── test_compiler.py     — 编译器单元测试（53 个用例）
  ├── accuracy/            — 自有 40 题基准
  └── spider2/             — Spider2-Lite SQLite 子集（123 题）
```

---

## 文档

| 文档 | 内容 |
|---|---|
| [架构设计](docs/architecture.md) | 系统整体架构与模块职责 |
| [工作原理与 DSL 能力](docs/how-it-works.md) | 执行流程详解、DSL 特性表、Schema RAG |
| [基准测试详情](docs/benchmarks.md) | 版本演化、跨模型 EA 对比、Spider2 结果 |
| [设计哲学与工程洞察](docs/philosophy.md) | 核心哲学、工程经验、开放问题 |
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

---

## License

MIT
