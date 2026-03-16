# Forge

**面向数据团队的 AI 查询 Agent —— 自然语言输入，确定性 SQL 输出。**

[English](README.md)

---

## Text-to-SQL 的根本问题

大多数 Text-to-SQL 方案直接让 LLM 输出 SQL，这种方式有一类固有失效模式：

| 失效类型 | 典型例子 |
|---|---|
| 字段名幻觉 | 输出 `orders.amount`，实际字段是 `total_amount` |
| JOIN 类型推断错误 | 用了 `INNER JOIN`，问题语义需要 `LEFT JOIN` |
| NOT IN 遇 NULL 静默失败 | `WHERE id NOT IN (子查询)` 在子查询含 NULL 时返回空结果 |
| WHERE 与 HAVING 混淆 | 把聚合过滤条件写进 WHERE |
| 业务指标定义歧义 | "复购率"在不同团队定义不同，模型无从判断 |
| SQL 方言差异 | 写了 PostgreSQL 语法但数据库是 SQLite |

根本原因：**LLM 在无约束的输出空间中生成，任何 token 在任何位置都是合法的，所以任何错误都可能发生。**

## Forge 的解法

Forge 在 LLM 和 SQL 之间插入一个结构化中间表示：

```
自然语言
    ↓
LLM（Structured Output）
    ↓
Forge JSON  ← 受约束：只有注册表中存在的表名/字段名才是合法 token
    ↓
确定性编译器
    ↓
SQL
```

核心洞察：**LLM 的错误率正比于输出空间大小。** 如果模型只能输出合法字段名（由 JSON Schema 在 token 级别强制约束），整整一类错误就在物理上不可能发生。

### 三层防御体系

**第一层 —— Schema 约束**（让错误生成物理上不可能发生）

`schema.registry.json` 定义所有合法的表和字段。Forge DSL 的 JSON Schema 将 `scan`、`filter.col`、`select` 等字段声明为严格枚举。Anthropic Structured Output 在 token 级别执行这一约束 —— 如果注册表中只有 `orders.total_amount`，模型就无法生成 `orders.amount`。

**第二层 —— `_coerce` 编译器修复**（意图正确，格式偏差）

模型有时输出的内容语义正确但结构略有偏差（例如 filter 写成对象而非数组、缺少 GROUP BY 字段）。编译器的 `_coerce()` 函数在编译前进行标准化。目前累积了 7 条从真实失败案例中提取的修复规则。

**第三层 —— 语义消歧库**（运行时歧义消解）

对已知的歧义查询模式，`semantic_lib.py` 在 LLM 调用前追加内联说明。例如：「超过 5 次」会被标注为明确使用 `op: "gt"` 而非 `op: "gte"`。纯正则匹配，零延迟，不增加 API 调用。

### 编译器保证

对于合法的 Forge JSON，编译器总能输出合法 SQL。相同 JSON → 相同 SQL，每次都是。这意味着：

- 错误可追溯：如果 SQL 不对，产生它的 Forge JSON 有日志可查
- LLM 的任务缩减为语义理解，不再负责 SQL 语法
- 方言支持是编译器的问题，不是 Prompt 的问题

## 架构

```
飞书 / 钉钉              ← 用户交互
后台管理页面             ← 数据工程师：Registry 管理、审计日志、系统配置
        ↓
Forge 后端（FastAPI，私有化部署）
  ├── Agent 循环         ← 查询模式 + 指标定义模式
  ├── LLM 客户端         ← Anthropic 或任意 OpenAI 兼容模型
  ├── Forge 编译器       ← Forge JSON → SQL（确定性）
  ├── Registry           ← 数据库结构 + 业务指标
  └── 数据库连接         ← 表结构同步 + 查询执行
```

私有化部署，自带 LLM API Key，数据不出内网。

## Forge DSL

Forge JSON 由 LLM 生成，确定性编译为 SQL。

```json
{
  "scan": "orders",
  "joins": [{"type": "inner", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "group":  ["users.city"],
  "agg":    [{"fn": "avg", "col": "orders.total_amount", "as": "avg_value"}],
  "select": ["users.city", "avg_value"],
  "sort":   [{"col": "avg_value", "dir": "desc"}]
}
```

编译结果：

```sql
SELECT users.city, AVG(orders.total_amount) AS avg_value
FROM orders
INNER JOIN users ON orders.user_id = users.id
WHERE orders.status = 'completed'
GROUP BY users.city
ORDER BY avg_value DESC
```

### DSL 能力一览

| 能力 | 说明 |
|---|---|
| 全部 JOIN 类型 | `inner / left / right / full / anti / semi` |
| 不等式 JOIN | `on_multi`：支持 `gt/gte/lt/lte/neq` 的多条件数组 |
| 聚合函数 | `count / count_all / count_distinct / sum / avg / min / max` |
| 窗口函数 | `row_number / rank / dense_rank / lag / lead`，支持 PARTITION BY / ORDER BY |
| CASE WHEN | `{"case": [...], "else": ..., "as": "alias"}` |
| 相对日期 | `{"$preset": "today / this_week / this_month / this_year / last_30_days"}` |
| CTE（多步聚合） | `"with": [{"name": "cte_name", "query": {...}}]` |
| 函数表达式 | `{"$expr": "STRFTIME('%Y-%m', t.col)"}` |
| OR 条件 | filter 数组中使用 `{"or": [{...}, {...}]}` |

## 准确性测试结果

在 40 个查询用例（8 个类别，难度 1–3）上测试了 9 个提示词版本。每个版本 × 每个用例 × 5 次运行，由 LLM 裁判打分（0–10）。

### 版本演进全景

| 版本 | 核心改动 | 均分 | 编译失败率 | vs 上版 |
|---|---|---|---|---|
| **A** | 基线（SQL 术语风格 DSL） | 7.63 | 3.8% | — |
| **B** | 对照组：直接生成 SQL | 8.38 | 0.0% | — |
| **D** | 新 DSL + 枚举 Schema | 8.46 | 1.2% | +0.83 vs A |
| **E** | 提示词精细化（HAVING alias、LIMIT、排名函数） | 8.41 | 0.0% | -0.05 |
| **F** | 语义精确化（semi→EXISTS、JOIN 完整性） | 8.43 | 0.6% | +0.02 |
| **G** | 规则稳健化（量词语义、正向规则、JSON 强化） | 8.69 | 0.0% | **+0.26** |
| **H** | 能力扩展（CASE WHEN、$preset、CTE、函数列） | 8.45 | 0.5% | -0.24 |
| **I** | 稳健性修复（编译器修复7、CTE 边界） | 8.45 | 2.0% | 0.00 |
| **J** | HAVING 精准化 + 人均模式澄清 | 8.65 | 0.5% | **+0.20** |
| **J+语义库** | J + 语义消歧库预处理 | **8.82** | **0.0%** | **+0.17** |
| **K** | 大 Schema（200 表电商数仓）+ 四层召回 + RAG 过滤 | **8.07** | **5.0%** | 基准切换¹ |
| **b_large** | 大 Schema 直出 SQL（对照组） | **8.25** | **0.0%** | 同基准² |
| **b_large_sem** | 大 Schema 直出 SQL + 语义消歧库 | **8.33** | **0.0%** | 同基准² |

> A/D/E/F/G 在 32 个用例上测试；H 起在全部 40 个用例上测试（含新能力用例 33–40）。
> ¹ Method K 换用 14 张真实电商数仓表（200 表子集），全新 40 题，与 J 系列不可直接比较。
> ² b_large/b_large_sem 与 K 使用同一套 40 题，三方公平对比。

### J+语义库 vs 直接生成 SQL（当前最优 vs 对照组）

| 类别 | 用例数 | 直接 SQL | Forge J+语义库 |
|---|---|---|---|
| 多表 JOIN + 聚合 | 6 | 8.53 | **8.73** |
| 复杂过滤 | 4 | 9.00 | **9.25** |
| 分组 + HAVING | 5 | 8.60 | **8.80** |
| 排名与 TopN | 5 | 8.36 | **9.00** |
| 窗口聚合 | 4 | 8.40 | **8.75** |
| 时序导航 | 3 | 8.40 | **9.00** |
| ANTI/SEMI JOIN | 3 | 7.80 | **8.60** |
| 综合复杂查询 | 2 | 7.60 | **8.00** |
| **总体** | **40** | **8.38** | **8.82** |

Forge J+语义库在全部类别均优于直接生成 SQL。ANTI/SEMI JOIN 差距最大（+0.80）：直接生成 SQL 频繁出现 `NOT IN` 遇 NULL 的静默错误；Forge 的 `anti` join 原语从根本上消灭了这类错误。

### 大 Schema 三方基准（K vs 直出 SQL，40 题公平对比）

| 类别 | 用例数 | K（Forge DSL） | b_large（直出 SQL） | b_large_sem（直出+语义库） |
|---|---|---|---|---|
| 多表 JOIN + 聚合 | 5 | 8.53 | 8.33 | **9.00** |
| 复杂过滤 | 5 | 8.20 | **9.00** | 8.60 |
| 分组 + HAVING | 5 | 8.27 | 8.73 | **8.87** |
| 排名与 TopN | 5 | 8.27 | 8.40 | **8.53** |
| 窗口聚合 | 5 | 7.87 | 8.00 | **8.20** |
| 时序导航 | 5 | 7.53 | 8.20 | **8.67** |
| ANTI/SEMI JOIN | 5 | **7.40** | 7.20 | 7.13 |
| 综合复杂查询 | 5 | **8.47** | 8.14 | 7.60 |
| **总体** | **40** | **8.07** | **8.25** | **8.33** |

在 14 张核心表的规模下，直出 SQL 整体略胜 Forge DSL。原因：Schema 规模尚在 LLM 可以直接记忆的范围，Forge DSL 的多层介入（RAG 召回 → DSL 生成 → 编译）引入了额外失败路径（5.0% 编译失败率），而直出 SQL 可以不受 DSL 语法限制地表达任意 SQL 结构。

Forge 的优势在 ANTI/SEMI JOIN 仍然成立：直出 SQL 的 `NOT IN` 有 NULL 陷阱，K 的 `anti` join 原语在这个子类是唯一真正安全的方案。语义库在大 Schema 场景净收益 +0.08，主要修复了 `pay_amount` vs `total_amount` 的歧义，但在少数用例（Case 7）改变了查询语义。

### 关键工程经验

**编译器修复比提示词修复更稳定。** 当模型意图正确、DSL 格式略有偏差时，在编译器层面做 `_coerce` 修复稳定性更高且无副作用。本次测试中单项提升最大的改进是编译器修复（Case 39: 3.0 → 9.0）。

**正向规则比负向规则副作用更少。** "正确做法是 X"比"禁止 Y"产生的副作用更少。版本 F 的「禁止虚构 HAVING 条件」规则导致 Case 4、9 回退。版本 G 将其改为正向模式示例，恢复 +0.26。

**新能力文档会导致 Overfitting。** 每次向 Prompt 新增能力文档，模型都有过度应用的风险。H 加入 CTE 文档后，模型开始对不需要 CTE 的查询也使用 CTE（Case 14 回退）。缓解方式：每个新能力文档必须配套「不该用的场景」反例。

**语义消歧库有时会改变查询语义。** 在小 Schema 场景（J 系列）语义库是纯加法（8.65→8.82）；在大 Schema 场景，语义库对字段名歧义的消解效果明显，但对查询意图本身的重新解读（如「客单价」改为用户平均消费聚合）可能引入新的语义偏差。净收益仍然为正，但不像小 Schema 那样"无副作用"。

## 快速开始

### 1. 安装

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
pip install -e .
```

### 2. 配置

```bash
cp .env.example .env
# 编辑 .env：填入 FEISHU_APP_ID、LLM_API_KEY、DATABASE_URL
```

### 3. 同步数据库表结构

```bash
forge sync
# 或指定连接：forge sync --db postgresql://user:pass@host/db
```

### 4. 启动服务

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 5. 配置飞书 Webhook

在[飞书开放平台](https://open.feishu.cn)将事件订阅地址设置为 `https://your-server/webhook/feishu`。

详见 [docs/feishu-setup.md](docs/feishu-setup.md)。

## 项目结构

```
forge/
  ├── compiler.py     — 确定性编译器：Forge JSON → SQL（3 方言，14 个容错修复）
  ├── retriever.py    — 四层召回：指标直接匹配 → embedding → 列名 → FK 扩展
  ├── executor.py     — SQL 执行器
  ├── cache.py        — SQL 查询缓存（精确 + 模糊，双阶段用户反馈）
  └── chart.py        — 图表生成（pyecharts 交互 + matplotlib 静态）

agent/
  ├── agent.py        — Agent 调度（查询 / 指标定义 / 缓存反馈）
  └── session.py      — 会话状态

web/
  └── feishu.py       — 飞书 WebSocket Bot（每用户串行队列 + 交互卡片）

demo/
  └── seed_large.py   — 200 张表电商数仓 mock 数据

tests/
  ├── accuracy/       — 自有 40 题基准（10 个版本）
  └── spider2/        — Spider2-Lite SQLite 子集（123 题）

docs/devlog/          — 开发日志
main.py               — FastAPI 入口
```

## 开发日志

| 篇 | 日期 | 主题 |
|---|---|---|
| [Day 0 · 开发实录](docs/devlog/forge-dev-story.md) | 2026-03 | 为什么做这件事；错误分类框架；核心洞见 |
| [Day 1 · 历史债 / 地面泥潭](docs/devlog/day1_2026-03-15.md) | 2026-03-15 | SQL 设计哲学、四层召回演进、飞书 Bot 踩坑、SQL 缓存双阶段反馈 |
| [Day 2 · CROSS JOIN / HAVING 别名 / EA 95%](docs/devlog/day2_2026-03-16.md) | 2026-03-16 | CROSS JOIN 标量 CTE 模式、HAVING alias 展开、DeepSeek strict tool calling 实验、M/O/N 三组 EA 基准 |

## 当前状态

编译引擎完成，53 个单元测试全绿。飞书 Bot 可用（WebSocket 长连接，交互卡片，SQL 语法高亮，图表生成）。SQL 查询缓存已集成（双阶段用户反馈 → verified 条目构成组织知识库）。

当前基准：**8.82 / 10**（Method J+语义库，40 用例 LLM Judge）；**95%**（Method M/O，small 数据集 EA，Claude + DeepSeek V3）；**65%**（Method N，large 200 表数仓 × DeepSeek V3）。
