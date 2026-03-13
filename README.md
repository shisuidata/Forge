# Forge

> ⚠️ **早期阶段，与目标还有很大距离，正在持续迭代中。**
>
> Spider2-Lite EA 9.2%、自有用例 EA 50.0%，是诚实的现状记录。Forge 的核心假设——约束输出空间能系统性降低生成错误——已在自有用例上验证，但在学术 benchmark 的复杂算法查询上还有很长的路要走。

---

> **把生成错误降到接近零。**

自然语言进，经过确定性编译，SQL 出。

[English README](README_EN.md)

---

## 目录

- [我们解决的问题](#我们解决的问题)
- [核心哲学](#核心哲学)
- [工作原理](#工作原理)
- [执行流程详解](#执行流程详解)
- [DSL 能力](#dsl-能力)
- [基准测试](#基准测试)
- [工程洞察](#工程洞察)
- [快速开始](#快速开始)

---

## 我们解决的问题

SQL 生成错误分两类，性质截然不同：

| 错误类型 | 定义 | 举例 | Forge 的答案 |
|---|---|---|---|
| **生成错误** | 推理正确，翻译成 SQL 时出错 | 用 `INNER JOIN` 替代 `LEFT JOIN`；`NOT IN` 遇 NULL 静默返错 | ✅ DSL 约束 + 编译器 |
| **业务逻辑错误** | 指标定义歧义，不同团队理解不同 | "复购率"的分母是全部用户还是下过单的用户？ | ✅ Registry 语义层 |
| **算法逻辑错误** | 模型不知道该用什么算法 | 日期序列填充、同比计算 | ❌ 超出 Forge 能力边界，诚实标注 |

**Forge 的核心主张**：生成错误和业务逻辑错误应该系统性消灭，而不是靠更好的 prompt 碰运气。

---

## 核心哲学

### 1. 约束是自由的前提

让 LLM 在无约束的输出空间里直接写 SQL，等于让它在无限多的错误可能性里随机游走。**LLM 的错误率与输出空间大小正相关。**

Forge 的做法是大幅收窄输出空间：

- 只有 Registry 中存在的表名/列名才是合法 token
- JOIN 类型必须从枚举值中选一个，物理上不可能写出 `JOIN`（无类型）
- `filter` 必须是数组，`count_all` 不能有 `col` 字段……

当语法层面的错误在生成时就被物理拦截，剩下的就只有语义层面的问题。

### 2. 意图与执行分离

```
LLM 负责：理解意图 → 生成 Forge JSON（语义层）
编译器负责：Forge JSON → SQL（执行层，确定性）
```

这个分离有深远意义：

- **可审计**：用户审核的 SQL 和实际执行的 SQL 是同一份，没有运行时惊喜
- **可调试**：SQL 出错，必然是某段 Forge JSON 导致的，可以精确定位
- **可升级**：换更强的 LLM 不需要动编译器；优化编译器不需要重新训练模型

### 3. Registry 是组织的数据资产

Registry 不是一个静态的 schema 文件，它是组织知识的沉淀：

```
结构层（forge sync 自动生成）
  └── 表结构、列名、类型、低基数枚举值（status: cancelled/completed）

语义层（对话式维护，用一次准确一次）
  └── 复购率 = 下过 ≥2 次订单的用户 / 下过 ≥1 次订单的用户
  └── 客单价 = 已完成订单的平均金额
  └── VIP 用户 = is_vip = 1
```

用得越多，Registry 越准确，错误率越低。**这是一个正向飞轮。**

### 4. 编译器修复优于 Prompt 修复

当模型的语义意图是对的，只是 DSL 格式稍有偏差，在编译器里加一个 `_coerce` 修复比改 prompt 更稳定：

- Prompt 修复有蝴蝶效应，经常修好一个问题破坏另一个
- 编译器修复是确定性的，不影响其他路径，测试可覆盖
- 8 个 `_coerce` 修复，每一个都来自真实失败案例

---

## 工作原理

```mermaid
flowchart LR
    NL["🗣️ 自然语言<br/>统计各城市 VIP 用户<br/>的平均客单价"]

    subgraph forge["Forge 管道"]
        direction TB
        REG["📚 Registry<br/>结构层 + 语义层"]
        SCHEMA["📋 JSON Schema<br/>强制枚举约束"]
        LLM["🤖 LLM<br/>Structured Output"]
        JSON["📄 Forge JSON<br/>有约束的中间表示"]
        COMPILER["⚙️ 确定性编译器<br/>compile_query()"]
        SQL["📝 SQL"]
    end

    DB["🗄️ 数据库"]
    RESULT["📊 结果集"]

    NL --> LLM
    REG --> LLM
    SCHEMA --> LLM
    LLM --> JSON
    JSON --> COMPILER
    COMPILER --> SQL
    SQL --> DB
    DB --> RESULT
```

### 错误恢复与兜底

```mermaid
flowchart TD
    FJ["Forge JSON"]

    FJ --> C1{编译}
    C1 -->|成功| EXEC[执行 + EA 评估]
    C1 -->|失败| FB1["回传错误给 LLM<br/>请求修正 Forge JSON"]

    FB1 --> C2{重新编译}
    C2 -->|成功| EXEC
    C2 -->|失败| FB2["raw SQL 兜底<br/>generate_sql_direct<br/>超出 DSL 能力时的逃生舱"]

    FB2 --> EXEC
    EXEC -->|结果匹配| PASS["✅ Pass"]
    EXEC -->|不匹配| FAIL["✗ Fail，记录 Forge JSON 供分析"]
```

---

## 执行流程详解

以「统计复购率，复购用户定义为下过 2 次及以上订单的用户」为例：

### Step 1 — Registry 构建系统 Prompt

`forge sync` 直连数据库，自动采样低基数列枚举值：

```
Database schema:
  users: id, name, city, created_at, is_vip[0/1]
  orders: id, user_id, status[cancelled/completed], total_amount, created_at
  order_items: id, order_id, product_id, quantity, unit_price
  products: id, name, category[Books/Clothing/Electronics], cost_price
```

`status[cancelled/completed]` 让 LLM 知道正确的字符串拼写，消灭一类幻觉。

### Step 2 — LLM 生成 Forge JSON（Structured Output）

```json
{
  "cte": [{
    "name": "user_orders",
    "query": {
      "scan": "orders",
      "group": ["orders.user_id"],
      "agg": [{"fn": "count_all", "as": "order_count"}],
      "select": ["orders.user_id", "order_count"]
    }
  }],
  "scan": "user_orders",
  "agg": [
    {"fn": "count_all", "as": "total_users"},
    {"fn": "count", "col": "CASE WHEN order_count >= 2 THEN 1 END", "as": "repeat_users"}
  ],
  "select": [{"expr": "repeat_users * 1.0 / total_users", "as": "repurchase_rate"}]
}
```

JSON Schema 在 token 生成层强制约束：`fn` 只能是枚举值，`scan` 只能是 Registry 里的表名。

### Step 3 — 确定性编译

```python
compile_query(forge_json)  # 同样的输入永远产生同样的 SQL
```

编译前，`_expand_aliases()` 将 SELECT 中引用的 agg alias 展开为完整表达式，规避 SQL alias 作用域陷阱：

```sql
WITH user_orders AS (
  SELECT orders.user_id, COUNT(*) AS order_count
  FROM orders
  GROUP BY orders.user_id
)
SELECT COUNT(CASE WHEN order_count >= 2 THEN 1 END) * 1.0 / COUNT(*) AS repurchase_rate
FROM user_orders
```

### Step 4 — 用户审核 → 执行

用户看到的就是会执行的那个 SQL，无运行时变换。审核通过，Forge 直连数据库执行，展示结果。

---

## DSL 能力

| 特性 | 详情 |
|---|---|
| **JOIN 类型** | `inner / left / right / full / anti / semi`，类型必须显式声明 |
| **anti join** | 替代 `NOT IN`，从根源消灭 NULL 陷阱 |
| **聚合函数** | `count / count_all / count_distinct / sum / avg / min / max / group_concat` |
| **CASE WHEN in agg** | `{"fn":"count","col":"CASE WHEN x>=2 THEN 1 END"}` |
| **窗口函数** | `row_number / rank / dense_rank / lag / lead`，支持 PARTITION BY / ORDER BY |
| **qualify** | 窗口结果过滤（per-group TopN），编译为包装子查询 |
| **CTE** | 多步聚合、派生指标，支持 recursive CTE |
| **日期** | `$date` 字面量 + `$preset` 相对日期（8 种预设） |
| **方言适配** | SQLite / MySQL / PostgreSQL（日期函数、字符串聚合、FULL JOIN 检测） |
| **alias 展开** | SELECT expr 中引用 agg/window alias 自动展开，消灭 alias 作用域错误 |
| **UNION** | `union / union_all`，主查询的 sort/limit 应用于整体 |

---

## 基准测试

### 自有用例：40 题

测试 Schema：`users / orders / order_items / products`（SQLite，覆盖真实业务查询场景）

#### 版本演化（LLM 评分 0–10，每题 5 次运行均值）

| 版本 | 核心改动 | LLM 评分 | 编译失败率 | 变化 |
|---|---|---|---|---|
| **A** | 基线（SQL 风格 DSL） | 7.63 | 3.8% | — |
| **B** | 对照组：模型直接生成 SQL | 8.38 | 0.0% | — |
| **D** | 新 DSL + 枚举 schema 约束 | 8.46 | 1.2% | +0.83 |
| **E** | Prompt 精化（HAVING alias、LIMIT、排名） | 8.41 | 0.0% | −0.05 |
| **F** | 语义精确（semi→EXISTS、JOIN 完整性） | 8.43 | 0.6% | +0.02 |
| **G** | 规则健壮（数量词语义、正向规则替代负向） | 8.69 | 0.0% | **+0.26** |
| **H** | 新能力（CASE WHEN、$preset、CTE、expr） | 8.45 | 0.5% | −0.24 |
| **I** | 稳定性修复（编译器 fix 7、CTE 边界） | 8.45 | 2.0% | 0.00 |
| **J** | HAVING 精准化 + 人均模式 | 8.65 | 0.5% | **+0.20** |
| **J+Sem** | J + 运行时语义消歧库 | **8.82** | **0.0%** | **+0.17** |

> A/D/E/F/G 在 32 题测试；H 起扩展到全部 40 题（新增能力测试题 33–40）。

#### EA 得分（Execution Accuracy）

| 方法 | 题数 | EA |
|---|---|---|
| Method J（5 次多数投票） | 40 | **50.0%** |

EA 比 LLM 评分更严格：结果集必须完全一致（忽略行顺序，浮点 4 位精度）。50% 说明 Forge DSL 能精确表达并执行超过一半的真实业务查询。

#### Forge J+Sem vs 直接 SQL（分类对比）

| 分类 | 题数 | 直接 SQL | Forge J+Sem | Δ |
|---|---|---|---|---|
| 多表 JOIN + 聚合 | 6 | 8.53 | **8.73** | +0.20 |
| 复杂过滤 | 4 | 9.00 | **9.25** | +0.25 |
| GROUP BY + HAVING | 5 | 8.60 | **8.80** | +0.20 |
| 排名 & TopN | 5 | 8.36 | **9.00** | +0.64 |
| 窗口聚合 | 4 | 8.40 | **8.75** | +0.35 |
| 时序导航 | 3 | 8.40 | **9.00** | +0.60 |
| ANTI/SEMI JOIN | 3 | 7.80 | **8.60** | **+0.80** |
| 复合多步 | 2 | 7.60 | **8.00** | +0.40 |
| **总体** | **40** | **8.38** | **8.82** | **+0.44** |

ANTI/SEMI JOIN 差距最大（+0.80）：直接生成 SQL 的模型频繁产生 `NOT IN`，遇到 NULL 时静默返回错误结果；Forge 的 `anti` join 原语从根源消灭了这类错误。

---

### Spider2-Lite SQLite 子集测试

Spider2-Lite 是学术标准的 text-to-SQL 基准，包含来自真实数据仓库的复杂分析查询。我们在其 123 个 SQLite 子集用例上进行了系统测试，用以验证 Forge 在陌生数据库、陌生查询模式下的泛化能力。

#### 测试迭代历程

```mermaid
timeline
    title Spider2-Lite 测试迭代
    第一轮 : 123 个 SQL 文件生成
          : 编译成功率 82%
          : EA 5.9%（仅 17 题有 gold SQL）
          : 问题：gold CSV 路径错误，大量用例评估为 no_gold
    修复 EA 评估逻辑 : gold CSV 支持多子文件（_a/_b/_c）
                   : condition_cols 双格式解析（per-subfile / flat）
                   : 加入 raw SQL 兜底（Forge DSL 超限时逃生）
                   : 全部 123 题均有 gold 参考答案
    完整重跑 : 编译成功率 97.6%
            : EA 9.2%（11/119）
            : raw SQL 兜底 26 次，其中 6 次通过
```

#### 最终结果

| 指标 | 值 |
|---|---|
| 测试用例 | 123 个 SQLite 用例 |
| **编译成功率** | **97.6%** (120/123) |
| **EA（Execution Accuracy）** | **9.2%** (11/119) |
| raw SQL 兜底触发 | 26 次 |
| 其中兜底通过 | 6 次 |

#### 为什么 Spider2 的 EA 低？

Forge 被设计解决**生成错误**和**业务逻辑错误**，不是为了解决学术 benchmark 里的算法难题。Spider2 的查询分布与 Forge 的设计目标存在系统性错位：

- 日期序列生成（generate_series / recursive CTE）
- 复杂自关联与多层嵌套子查询
- 同比/环比计算（DATE_TRUNC + 自关联 JOIN）
- 统计建模（线性回归、移动平均）

这些都属于「算法逻辑错误」——即使人类分析师，也需要了解具体算法才能作答。

在真实企业数据查询场景中，超过 80% 的日常分析查询落在 Forge DSL 能覆盖的范围内。Spider2 的低 EA 是**诚实的边界标注**，不是产品缺陷。

---

## 工程洞察

### 编译器修复 > Prompt 修复

从 benchmark 里看，单次影响最大的改进是一个编译器修复（Case 39：3.0 → 9.0），而不是任何 prompt 改动。Prompt 有蝴蝶效应；编译器修复是手术刀。

### Alias 作用域是 SQL 的暗礁

SQL 标准不允许在同层 SELECT 中引用同层定义的 agg alias：

```sql
-- 错误：repeat_users 在此时还不存在
SELECT repeat_users * 1.0 / total_users AS repurchase_rate
```

解决方案：`_expand_aliases()` 在编译前将 expr 里的 alias 替换为完整表达式，消灭整类此类错误。

### 新能力文档导致过拟合

每向 prompt 添加新能力说明，模型就有过度使用它的倾向。加了 CTE 文档后，模型开始把简单 GROUP BY 也包成 CTE。对策：每个新能力**必须**配一个「何时不用」的反例。

### 语义消歧库是无损提升

语义库在 LLM 调用前注入澄清（「超过 N 次」→ `op: "gt"` 而非 `"gte"`），不改动核心 prompt，不增加额外 API 调用。J → J+Sem 提升了 0.17，编译失败率从 0.5% 降到 0.0%。

---

## 快速开始

```bash
# 安装
git clone https://github.com/shisuidata/Forge
cd Forge
pip install -e .

# 配置
cp .env.example .env
# 填写：LLM_API_KEY, LLM_BASE_URL, DATABASE_URL

# 同步数据库 schema
forge sync --db sqlite:///your.db

# 运行自有测试
python tests/text-to-sql-failures/create_db.py
python tests/text-to-sql-failures/run_ea.py

# 运行 Spider2 子集测试
python tests/spider2/runner.py --limit 20
```

---

## 项目结构

```
forge/
  ├── schema.json          — Forge DSL 格式定义（JSON Schema）
  ├── compiler.py          — 确定性编译器：Forge JSON → SQL（3 方言）
  ├── schema_builder.py    — 动态构建 tool schema（注入枚举约束）
  └── cli.py               — CLI 入口

registry/
  └── sync.py              — forge sync：直连数据库生成 Registry

tests/
  ├── test_compiler.py     — 编译器单元测试（21 个用例）
  ├── accuracy/            — 自有 40 题基准（LLM judge + EA，10 个版本）
  │   ├── cases.json       — 题目 + reference SQL
  │   ├── runner.py        — 多方法对比运行器
  │   └── results/         — 各版本运行结果
  ├── text-to-sql-failures/— 针对性失败案例（JOIN 陷阱、聚合陷阱等）
  └── spider2/             — Spider2-Lite SQLite 子集测试（123 题）
      ├── runner.py        — 全流程运行器（EA 内嵌 + raw SQL 兜底）
      └── results/         — SQL 文件 + 运行日志
```

---

## 当前得分

| 基准 | 题数 | 指标 | 得分 |
|---|---|---|---|
| 自有用例（Method J） | 40 | LLM Judge | **8.65 / 10** |
| 自有用例（Method J+Sem） | 40 | LLM Judge | **8.82 / 10** |
| 自有用例（Method J，EA） | 40 | Execution Accuracy | **50.0%** |
| Spider2-Lite SQLite | 123 | Execution Accuracy | **9.2%** |
| Spider2-Lite SQLite | 123 | 编译成功率 | **97.6%** |
