# Forge 开发者文档

## 目录

1. [项目概览](#1-项目概览)
2. [目录结构](#2-目录结构)
3. [快速开始](#3-快速开始)
4. [核心模块详解](#4-核心模块详解)
   - 4.1 [Forge DSL 编译器（forge/）](#41-forge-dsl-编译器forge)
   - 4.2 [Agent 调度层（agent/）](#42-agent-调度层agent)
   - 4.3 [注册表层（registry/）](#43-注册表层registry)
   - 4.4 [Web 管理后台（web/）](#44-web-管理后台web)
   - 4.5 [配置模块（config.py）](#45-配置模块configpy)
5. [数据流与调用链路](#5-数据流与调用链路)
6. [Forge DSL 规范](#6-forge-dsl-规范)
   - 6.1 [顶层字段](#61-顶层字段)
   - 6.2 [JOIN 类型](#62-join-类型)
   - 6.3 [过滤条件（filter / having）](#63-过滤条件filter--having)
   - 6.4 [聚合函数（agg）](#64-聚合函数agg)
   - 6.5 [窗口函数（window）](#65-窗口函数window)
   - 6.6 [值类型](#66-值类型)
7. [注册表规范](#7-注册表规范)
   - 7.1 [结构层（schema.registry.json）](#71-结构层schemaregistryjson)
   - 7.2 [语义层（metrics.registry.yaml）](#72-语义层metricsregistryyaml)
8. [指标校验规则](#8-指标校验规则)
9. [测试说明](#9-测试说明)
10. [扩展指南](#10-扩展指南)

---

## 1. 项目概览

Forge 是一个面向数据团队的 **AI 数据查询助手**，核心思路是在自然语言和 SQL 之间引入一个中间层——**Forge DSL**。

```
用户自然语言
      ↓
   LLM 生成 Forge JSON（工具调用）
      ↓
   Forge 编译器（确定性）
      ↓
   SQL（用户审核后执行）
```

**关键设计决策：**

| 决策 | 原因 |
|---|---|
| LLM 生成 Forge JSON，不直接生成 SQL | SQL 难以校验，Forge JSON 有严格 Schema，可被确定性编译 |
| 两层注册表（结构层 + 语义层） | 表结构由 sync 自动生成，业务指标由人工/LLM 维护，职责分离 |
| 只有原子指标和衍生指标两种类型 | 足以覆盖 ADS 层的所有查询场景，避免过度抽象 |
| 审核后执行（approve/cancel） | 防止 LLM 误解意图直接修改数据，数据团队保持控制权 |
| anti join 替代 NOT IN | NOT IN 遇到 NULL 值会产生错误结果，anti join 是安全的等价替换 |

---

## 2. 目录结构

```
Forge/
├── forge/                    # DSL 编译器（核心，无外部依赖）
│   ├── schema.json           # Forge JSON Schema（jsonschema 校验用）
│   ├── compiler.py           # Forge JSON → SQL 编译器
│   └── cli.py                # 命令行入口（forge compile / forge sync）
│
├── agent/                    # Agent 调度层
│   ├── agent.py              # 主调度：process / approve / cancel
│   ├── session.py            # 会话状态管理（内存 SessionStore）
│   ├── llm.py                # LLM 客户端（Anthropic + OpenAI 兼容）
│   ├── prompts.py            # 系统提示词 + build_system()
│   └── audit.py              # 审计日志（aiosqlite）
│
├── registry/                 # 注册表管理层
│   ├── __init__.py           # 公开 run_sync / validate_metric
│   ├── sync.py               # 数据库内省 → schema.registry.json
│   └── validator.py          # 指标定义校验器
│
├── web/                      # 管理后台（FastAPI + Jinja2）
│   ├── router.py             # 路由：注册表 / 审计日志 / 系统设置
│   └── templates/            # HTML 模板（Tailwind CSS）
│       ├── base.html
│       ├── registry.html
│       ├── audit.html
│       └── settings.html
│
├── tests/                    # 测试套件
│   ├── test_compiler.py          # 编译器基础用例
│   ├── test_compiler_extended.py # 编译器扩展用例（全 join 类型、聚合边界）
│   ├── test_compiler_window.py   # 窗口函数用例
│   ├── test_metric_validator.py  # 指标校验器用例
│   ├── test_session.py           # 会话管理用例
│   ├── test_agent_logic.py       # Agent 调度逻辑用例（mock LLM）
│   ├── test_registry_context.py  # LLM 注册表上下文格式用例
│   └── test_sync.py              # 数据库同步用例
│
├── demo/
│   └── seed.py               # 演示数据库种子脚本
│
├── docs/                     # 文档
│   ├── development.md        # 本文档
│   ├── architecture.md       # 架构设计
│   ├── registry.md           # 注册表使用指南
│   └── feishu-setup.md       # 飞书机器人配置
│
├── config.py                 # 全局配置（环境变量读取）
├── main.py                   # FastAPI 应用入口
├── pyproject.toml            # 项目元数据 + 依赖声明
├── schema.registry.json      # 结构层注册表（forge sync 自动生成）
├── metrics.registry.yaml     # 语义层注册表（人工/LLM 维护）
└── metrics.registry.example.yaml  # 语义层模板（含字段说明）
```

---

## 3. 快速开始

### 安装依赖

```bash
pip install -e ".[dev]"
```

### 配置环境变量

复制 `.env.example` 并填写：

```bash
cp .env.example .env
```

最小配置（本地 SQLite 演示）：

```env
LLM_PROVIDER=anthropic
LLM_API_KEY=sk-ant-...
DATABASE_URL=sqlite:///./demo/forge_demo.db
```

### 初始化演示数据库

```bash
python demo/seed.py
```

### 同步数据库结构

```bash
forge sync
# 输出：已同步 4 张表 → schema.registry.json
```

### 运行测试

```bash
# 全部测试
pytest

# 单个文件
pytest tests/test_compiler.py -v

# 单个测试函数
pytest tests/test_compiler.py::test_inner_join -v
```

### 启动 Web 服务

```bash
uvicorn main:app --reload
# 访问 http://localhost:8000/admin
```

---

## 4. 核心模块详解

### 4.1 Forge DSL 编译器（forge/）

编译器是整个系统的**确定性核心**，无副作用，无随机性。

#### `forge/compiler.py`

**入口函数：**

```python
def compile_query(forge: dict) -> str:
    """校验 Forge JSON 并编译为 SQL。"""
```

**内部调用链：**

```
compile_query(forge)
  └─ jsonschema.validate(forge, _SCHEMA)    # Schema 校验，失败时抛出 ValidationError
  └─ _compile(forge)
       ├─ _select_exprs(q)                  # 构建 SELECT 表达式列表
       │    ├─ _agg_expr(agg)               # 普通聚合 → SUM(col) 等
       │    └─ _window_expr(w)              # 窗口函数 → fn() OVER (...)
       ├─ _join(join)                       # 每个 JOIN 定义 → SQL + 额外 WHERE
       ├─ _condition(cond)                  # 条件表达式（支持 OR 组递归）
       └─ _val(v)                           # Python 值 → SQL 字面量
```

**值格式化优先级（`_val`）：**

```python
{"$date": "2024-01-01"}  → '2024-01-01'   # 日期类型，优先检测
True / False             → TRUE / FALSE   # bool 必须在 int 之前检测（bool 是 int 子类）
"string"                 → 'string'       # 字符串，内部单引号转义为 ''
42 / 3.14               → 42 / 3.14      # 数字直接 str() 转换
```

**anti join 实现原理：**

```sql
-- Forge JSON
{"type": "anti", "table": "order_items", "on": {"left": "orders.id", "right": "order_items.order_id"}}

-- 编译结果
LEFT JOIN order_items ON orders.id = order_items.order_id
WHERE order_items.order_id IS NULL
```

anti join 比 `NOT IN` 更安全：当右表含 NULL 值时，`NOT IN` 的结果是空集；LEFT JOIN + IS NULL 则正确处理 NULL。

#### `forge/schema.json`

定义 Forge DSL 的 JSON Schema（Draft-07），作为 LLM 的工具参数 Schema 和编译前的校验依据。

关键约束点：

- `additionalProperties: false` — 所有对象不允许额外字段，防止 LLM 发明不存在的字段
- `required` 精确声明 — 每个子 Schema 明确必填字段
- `enum` 约束字符串类型 — join.type、agg.fn、sort.dir 等字段均使用枚举
- `oneOf` 区分变体 — Aggregation（AggWithCol vs AggCountAll）和 WindowExpr（三种变体）使用 oneOf

---

### 4.2 Agent 调度层（agent/）

#### `agent/agent.py`

核心 `process()` 函数的状态机：

```
process(user_id, user_text)
│
├── llm.call() → {"tool": None, "text": ...}
│   └── 返回 AgentResponse(action="message")
│
├── llm.call() → {"tool": "generate_forge_query", "input": forge_json}
│   ├── compile_query(forge_json) 成功
│   │   └── session.pending_sql = sql
│   │   └── 返回 AgentResponse(action="sql_review", sql=sql)
│   └── compile_query(forge_json) 失败
│       ├── attempt < MAX_RETRIES → 注入错误到 session，continue 重试
│       └── 超出重试上限 → 返回 AgentResponse(action="error")
│
└── llm.call() → {"tool": "define_metric", "input": metric}
    ├── _validate_and_save() 通过 → 返回 AgentResponse(action="metric_saved")
    └── _validate_and_save() 失败 → 返回 AgentResponse(action="error")
```

**重试机制：**

```python
MAX_RETRIES = 2   # 最多重试 2 次，共最多 3 次 LLM 调用

for attempt in range(1 + MAX_RETRIES):
    result = llm.call(session.recent())
    try:
        sql = compile_query(result["input"])
        break   # 成功则跳出
    except Exception as exc:
        if attempt < MAX_RETRIES:
            # 将错误注入 session，LLM 在下一轮看到错误并自我修正
            session.add("user", f"[系统] 编译错误：{exc}")
            continue
        # 已达上限，报错
```

#### `agent/session.py`

`Session` 的关键约束：

- **历史截断**：超过 20 条消息时从头部丢弃，保留最近 20 条（约 10 轮对话）
- **pending 状态**：`pending_sql` 和 `pending_forge` 必须同步操作，approve/cancel 都会同时清空两者
- **全局单例**：`store = SessionStore()` 在模块加载时创建，整个进程共享

#### `agent/llm.py`

两个后端的关键差异：

| 差异点 | Anthropic | OpenAI 兼容 |
|---|---|---|
| 工具参数字段名 | `input_schema` | `parameters` |
| 工具调用结果位置 | `content[].type == "tool_use"` | `choices[0].message.tool_calls` |
| 函数参数类型 | dict 对象 | JSON 字符串（需 `json.loads`） |
| system 传递方式 | 独立的 `system` 参数 | messages 数组首条 |

**注册表上下文格式化（`_registry_context()`）：**

每次 LLM 调用前实时读取两个文件：

1. `schema.registry.json` → 格式化为「表结构」段落
2. `metrics.registry.yaml` → 分别格式化「原子指标」和「衍生指标」段落

每次实时读取的原因：schema 变更（如 `forge sync` 后或添加新指标）需要立即对 LLM 生效，无需重启服务。

---

### 4.3 注册表层（registry/）

#### `registry/sync.py`

**内省流程：**

```python
create_engine(database_url)     # 创建 SQLAlchemy 引擎
inspect(engine)                 # 创建 Inspector
inspector.get_table_names()     # 获取所有表名
inspector.get_columns(table)    # 获取每张表的字段列表
engine.dispose()                # 释放连接池
```

输出格式：

```json
{
  "tables": {
    "orders": {"columns": ["id", "user_id", "status", "total_amount", "created_at"]},
    "users":  {"columns": ["id", "name", "city", "is_vip"]}
  }
}
```

#### `registry/validator.py`

校验器的分发逻辑：

```
validate_metric(metric, structural_registry, metric_name, all_metrics)
  ├── 校验通用字段（label、description、metric_class）
  ├── _valid_columns(structural_registry) → 合法列引用集合
  ├── metric_class == "atomic"
  │   └── _validate_atomic(metric, valid_cols)
  │        ├── measure 格式 + 存在性
  │        ├── aggregation 枚举
  │        ├── qualifiers/dimensions/period_col 字段引用
  │        └── 软警告（无 qualifiers、无 dimensions）
  └── metric_class == "derivative"
      └── _validate_derivative(metric, metric_name, all_metrics)
           ├── numerator/denominator 必填
           ├── 自引用检测
           ├── 引用存在性 + 必须是 atomic
           ├── 粒度一致性（measure 表对比）
           ├── qualifier 一致性（可用 notes 抑制警告）
           └── period_col 一致性
```

---

### 4.4 Web 管理后台（web/）

FastAPI + Jinja2 实现，挂载在 `/admin` 路径下。

| 路由 | 方法 | 功能 |
|---|---|---|
| `/admin` | GET | 重定向到 `/admin/registry` |
| `/admin/registry` | GET | 注册表总览（表结构 + 原子指标 + 衍生指标） |
| `/admin/registry/metric` | POST | 新增或更新指标定义（含校验） |
| `/admin/registry/metric/{name}` | DELETE | 删除指标 |
| `/admin/audit` | GET | 审计日志（最近 100 条） |
| `/admin/settings` | GET | 系统配置（敏感信息脱敏显示） |

**指标保存流程（POST /admin/registry/metric）：**

```
表单数据 → 构造 metric dict
  → validate_metric() 校验
  ├── 校验失败 → 返回 422 + registry.html（含错误信息 + 表单预填）
  └── 校验通过 → 注入 updated_at → _save_metrics() → 重定向 303
```

---

### 4.5 配置模块（config.py）

所有配置通过环境变量读取，`Config` 类使用类属性（非实例属性）存储，`cfg = Config()` 创建全局单例。

```python
from config import cfg

cfg.DATABASE_URL    # str
cfg.REGISTRY_PATH   # Path 对象，可直接调用 .read_text() / .write_text()
cfg.LLM_PROVIDER    # "anthropic" | 其他（走 OpenAI 兼容接口）
```

---

## 5. 数据流与调用链路

### 查询流（NL → SQL）

```
飞书用户发消息
    │
    ▼
web/feishu_handler (或 web API)
    │  user_id, user_text
    ▼
agent.process(user_id, user_text)
    │
    ├── store.get(user_id) → Session
    │   session.add("user", user_text)
    │
    ├── llm.call(session.recent())
    │   ├── _registry_context()        # 读取 schema.registry.json + metrics.registry.yaml
    │   ├── build_system(context)      # 拼接 system prompt
    │   └── _call_anthropic / _call_openai
    │
    ├── [工具调用: generate_forge_query]
    │   └── compile_query(forge_json)
    │       ├── jsonschema.validate()   # Schema 校验
    │       └── _compile()             # 生成 SQL
    │
    └── AgentResponse(action="sql_review", sql=sql)
            │
            ▼
    展示给用户（飞书卡片 / Web 界面）
            │
   用户确认 ▼  用户取消
    agent.approve()   agent.cancel()
```

### 指标定义流

```
用户描述业务指标
    │
    ▼
agent.process() → llm.call() → define_metric 工具
    │
    ▼
_validate_and_save(metric, name)
    ├── 读取 schema.registry.json（字段引用校验依据）
    ├── 读取 metrics.registry.yaml（衍生指标引用检查依据）
    ├── validate_metric() → ValidationResult
    ├── 校验失败 → 返回 errors，不写文件
    └── 校验通过 → 写入 metrics.registry.yaml
```

---

## 6. Forge DSL 规范

### 6.1 顶层字段

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `scan` | string | ✅ | 主扫描表（FROM 子句） |
| `select` | string[] | ✅ | 输出列，至少一项；可引用 agg/window 别名 |
| `joins` | Join[] | — | JOIN 定义列表，顺序保留 |
| `filter` | Condition[] | — | 行级过滤（WHERE），条目间 AND |
| `group` | string[] | — | GROUP BY 键列表 |
| `agg` | Aggregation[] | — | 聚合表达式列表 |
| `having` | Condition[] | — | 聚合后过滤（HAVING），条目间 AND |
| `window` | WindowExpr[] | — | 窗口函数列表，别名在 select 中引用 |
| `sort` | SortKey[] | — | 排序键列表 |
| `limit` | integer ≥ 1 | — | 结果行数上限 |

### 6.2 JOIN 类型

| type | 编译结果 | 适用场景 |
|---|---|---|
| `inner` | `INNER JOIN` | 取两表交集 |
| `left` | `LEFT JOIN` | 保留左表所有行 |
| `right` | `RIGHT JOIN` | 保留右表所有行 |
| `full` | `FULL OUTER JOIN` | 保留两表所有行 |
| `anti` | `LEFT JOIN … WHERE right.key IS NULL` | 不存在于右表（NOT IN 的安全替代） |
| `semi` | `WHERE EXISTS (SELECT 1 FROM … WHERE …)` | 存在于右表（不拉取右表列） |

### 6.3 过滤条件（filter / having）

**简单条件（SimpleCondition）：**

```json
{"col": "orders.status", "op": "eq", "val": "completed"}
```

| op | SQL | 备注 |
|---|---|---|
| `eq` | `col = val` | |
| `neq` | `col != val` | |
| `gt` / `gte` | `col > val` / `col >= val` | |
| `lt` / `lte` | `col < val` / `col <= val` | |
| `in` | `col IN (v1, v2, ...)` | val 为数组 |
| `like` | `col LIKE val` | val 含通配符 % |
| `is_null` | `col IS NULL` | 无 val |
| `is_not_null` | `col IS NOT NULL` | 无 val |
| `between` | `col BETWEEN lo AND hi` | 用 lo/hi 代替 val |

**OR 组（OrCondition）：**

```json
{"or": [
  {"col": "orders.status", "op": "eq", "val": "pending"},
  {"col": "orders.status", "op": "eq", "val": "completed"}
]}
```

OR 组只能出现在 `filter` / `having` 的顶层，内部条件为简单条件，编译为 `(cond1 OR cond2)`。

### 6.4 聚合函数（agg）

| fn | 编译结果 | 是否需要 col |
|---|---|---|
| `count_all` | `COUNT(*)` | ❌（additionalProperties: false，禁止填 col） |
| `count` | `COUNT(col)` | ✅ |
| `count_distinct` | `COUNT(DISTINCT col)` | ✅ |
| `sum` | `SUM(col)` | ✅ |
| `avg` | `AVG(col)` | ✅ |
| `min` | `MIN(col)` | ✅ |
| `max` | `MAX(col)` | ✅ |

所有聚合函数必须声明 `as`（别名），在 `select` 中通过别名引用。

### 6.5 窗口函数（window）

三种变体，通过 Schema `oneOf` 区分：

**排名类（WindowRanking）：**

```json
{
  "fn": "row_number",
  "partition": ["orders.user_id"],
  "order": [{"col": "orders.created_at", "dir": "desc"}],
  "as": "rn"
}
```

支持 `row_number` / `rank` / `dense_rank`，无 `col` 字段。

**聚合类（WindowAgg）：**

```json
{
  "fn": "sum",
  "col": "orders.total_amount",
  "partition": ["orders.user_id"],
  "as": "user_total"
}
```

支持 `sum` / `avg` / `count` / `min` / `max`，必须有 `col`。

**导航类（WindowNav）：**

```json
{
  "fn": "lag",
  "col": "orders.total_amount",
  "offset": 1,
  "default": 0,
  "order": [{"col": "orders.created_at", "dir": "asc"}],
  "as": "prev_amount"
}
```

支持 `lag` / `lead`，`offset` 默认 1，`default` 为缺少行时的填充值，`order` 至少一项。

**编译规则：**
- `partition` 存在 → `PARTITION BY col1, col2`
- `order` 存在 → `ORDER BY col dir`
- 两者均缺 → `OVER ()` （全局窗口，适用于全局排名）

### 6.6 值类型

| Python / JSON 类型 | SQL 字面量 | 示例 |
|---|---|---|
| `{"$date": "YYYY-MM-DD"}` | `'YYYY-MM-DD'` | `{"$date": "2024-01-01"}` → `'2024-01-01'` |
| `true` / `false` | `TRUE` / `FALSE` | |
| `"string"` | `'string'`（单引号转义） | `"it's"` → `'it''s'` |
| `42` / `3.14` | `42` / `3.14` | |
| `[1, 2, 3]`（in 专用） | `1, 2, 3` | IN 列表 |

---

## 7. 注册表规范

### 7.1 结构层（schema.registry.json）

**由 `forge sync` 自动生成，不要手动修改。**

```json
{
  "tables": {
    "users": {
      "columns": ["id", "name", "city", "is_vip"]
    },
    "orders": {
      "columns": ["id", "user_id", "status", "total_amount", "created_at"]
    }
  }
}
```

每次 `forge sync` 完整覆盖，不做增量合并。

### 7.2 语义层（metrics.registry.yaml）

**人工维护或通过管理后台 / AI 添加。**

#### 原子指标（atomic）

```yaml
order_amount:
  metric_class: atomic
  label: 订单金额
  description: 已完成订单的成交金额（只统计 status=completed 的订单）
  measure: orders.total_amount       # 必填：table.column 格式
  aggregation: sum                   # 必填：sum|count|count_distinct|avg|min|max
  qualifiers:                        # 永远应用的业务过滤条件
    - "orders.status = 'completed'"
  period_col: orders.created_at      # 时间窗口过滤作用于此字段
  dimensions:                        # 支持的分析维度（用于 GROUP BY）
    - users.city
    - users.is_vip
  updated_at: "2024-01-15"          # 自动注入，勿手动修改
```

#### 衍生指标（derivative）

```yaml
repurchase_rate:
  metric_class: derivative
  label: 复购率
  description: 有重复购买行为的用户占有过下单记录的用户的比例
  numerator: repurchase_users        # 必填：已注册的原子指标名
  denominator: ordered_users         # 必填：已注册的原子指标名
  period_col: orders.created_at      # 统一应用于分子和分母的时间字段
  notes: |                           # qualifier 不一致时必填
    分子和分母均不限定 status
    时间窗口通过 period_col 统一注入
  updated_at: "2024-01-15"
```

**约束规则：**
- `numerator` / `denominator` 只能引用原子指标，不能链式引用衍生指标
- qualifier 不一致时，必须在 `notes` 中说明原因（否则触发警告）
- `period_col` 在衍生指标层声明时，覆盖两个原子指标各自的 `period_col`

---

## 8. 指标校验规则

### 硬错误（阻止保存）

| 规则 | 触发条件 |
|---|---|
| label 缺失 | label 为空字符串 |
| description 缺失 | description 为空字符串 |
| 无效 metric_class | 不是 "atomic" 或 "derivative" |
| measure 缺失（atomic） | measure 为空 |
| measure 格式错误（atomic） | 不符合 table.column 正则 |
| measure 字段不存在（atomic） | 不在 schema.registry.json 中 |
| 无效 aggregation（atomic） | 不在合法聚合函数枚举中 |
| qualifiers 引用不存在的字段 | 条件字符串中的 table.col 不在注册表 |
| dimensions 引用不存在的字段 | 含 . 的维度项不在注册表 |
| period_col 不存在（atomic） | 字段不在注册表 |
| numerator/denominator 缺失（derivative） | 为空字符串 |
| 自引用（derivative） | numerator 或 denominator 等于自身名称 |
| 引用不存在的指标（derivative） | numerator/denominator 在 all_metrics 中不存在 |
| 引用衍生指标（derivative） | numerator/denominator 的 metric_class 是 "derivative" |

### 软警告（保存继续）

| 规则 | 触发条件 | 抑制方式 |
|---|---|---|
| sum/avg 无 qualifiers | aggregation 是 sum/avg 但 qualifiers 为空 | 添加 qualifiers |
| 无 dimensions | dimensions 为空 | 添加 dimensions |
| 跨表粒度差异 | 分子分母 measure 来自不同表 | 业务确认后无需处理（警告仅提示） |
| qualifier 不一致 | 分子分母 qualifiers 不同 | 填写 notes 字段 |
| period_col 不一致 | 分子分母 period_col 不同且衍生指标无 period_col | 在衍生指标上显式声明 period_col |

---

## 9. 测试说明

测试套件共 **146 个测试用例**，使用 pytest，无需真实 LLM API Key。

### 测试文件职责

| 文件 | 用例数 | 测试内容 |
|---|---|---|
| `test_compiler.py` | 18 | 编译器核心：JOIN、聚合、过滤、排序、Schema 校验 |
| `test_compiler_extended.py` | 26 | 扩展用例：全 JOIN 类型、所有聚合函数、边界条件 |
| `test_compiler_window.py` | 21 | 窗口函数：三种变体、参数组合、Schema 校验 |
| `test_metric_validator.py` | 30 | 指标校验：原子/衍生指标的正/反用例 |
| `test_session.py` | 17 | 会话管理：截断、隔离、pending 状态 |
| `test_agent_logic.py` | 14 | Agent 调度：approve/cancel、重试、mock LLM |
| `test_registry_context.py` | 24 | 注册表上下文格式化：各字段输出、段落顺序 |
| `test_sync.py` | 3 | 数据库同步：SQLite 内存库 |

### 运行命令

```bash
# 全部测试
pytest

# 带详细输出
pytest -v

# 只运行编译器相关
pytest tests/test_compiler*.py -v

# 只运行失败的测试
pytest --lf

# 覆盖率报告（需要 pytest-cov）
pytest --cov=forge --cov=agent --cov=registry --cov-report=term-missing
```

### Mock 策略

`test_agent_logic.py` 使用 `unittest.mock.patch` 隔离外部依赖：

```python
# mock LLM 调用（避免真实 API 请求）
with patch.object(agent_mod.llm, "call", return_value={...}):
    resp = agent_mod.process("user", "query")

# mock 配置路径（使用 tmp_path 隔离文件系统）
with patch.object(config.cfg, "REGISTRY_PATH", schema_path), \
     patch.object(config.cfg, "METRICS_PATH",  metrics_path):
    errors, _ = agent_mod._validate_and_save(metric, "name")
```

---

## 10. 扩展指南

### 添加新的过滤运算符

1. 在 `forge/schema.json` 的 `SimpleCondition.op.enum` 中添加新值
2. 在 `forge/compiler.py` 的 `_condition()` 函数中添加对应的 if 分支
3. 在 `tests/test_compiler.py` 或 `test_compiler_extended.py` 中添加测试用例

### 添加新的聚合函数

1. 在 `forge/schema.json` 的 `AggWithCol.fn.enum` 中添加新值
2. 在 `forge/compiler.py` 的 `_agg_expr()` 函数中添加处理逻辑
3. 同步更新 `agent/llm.py` 中 `define_metric` 工具的 `aggregation.enum`
4. 同步更新 `registry/validator.py` 中的 `VALID_AGGREGATIONS` 集合

### 添加新的窗口函数

1. 判断新函数属于哪个变体（WindowRanking / WindowAgg / WindowNav）
2. 在 `forge/schema.json` 对应变体的 `fn.enum` 中添加新值
3. 在 `forge/compiler.py` 的 `_window_expr()` 中添加 if 分支（若有特殊参数处理）

### 接入新的 LLM 提供商

1. 在 `config.py` 中为新提供商添加必要的配置项
2. 在 `agent/llm.py` 中添加 `_call_<provider>()` 函数（参考 `_call_openai`）
3. 在 `call()` 函数中添加 `cfg.LLM_PROVIDER == "<provider>"` 分支

### 切换到持久化 Session 存储

当前 `SessionStore` 是内存实现。若需多进程部署，替换为 Redis：

```python
# agent/session.py — 替换 SessionStore 实现

import redis
import pickle

class SessionStore:
    def __init__(self, redis_url: str):
        self._redis = redis.from_url(redis_url)

    def get(self, user_id: str) -> Session:
        data = self._redis.get(f"session:{user_id}")
        if data is None:
            return Session(user_id=user_id)
        return pickle.loads(data)

    def save(self, session: Session) -> None:
        self._redis.setex(
            f"session:{session.user_id}",
            86400,  # TTL: 24 小时
            pickle.dumps(session),
        )

    def clear(self, user_id: str) -> None:
        self._redis.delete(f"session:{user_id}")
```

### 支持 SQL 方言

当前编译器输出方言中性的 SQL（ANSI 标准）。若需支持特定方言差异（如 MySQL 的 `LIMIT x OFFSET y`、BigQuery 的日期函数），在 `_compile()` 函数末尾增加方言后处理层：

```python
def compile_query(forge: dict, dialect: str = "ansi") -> str:
    jsonschema.validate(forge, _SCHEMA)
    sql = _compile(forge)
    return _apply_dialect(sql, dialect)

def _apply_dialect(sql: str, dialect: str) -> str:
    if dialect == "bigquery":
        # 替换 DATE 字面量格式等
        sql = sql.replace("'2024-01-01'", "DATE '2024-01-01'")
    return sql
```
