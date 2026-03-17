# Registry：构建你自己的语义库

Registry 是 Forge 的知识库，分为**结构层**和**语义层**。结构层自动生成，语义层需要业务人员参与维护。

---

## 一、结构层（自动）

连接你的数据库，一条命令生成：

```bash
forge sync --db postgresql://user:pass@host/db
```

产出 `schema.registry.json`，记录所有表名、列名、低基数列的枚举值：

```json
{
  "tables": {
    "orders": {
      "columns": {
        "id":           {},
        "user_id":      {},
        "status":       {"enum": ["completed", "cancelled", "pending"]},
        "total_amount": {},
        "created_at":   {}
      }
    }
  }
}
```

`status` 的枚举值让 LLM 知道正确的拼写（不会幻觉出 `"finished"`），这是 Forge 消灭字段值幻觉的关键手段。

**何时重跑**：数据库加表/改列后运行 `forge sync`。

---

## 二、语义层（三个文件，业务人员维护）

语义层由三个 YAML 文件组成，各司其职：

### 2.1 指标定义 — `metrics.registry.yaml`

定义你的业务指标，让 LLM 知道「复购率」的精确计算公式。

```yaml
# 原子指标：直接聚合
gmv:
  metric_class: atomic
  label: GMV（成交总额）
  description: 所有已提交订单的总成交金额
  measure: orders.total_amount         # 度量字段
  aggregation: sum                     # 聚合函数
  period_col: orders.created_at        # 时间维度列
  dimensions:                          # 可切分的维度
    - users.city
    - products.category

# 带限定的原子指标：加过滤条件
paid_gmv:
  metric_class: atomic
  label: 支付 GMV
  description: 已完成支付的订单金额
  measure: orders.total_amount
  aggregation: sum
  qualifiers:                          # 限定条件
    - "orders.status = 'completed'"
  period_col: orders.created_at

# 衍生指标：由原子指标计算
repurchase_rate:
  metric_class: derived
  label: 复购率
  description: 下过 2 次及以上订单的用户占比
  formula: repeat_buyers / total_buyers
  components:
    repeat_buyers:
      description: 订单数 >= 2 的用户数
    total_buyers:
      description: 下过至少 1 次订单的用户数
```

**维护方式**：
- 手动编写（初始化时）
- 通过飞书 Bot 对话定义：用户说「复购率是…」→ Forge 提取结构 → 确认后写入

### 2.2 歧义消除 — `disambiguations.registry.yaml`

当用户的提问包含有歧义的词时，自动注入上下文或发起澄清。

```yaml
revenue:
  label: 销售额 / 营收
  triggers: [销售额, 营收, GMV, 收入]    # 命中这些词时触发
  context: |                              # 注入 LLM 的上下文
    "销售额"默认指 orders.total_amount（订单金额），
    而非 order_items.quantity * unit_price（明细行金额）。
  requires_clarification: false           # false=静默注入，true=先问用户
  confirmed_by_users: false               # 用户确认过后自动变 true

repurchase_definition:
  label: 复购定义
  triggers: [复购, 复购率, 回购]
  context: |
    复购率 = 下过 ≥2 次订单的用户 / 下过 ≥1 次订单的用户。
    分母含全部下单用户，不含未下单用户。
  requires_clarification: true            # 首次使用时向用户确认
  confirmed_by_users: false
```

**`requires_clarification: true` 的效果**：用户问「统计复购率」时，Forge 不会直接生成 SQL，而是先回复「复购率的定义是…，是否正确？」，用户确认后才继续。确认记录自动写入 staging 目录，运行 `forge sync-staging` 后合并回此文件。

### 2.3 字段使用约定 — `field_conventions.registry.yaml`

描述某些字段的特殊使用规范，防止 LLM 犯常见错误。

```yaml
order_status_filter:
  label: 订单状态过滤原则
  applies_to: [orders.status]             # 适用的字段
  convention: |
    - 统计"销售额/销量排行" → 不需要过滤 status
    - 统计"消费行为/复购" → 默认加 status = 'completed'
    - 用户明确要求看全部状态时以用户为准
  confirmed_by_users: false

amount_field_meaning:
  label: 金额字段含义区分
  applies_to: [orders.total_amount, order_items.unit_price]
  convention: |
    orders.total_amount = 订单总价（一次下单的合计）
    order_items.unit_price = 单品单价
    order_items.quantity * unit_price = 明细行金额
    三者不可混用：订单维度聚合用 total_amount，商品维度聚合用明细行
```

---

## 三、从零构建你的语义库

### Step 1：同步结构层

```bash
forge sync --db your_database_url
```

### Step 2：创建空的语义层文件

```bash
# 在 registry 目录下创建三个空文件
touch registry/data/metrics.registry.yaml
touch registry/data/disambiguations.registry.yaml
touch registry/data/field_conventions.registry.yaml
```

更新 `forge.yaml` 指向这些文件：

```yaml
registry:
  schema_path:          "registry/data/schema.registry.json"
  metrics_path:         "registry/data/metrics.registry.yaml"
  disambiguations_path: "registry/data/disambiguations.registry.yaml"
  conventions_path:     "registry/data/field_conventions.registry.yaml"
```

### Step 3：定义核心指标

从你的业务中挑出最常查询的 5-10 个指标写入 `metrics.registry.yaml`。模板：

```yaml
指标英文名:
  metric_class: atomic          # atomic（直接聚合）或 derived（公式）
  label: 指标中文名
  description: 一句话说明计算口径
  measure: 表名.列名             # 度量字段
  aggregation: sum              # sum / count / count_distinct / avg / min / max
  qualifiers:                   # 可选：过滤条件
    - "表名.列名 = '值'"
  period_col: 表名.时间列        # 可选：时间维度
  dimensions:                   # 可选：可切分的维度列
    - 表名.维度列
```

### Step 4：添加歧义消除规则

回顾团队常问的问题，找出那些「每次都需要确认含义」的词：

```yaml
规则英文名:
  label: 中文标签
  triggers: [触发词1, 触发词2]
  context: |
    一段话解释这个词在你们业务中的准确含义
  requires_clarification: false   # true=先问用户确认
  confirmed_by_users: false
```

### Step 5：让反馈机制自动丰富语义库

开启反馈机制（`forge config feedback.enabled true`）后，用户的每次歧义澄清都会自动记录到 `.forge/staging/` 目录。定期运行：

```bash
forge sync-staging
```

就会将这些用户确认过的规则合并回 `disambiguations.registry.yaml`，标记 `confirmed_by_users: true`。

**这是一个正向飞轮**：用得越多 → 语义库越准确 → 需要澄清的次数越少。

---

## 四、目录结构参考

```
registry/data/                          ← 生产环境推荐路径
├── schema.registry.json                ← forge sync 自动生成（结构层）
├── metrics.registry.yaml               ← 指标定义（语义层）
├── disambiguations.registry.yaml       ← 歧义消除规则（语义层）
└── field_conventions.registry.yaml     ← 字段使用约定（语义层）

.forge/staging/                         ← 用户反馈暂存区
└── *.json                              ← forge sync-staging 消费后删除
```

## 五、配置路径

通过 `forge.yaml` 或环境变量指定 Registry 文件位置：

```yaml
# forge.yaml
registry:
  schema_path:          "registry/data/schema.registry.json"
  metrics_path:         "registry/data/metrics.registry.yaml"
  disambiguations_path: "registry/data/disambiguations.registry.yaml"
  conventions_path:     "registry/data/field_conventions.registry.yaml"
```

或使用 CLI：

```bash
forge config registry.schema_path "path/to/schema.registry.json"
```

## 六、版本控制建议

- `schema.registry.json` — 提交到 git，团队共享数据结构契约
- `metrics.registry.yaml` — 提交到 git，指标定义需 review
- `disambiguations.registry.yaml` — 提交到 git，消歧规则需 review
- `field_conventions.registry.yaml` — 提交到 git
- `.forge/staging/*.json` — 不提交（已在 .gitignore）
