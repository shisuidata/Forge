---
title: Registry 语义层
description: Registry 的两层设计（结构层 + 语义层）与从零构建指南
---

Registry 是 Forge 的知识库，分为**结构层**和**语义层**。结构层自动生成，语义层需要业务人员参与维护。

## 结构层（自动）

连接数据库，一条命令生成：

```bash
forge sync --db postgresql://user:pass@host/db
```

产出 `schema.registry.json`，记录所有表名、列名、低基数列的枚举值：

```json
{
  "tables": {
    "orders": {
      "columns": {
        "status": {"enum": ["completed", "cancelled", "pending"]},
        "total_amount": {},
        "created_at": {}
      }
    }
  }
}
```

`status` 的枚举值让 LLM 知道正确的拼写（不会幻觉出 `"finished"`）。数据库加表/改列后重跑 `forge sync` 即可。

---

## 语义层（三个文件）

### 指标定义 — `metrics.registry.yaml`

```yaml
# 原子指标：直接聚合
gmv:
  metric_class: atomic
  label: GMV（成交总额）
  measure: orders.total_amount
  aggregation: sum
  period_col: orders.created_at

# 衍生指标：由原子指标计算
repurchase_rate:
  metric_class: derived
  label: 复购率
  formula: repeat_buyers / total_buyers
  components:
    repeat_buyers:
      description: 订单数 >= 2 的用户数
    total_buyers:
      description: 下过至少 1 次订单的用户数
```

### 歧义消除 — `disambiguations.registry.yaml`

```yaml
repurchase_definition:
  label: 复购定义
  triggers: [复购, 复购率, 回购]
  context: |
    复购率 = 下过 >= 2 次订单的用户 / 下过 >= 1 次订单的用户。
    分母含全部下单用户，不含未下单用户。
  requires_clarification: true   # 首次使用时向用户确认
```

当 `requires_clarification: true` 时，Forge 不会直接生成 SQL，而是先确认定义是否正确。

### 字段使用约定 — `field_conventions.registry.yaml`

```yaml
order_status_filter:
  label: 订单状态过滤原则
  applies_to: [orders.status]
  convention: |
    - 统计"销售额/销量排行" → 不需要过滤 status
    - 统计"消费行为/复购" → 默认加 status = 'completed'
```

---

## 从零构建

### Step 1：同步结构层

```bash
forge sync --db your_database_url
```

### Step 2：创建空的语义层文件

```bash
touch registry/data/metrics.registry.yaml
touch registry/data/disambiguations.registry.yaml
touch registry/data/field_conventions.registry.yaml
```

在 `forge.yaml` 中配置路径：

```yaml
registry:
  schema_path:          "registry/data/schema.registry.json"
  metrics_path:         "registry/data/metrics.registry.yaml"
  disambiguations_path: "registry/data/disambiguations.registry.yaml"
  conventions_path:     "registry/data/field_conventions.registry.yaml"
```

### Step 3：定义核心指标

从业务中挑出最常查询的 5-10 个指标写入 `metrics.registry.yaml`。

### Step 4：添加歧义消除规则

回顾团队常问的问题，找出「每次都需要确认含义」的词。

### Step 5：让反馈机制自动丰富语义库

```bash
forge sync-staging
```

用户的每次歧义澄清会记录到 `.forge/staging/`，运行上述命令合并回语义层文件。

**正向飞轮**：用得越多 -> 语义库越准确 -> 需要澄清的次数越少。

---

## 目录结构

```
registry/data/
├── schema.registry.json            ← forge sync 自动生成（结构层）
├── metrics.registry.yaml           ← 指标定义（语义层）
├── disambiguations.registry.yaml   ← 歧义消除规则（语义层）
└── field_conventions.registry.yaml ← 字段使用约定（语义层）

.forge/staging/                     ← 用户反馈暂存区
└── *.json
```

所有 registry 文件建议提交到 git，团队共享数据结构和业务语义契约。

---

> 本页为精简版。完整内容参见 [docs/registry.md](https://github.com/shisuidata/Forge/blob/main/docs/registry.md)。
