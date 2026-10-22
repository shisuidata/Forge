---
title: Registry 语义层
description: Registry 的两层设计（结构层 + 语义层）与从零构建指南
---

Registry 是 Forge 的知识库，分为**结构层**和**语义层**。结构层自动生成，语义层需要业务人员参与维护。它提供上下文与版本化语义，不是执行授权，也不证明业务定义天然正确。

:::caution[数据与使用边界]
先使用[Quickstart](/guides/quickstart/)的隔离合成数据。结构同步会读取 Schema 并可能采样字段值；Registry、样本、反馈和审计都可能含敏感信息，不应未经审查上传到模型或公开 Git。当前安全发布阻断见[验证边界](/reference/benchmarks/)，本页不是接入真实客户数据的安全批准。
:::

## 结构层（自动）

在已获准的隔离测试数据源上同步结构。通过安全配置提供只读连接，命令示意如下，不要把真实密码写进命令历史：

```bash
forge sync --db "$DATABASE_URL"
```

公开源码的 `forge sync` 默认只输出 drift proposal，不修改 Registry；审阅后才可显式使用 `--apply` 应用同步。输出路径以当前配置与参数为准，结构文件包含表列与可选的低基数枚举值。以下为合成示意：

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

`status` 的枚举值提供已知拼写提示，但不会保证所有表达式位置都无法产生错误值。数据库加表/改列后，应重新同步、审阅差异并回归，而不是自动覆盖后立即执行。

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

`requires_clarification: true` 表达需要先确认的规则；实际是否触发取决于上游编排和规则匹配路径，不能把这个字段当作适用于所有 Direct SQL/Agent 接入的强制授权门禁。

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

### Step 5：审阅反馈后再丰富语义库

```bash
forge sync-staging
```

旧对话流程可将歧义澄清暂存在 `.forge/staging/`。合并前应审查候选、来源、作用域与敏感值，合并后检查差异和回归结果，不要将任意用户的一次确认自动提升为组织事实。

经审阅的反馈可能改善语义库；更多使用并不自动保证错误率下降。

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

可将经脱敏、获准共享的 Registry 定义纳入团队的私有版本控制；不要将生产结构、采样枚举、个人信息、凭证或客户业务口径直接提交到公开仓库。

---

> 本页为精简版。完整内容参见 [docs/registry.md](https://github.com/shisuidata/Forge/blob/main/docs/registry.md)。
