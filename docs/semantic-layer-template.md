# 语义层内容模板

Forge 的语义层由三个 YAML 文件组成，分别解决不同层面的问题：

| 文件 | 解决的问题 | 举例 |
|------|-----------|------|
| `metrics.registry.yaml` | "这个指标怎么算？" | 复购率 = 下过≥2单用户数 / 下过≥1单用户数 |
| `disambiguations.registry.yaml` | "这个词指什么？" | "销售额"是订单总价还是商品明细成交额？ |
| `field_conventions.registry.yaml` | "这个字段怎么用？" | 客单价在X-Y之间是 WHERE 过滤，不是 AVG+HAVING |

三个文件放在 Registry 目录下（如 `registry/data/`），`forge sync` 会自动加载。

---

## 1. metrics.registry.yaml — 指标定义

指标分两类：**原子指标**（直接聚合）和**衍生指标**（指标之间的比率）。

### 原子指标模板

```yaml
# ── 原子指标：直接对某张表的某个字段做聚合 ──

your_metric_name:                              # 指标 ID（英文，唯一）
  metric_class: atomic                         # 固定值
  label: 中文名称                               # 用户看到的名字
  description: 一句话说清楚这个指标算的是什么        # 注入 LLM 提示
  measure: schema.table.column                 # 聚合的字段（table.column 格式）
  aggregation: sum | count_distinct | avg | max | min   # 聚合方式
  qualifiers:                                  # 可选：前置过滤条件（SQL WHERE 片段）
    - "table.status = '已完成'"
    - "table.amount > 0"
  period_col: table.date_column                # 时间基准列（按日/月/年统计用）
  dimensions:                                  # 可选：常见分析维度
    - dim_table.dimension_name
    - dim_table2.dimension_name
```

### 衍生指标模板

```yaml
# ── 衍生指标：两个指标之间的比率 ──

your_rate_name:
  metric_class: derivative                     # 固定值
  label: 中文名称
  description: 一句话说清楚分子分母分别是什么
  numerator: metric_id_or_expression           # 分子（引用原子指标 ID 或 SQL 表达式）
  denominator: metric_id_or_expression         # 分母
  period_col: table.date_column
  dimensions:
    - dim_table.dimension_name
  notes: |                                     # 可选：补充说明，注入 LLM 提示
    在这里写需要 LLM 特别注意的事项，比如：
    - 分子分母的口径差异
    - 容易混淆的概念（退款率 ≠ 退货率）
    - JOIN 路径提示
    - ⚠️ 某张表上没有某个字段（防止幻觉）
```

### 实际示例

```yaml
# ── 交易域 ──

gmv:
  metric_class: atomic
  label: GMV（成交总额）
  description: 所有已提交订单的总成交金额，含已取消订单，口径为下单金额
  measure: dwd_order_detail.total_amount
  aggregation: sum
  period_col: dwd_order_detail.order_dt
  dimensions:
    - dim_channel.channel_name
    - dim_category.category_name

completed_order_count:
  metric_class: atomic
  label: 完成订单量
  description: 状态为"已完成"的订单数量，为最严口径
  measure: dwd_order_detail.order_id
  aggregation: count_distinct
  qualifiers:
    - "dwd_order_detail.order_status = '已完成'"
  period_col: dwd_order_detail.complete_dt

# ── 衍生指标 ──

repurchase_rate:
  metric_class: derivative
  label: 复购率
  description: 统计周期内下过≥2笔支付订单的用户占全部购买用户的比例
  numerator: repurchase_user_count
  denominator: buying_user_count
  period_col: dwd_order_detail.pay_dt
  notes: |
    分子：支付订单 ≥2 笔的用户数
    分母：支付订单 ≥1 笔的用户数
    两侧均限定相同时间窗口

refund_rate:
  metric_class: derivative
  label: 退款率
  description: 有退款记录的订单数除以总订单数
  numerator: COUNT(DISTINCT dwd_refund_detail.order_id)
  denominator: COUNT(DISTINCT dwd_order_detail.order_id)
  notes: |
    退款率 ≠ 退货率。退款率基于 dwd_refund_detail，退货率基于 dwd_return_goods_detail。
    JOIN 路径：dwd_order_detail LEFT JOIN dwd_refund_detail ON order_id
    ⚠️ dwd_order_detail 表上没有 refund_id 字段，不要尝试访问
```

---

## 2. disambiguations.registry.yaml — 歧义消除规则

当用户提到模糊词时，自动向 LLM 注入澄清上下文，或先向用户发起一轮确认。

### 模板

```yaml
rule_id:                                       # 规则 ID（英文，唯一）
  label: 中文名称                               # 简短标签
  triggers:                                    # 触发词列表（用户问题中出现任一词即触发）
    - 触发词1
    - 触发词2
    - 同义词3
  context: |                                   # 注入 LLM 的澄清上下文
    在这里写清楚这个词的准确含义、计算口径、容易犯的错误。
    LLM 会在生成 SQL 前看到这段话。
  requires_clarification: false                # false = 直接注入；true = 先问用户
  clarification_question: |                    # 仅 requires_clarification: true 时需要
    给用户看的选择题，让用户明确到底要哪种口径。
  confirmed_by_users: false                    # true = 已有用户验证过这条规则
```

### 实际示例

```yaml
# ── 直接注入型（大多数场景）──

revenue:
  label: 销售额 / 营收
  triggers: [销售额, 营收, GMV, 收入, 流水]
  context: |
    "销售额"默认指订单维度金额（dwd_order_detail.total_amount），
    而非商品明细成交额。若需按商品品类汇总，
    应使用 dwd_order_item_detail.actual_amount。
  requires_clarification: false
  confirmed_by_users: false

repurchase_definition:
  label: 复购定义
  triggers: [复购, 复购率, 回购, 二次购买]
  context: |
    复购率 = 下过 2 次及以上订单的用户数 / 下过至少 1 次订单的用户数。
    分母包含所有下单用户，分子仅包含有重复购买记录的用户。
  requires_clarification: false
  confirmed_by_users: false

# ── 需要用户确认型（口径有根本歧义）──

revenue_granularity:
  label: 销售额口径
  triggers: [按品类统计, 品类销售额]
  context: |
    "按品类统计销售额"有两种口径：
    A. 订单口径：含该品类商品的订单总金额（可能含其他品类的钱）
    B. 明细口径：该品类商品的 actual_amount 之和（精确到品类）
  clarification_question: |
    您需要的"品类销售额"是指：
    A. 含该品类商品的订单总金额（快速、略有误差）
    B. 该品类商品实际成交金额（精确，需关联明细表）
    请回复 A 或 B。
  requires_clarification: true
  confirmed_by_users: false
```

---

## 3. field_conventions.registry.yaml — 字段使用约定

描述字段在业务上的正确使用方式，防止模型犯"语法对但语义错"的错误。

### 模板

```yaml
convention_id:                                 # 约定 ID（英文，唯一）
  label: 中文名称
  applies_to:                                  # 约定适用的字段或表（table.column 格式）
    - table.column
    - table2.column2
  convention: |                                # 具体使用规范，注入 LLM 系统提示
    用清晰的条件-动作格式写规则：
    - 情况 A → 做法 A
    - 情况 B → 做法 B
    - 默认 → 做法 C
    越具体越好。给出正确写法示例。
  confirmed_by_users: false                    # true = 已有用户场景验证
```

### 实际示例

```yaml
order_status_filter:
  label: 订单状态过滤原则
  applies_to: [dwd_order_detail.order_status]
  convention: |
    - 纯维度统计（"各品类总销售额"）→ 不需要加 order_status 过滤
    - 用户行为分析（"消费排名"、"复购"）→ 加 order_status = '已完成'
    - 用户明确要求看全部状态 → 以用户要求为准
  confirmed_by_users: true

unit_price_calculation:
  label: 客单价计算粒度
  applies_to: [dwd_order_detail.total_amount, dwd_order_detail.pay_amount]
  convention: |
    "客单价"= 单笔订单的平均实付金额，即 AVG(pay_amount)。
    "客单价在 X 到 Y 之间"是对单笔订单金额的 WHERE 过滤，
    不是对用户平均消费的 HAVING 过滤。
  confirmed_by_users: false

refund_rate_join:
  label: 退款率的计算方式
  applies_to: [dwd_refund_detail.order_id, dwd_order_detail.order_id]
  convention: |
    退款率 = 有退款记录的订单数 / 总订单数。
    1. 从 dwd_order_detail 出发（分母）
    2. LEFT JOIN dwd_refund_detail ON order_id
    3. 分子用 COUNT(DISTINCT dwd_refund_detail.order_id)
    ⚠️ dwd_order_detail 上【没有】refund_id 字段
    ⚠️ 退款率 ≠ 退货率（退货率来自 dwd_return_goods_detail）
  confirmed_by_users: true
```

---

## 快速起步清单

刚接入一个新数据库时，按优先级填写：

1. **metrics** — 先定义 5-10 个核心指标（GMV、订单量、客单价、转化率、退款率），覆盖 80% 的日常查询
2. **disambiguations** — 梳理 3-5 个最常混淆的业务术语（销售额口径、复购定义、时间范围）
3. **conventions** — 记录 2-3 条团队内部已知的"坑"（状态过滤规则、JOIN 路径、字段命名陷阱）

不需要一次写完。**用得越多越准确**——每次模型犯错，就是一次补充语义层的机会。
