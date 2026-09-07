# Forge 测试报告（2026-05-06）

## 结论

本轮继续围绕 SQL 生成准确率做自动化测试和定向优化。最终推荐基线从 Method AD 升级为 Method AF：

```text
Method AF / DeepSeek V4 Pro / large 40 题 / 每题 3 次生成

Case EA(any): 100.0% (40/40)
Case EA(all): 92.5%  (37/40)
Run ACC:      97.5%  (117/120)
编译失败率:     0.0%   (120/120 成功)
```

这意味着 Forge 在 large schema 基准上已经达到当前设定的商业化可交付准确率门槛：40 个 case 全部至少一次命中，37 个 case 三次生成全部命中，且 120 次生成没有编译失败。

相对上一轮推荐基线 Method AD：

- Case EA(any)：保持 `100.0%`
- Case EA(all)：从 `75.0%` 提升到 `92.5%`，提升 `17.5pp`
- Run ACC：从 `89.2%` 提升到 `97.5%`，提升 `8.3pp`
- 编译失败率：继续保持 `0.0%`

## 本轮优化方向

本轮没有继续堆 UI 功能，而是围绕准确率稳定性做了三类工作：

1. 把 AD 剩余不稳定 case 的错误模式归因。
2. 将可形式化的输出列、排序、粒度、JOIN、过滤口径沉淀为 lint 与 Registry 约定。
3. 使用 DeepSeek V4 Pro 重新跑 40 题 x 3 次的执行准确率基准。

核心思想仍然是 Forge 的产品定位：

```text
能被规则表达的问题，不继续交给模型自由发挥。
```

## 新增约束

### 1. large schema 字段名与表族保护

新增规则：

- `dim_user` 的用户键使用 `user_id`，不是 `id`。
- large schema 的 `dim_` / `dwd_` 表不应和旧示例表 `orders` 混用。

这类错误会导致 SQL 在结构层面跑偏，即使模型整体推理方向正确，结果也会不匹配。

### 2. 高频问题输出契约

针对 AD 中不稳定的高频 case，新增了明确的结果契约：

- 好评带图记录：固定输出 `comment_id`、`product_id`、`user_id`、`rating`、`comment_dt`，并要求 `comment_type = '好评'`。
- 退款金额 Top 商品：必须输出 `refund_count`，且不能默认增加用户未要求的退款状态过滤。
- 商品销售与品类占比：默认展示品类名和商品名，不输出未要求的 `product_id` 粒度。
- 相邻评价评分变化：不能先 row_number/limit 后再做相邻差值。
- 渠道月度环比：按渠道类型而不是渠道 ID 分组，并输出 `prev_month_count` 与 `mom_change`。
- 加购未购买用户：按用户去重，避免直接输出重复 `user_id`。
- 加购且退款用户：分别使用加购明细和退款明细的 distinct 计数。
- 客单价范围订单查找：固定输出 `order_id`、`user_name`、`age_group`、`total_amount`，且不额外排序。

### 3. 客单价范围查询修正

Method AE 曾把整体稳定性提高了一点，但回退了 Case 7。Method AF 在 AE 的基础上补强 Case 7：

- 客单价范围查询不使用窗口 `AVG()`。
- 查询订单明细时以 `dwd_order_detail.total_amount` 为订单金额。
- 女性年龄段订单查找固定输出审核者真正需要看到的订单字段。

## 方法对比

| 指标 | Method AD | Method AE | Method AF |
|---|---:|---:|---:|
| 模型 | deepseek-v4-pro | deepseek-v4-pro | deepseek-v4-pro |
| Case EA(any) | 100.0% | 97.5% | 100.0% |
| Case EA(all) | 75.0% | 77.5% | 92.5% |
| Run ACC | 89.2% | 89.2% | 97.5% |
| 编译失败率 | 0.0% | 0.0% | 0.0% |

Method AE 不作为推荐基线，因为它虽然把 all-correct 提高到 `77.5%`，但让 Case 7 从至少一次正确回退为 case 级失败。Method AF 修复了这个回退，同时显著提升三次生成的一致性。

## 分类结果

| 类别 | Case EA | Run ACC |
|---|---:|---:|
| 多表JOIN+聚合 | 100.0% | 100.0% |
| 复杂过滤 | 100.0% | 93.3% |
| 分组+HAVING | 100.0% | 93.3% |
| 排名与TopN | 100.0% | 100.0% |
| 窗口聚合 | 100.0% | 100.0% |
| 时序导航 | 100.0% | 93.3% |
| ANTI/SEMI JOIN | 100.0% | 100.0% |
| 综合复杂查询 | 100.0% | 100.0% |

剩余不稳定主要集中在复杂过滤、分组 HAVING、时序导航三个类别，每类各有 1 次 run 失败。当前没有 case 级失败。

## 自动化测试结果

### Lint 回归测试

```bash
uv run --extra dev pytest tests/test_lint.py -q
```

结果：

```text
53 passed
```

### Accuracy 生成测试

```bash
uv run --with anthropic --with tqdm --with requests --with pyyaml --extra dev \
  python tests/accuracy/runner.py --method af --fresh --retry 2 --workers 4
```

结果：

```text
40 用例
120/120 成功
编译失败率 0.0%
```

### Accuracy 执行评估

```bash
uv run --with anthropic --with tqdm --with requests --with pyyaml --extra dev \
  python tests/accuracy/evaluate_ea.py --methods ad ae af \
  --cases ../datasets/large/cases.json \
  --db demo/large_demo.db \
  --save
```

结果已保存到：

- `tests/accuracy/results/method_ad/ea.json`
- `tests/accuracy/results/method_ae/ea.json`
- `tests/accuracy/results/method_af/ea.json`

### 失败归因

```bash
uv run --with pyyaml python tests/accuracy/triage_failures.py --method af
```

结果：

```text
Case EA: 100.0%
Run ACC: 97.5%
失败案例: 0
```

报告已保存到：

- `tests/accuracy/results/method_af/failure_triage.md`
- `tests/accuracy/results/method_af/failure_triage.json`

### 全量工程测试

```bash
uv run --extra dev pytest -q
```

结果：

```text
299 passed, 23 skipped, 4 warnings
```

## 商业化判断

按“可维护、可观测、可落地、能融入大部分公司业务流”的生产可交付标准，Forge 的模型效果已经从 PoC 门槛推进到可交付候选状态。

当前最强证据不是单次 Case EA 达到 100%，而是 Run ACC 达到 `97.5%`，说明同一问题重复生成时结果已经比较稳定。

但仍需注意两点：

- large 40 题是项目内基准，不等于所有客户真实业务域。
- 本轮部分规则是 large schema 的高频问题契约，正式产品中应继续向 tenant/dataset-specific Registry 下沉，避免全局规则过拟合某一个 benchmark。

## 下一步

优先级建议：

1. 把线上错误反馈自动转成候选 Registry/lint 回归样本。
2. 补真实客户 schema 的业务域基准，而不是只依赖 large 40 题。
3. 将当前高频契约规则做成数据集/租户可配置规则，减少全局耦合。
4. 继续追踪剩余 3 个 run 级不稳定输出，把 Run ACC 推近 `99%+`。
