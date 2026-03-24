# EA 优化方案（2026-03-19）

**起点**：M2.7 65.0%（26/40），DeepSeek 65.0%（26/40）
**目标**：80%+

---

## 失败的结构性归因

14 个失败不是 14 个独立问题，而是 5 类系统性缺陷：

| 根因类型 | 失败案例 | 共同特征 |
|----------|---------|---------|
| **A. 编译失败无重试** | C34, C38 | Agent 有 3 次重试，Benchmark 单次即死 |
| **B. 输出列规格模糊** | C10, C22, C36 | 行数对但列不同——问题没说要哪些列 |
| **C. 约定未被应用** | C19, C7, C40 | 规则在 prompt 里，模型没遵守 |
| **D. 模式示例缺失** | C26, C28 | 模型不知道怎么用 DSL 表达这个模式 |
| **E. 模型推理极限** | C5, C37, C39 | Forge 边界外，多步推理/复杂逻辑 |

---

## 三层系统改进

### 层 1：编译重试对齐（解决 A 类）

**问题**：Agent 生产环境有 `MAX_RETRIES=2`（3 次机会），编译报错后把错误反馈给模型修正。Benchmark 是单次即死。

**做法**：
- `runner.py` 加 `--retry N` 参数
- 编译失败时把错误信息拼入 messages 重新调用 LLM
- 和 `agent.py` 的重试逻辑对齐
- 保留无 retry 的原始分数作为 baseline

**预期**：+5pp（C34, C38）

### 层 2：生成后约定检查（解决 C 类）

**问题**：15 条约定全塞进 system prompt，依赖模型"自觉"遵守。14K 字 prompt 里模型对细节注意力有限。

**做法**：编译之前加"约定 lint"层——用代码检查 Forge JSON 是否违反关键约定：

```python
def lint_conventions(forge_json: dict, question: str) -> list[str]:
    warnings = []
    # 规则 1: 用户行为查询缺少 order_status 过滤
    if matches_user_behavior(question) and not has_order_status_filter(forge_json):
        warnings.append("此查询涉及用户消费行为分析，建议添加 order_status='已完成' 过滤")
    # 规则 2: SELECT 含 category_id 但缺 category_name
    # 规则 3: 客单价用了 AVG 而不是 WHERE 过滤
    return warnings
```

有 warnings 时拼入 messages 让模型修正（和重试合并）。

**关键认知**：程序检查的覆盖率是 100%，模型注意力不是。约定应该被程序化验证，不应该靠 prompt 措辞。

**预期**：+5-7.5pp（C19, C7, C22）

### 层 3：示例模式补全（解决 D 类）

**问题**：`prompt_examples/window_lag.md` 只有"按月聚合后打 LAG"一种模式。

**补充两种通用模式**：
1. **明细行 LAG**：对原始订单行直接打 LAG（C26 模式）
2. **LAG 求时差**：julianday(dt) - julianday(LAG(dt))（C28 模式）

**预期**：+5pp（C26, C28）

---

## 不做什么

- **E 类不做**：C5/C37/C39 是模型推理硬边界，Forge 是"让弱模型生成可信 SQL"，不是"让弱模型变强模型"
- **评估器不再放宽**：当前 Layer 2 列投影已足够宽容，再放宽会引入 false positive
- **不逐个打补丁**：不为单个 case 改 prompt 措辞

---

## B 类（参考 SQL 微调）作为附带工作

| Case | 修改 | 原因 |
|------|------|------|
| C10 | 参考 SQL 去掉 brand_type SELECT 列 | 问题没要求展示 brand_type，只要求过滤 |
| C40 | 题目已修正（总订单数），需重跑验证 | 参考 SQL 不过滤 status，旧题目说"总完成订单数"误导模型 |

---

## 预期收益路径

| 步骤 | EA | 变化 |
|------|-----|------|
| 当前 baseline | 65.0% | — |
| + 层 1（编译重试） | ~70.0% | +5pp |
| + 层 2（约定 lint） | ~75-77.5% | +5-7.5pp |
| + 层 3（LAG 模式） | ~80.0% | +5pp |
| + B 类微调 | ~82.5% | +2.5pp |

---

## 实施顺序

1. **层 1**：改 runner.py，加编译重试（最小改动，效果立竿见影，方法论上最正当）
2. **B 类**：顺手修参考 SQL（C10, C40）
3. **层 3**：补 window_lag.md 示例（简单，确定性高）
4. **层 2**：设计并实现约定 lint 层（最复杂，但收益最大且可泛化）
