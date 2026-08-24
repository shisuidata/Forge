---
title: 07｜Schema 检索与上下文工程
summary: 在大 Schema 下控制召回、噪声、权限和 token 预算
---

# Schema 检索与上下文工程

## 1. 全量 Schema 为什么会伤害效果

面对 200 张表，把全部 DDL 塞进 prompt 会带来三种成本：

1. **容量成本**：占用 context window 和 token；
2. **注意力成本**：相似表和历史表干扰选择；
3. **权限风险**：不该看见的表不应进入模型上下文。

Schema RAG 的目标不是“找到答案”，而是先缩小模型可见的数据库地图。

## 2. 表描述是召回上限

Forge 将表名、业务描述、列和枚举值拼成检索文本：

```text
Table: orders. Description: 订单主表.
Columns: id, user_id, status(completed,cancelled), total_amount, created_at
```

如果表名是 `t_001` 且没有描述，Embedding 再好也很难理解“已完成订单”。Schema 治理是 RAG 质量的前提。

## 3. 两级检索

```text
问题
  → ACL 可见表集合
  → 已有向量索引且 embed_fn 可用？
       ├─ 是：cosine similarity
       └─ 否：BM25-lite + 中文 bigram
  → top-k 表
  → 追问所需历史表补充
  → Registry/SMP/WMB 合并
  → LLM
```

### 向量模式

表描述批量 Embedding、L2 归一化并缓存到 `.forge/schema_embeddings.pkl`。查询向量与表向量做余弦相似度排序。表集合变化时索引失效重建。

### BM25-lite 降级

Embedding 不可用时，英文 token、中文原串和字符 bigram 共同参与 TF×IDF。它保证系统可退化运行，但不是向量语义召回的等价替代。

## 4. 四类上下文来源

| 来源 | 回答的问题 |
|---|---|
| Schema Registry | 有哪些表、列和值？ |
| Metrics/Rules | 组织如何定义指标和字段？ |
| SMP | 过去确认过哪些知识和纠错？ |
| EMS recent turns | 当前对话承接了什么？ |

WMB 按 query/define/analyze/admin 场景分配预算。追问时，EMS 中最近使用过的表可作为 `extra_tables` 补回，避免“继续按城市拆分”丢失上一轮数据源。

## 5. top-k 的工程取舍

- 太小：漏表，多表 JOIN 问题无法表达；
- 太大：噪声和 token 上升；
- 小 Schema：`top_k >= 表总数` 时直接全取，避免无意义 Embedding；
- 多轮问题：在检索结果上补充历史表，而不是盲目提高全局 k。

客户 PoC 应用 golden questions 统计 `recall@k`，并按问题类别调参，而不是照搬 Demo 的 k=5。

## 6. ACL 必须先于注入

权限过滤的正确位置是模型看见 Schema 之前。仅在执行时拒绝 SQL，会泄漏表名、字段名和业务结构。当前团队表 ACL 应同时影响 Retriever、动态 Tool Schema 和错误提示。

但表级 ACL 不是完整权限系统：行级、列级、脱敏、结果导出和数据源权限仍需数据库或目标权限层承担。

## 7. 如何评估检索

至少记录：

- golden tables 是否都在 top-k；
- 无关表数量；
- ACL 后是否零泄漏；
- Embedding 故障时 BM25 是否仍可用；
- 索引变更是否正确失效；
- 多轮追问是否保留必要表；
- 召回变化对最终 EA 的影响。

历史文档中的 92.9% 或 100% 召回来自小样本，只能作为当时实验记录，不能对陌生客户 Schema 作普遍承诺。

## 8. 源码与测试

- [`forge/retriever.py`](https://github.com/shisuidata/Forge/blob/main/forge/retriever.py)
- [`agent/llm.py`](https://github.com/shisuidata/Forge/blob/main/agent/llm.py)
- [`tests/test_registry_context.py`](https://github.com/shisuidata/Forge/blob/main/tests/test_registry_context.py)
