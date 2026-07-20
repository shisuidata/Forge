---
title: 介绍
description: Forge 是什么，解决什么问题
---

## Forge 是什么

Forge 是一个面向数据团队的 AI 查询 Agent，**私有化部署，让弱模型也能生成可信 SQL**。

### 核心主张

> 生成错误和业务逻辑错误应该系统性消灭，而不是靠更好的 prompt 碰运气。

### 解决的问题

| 错误类型 | 定义 | Forge 能解决 |
|---|---|---|
| **生成错误** | 推理正确，但翻译成 SQL 时出错 | ✅ DSL 约束 + Structured Output |
| **业务逻辑错误** | 指标定义歧义（复购率的分母是什么） | ✅ Registry 语义层 |
| **算法逻辑错误** | 模型不知道该用什么算法 | ❌ 超出 Forge 能力边界 |

### 完整工作流

```
用户自然语言
  ↓
Forge Agent（理解意图）
  ↓ Structured Output
Forge JSON       ← 格式由 JSON Schema 强制约束
  ↓ 确定性编译
SQL              ← 人工审核，审核者看到的 = 实际执行的
  ↓ 用户确认
Forge 直连数据库执行，展示结果
```

### 产品形态

- **独立产品**，私有化部署
- **用户自配 LLM**：Claude API / DeepSeek / MiniMax / 任意 OpenAI 兼容接口
- **直连内网数据库**：`forge sync` 自动同步表结构
- **Registry**：组织知识库，用得越多越准确
- **交付门禁**：`forge doctor` + `production-smoke` 区分 demo、PoC 和生产可交付状态
