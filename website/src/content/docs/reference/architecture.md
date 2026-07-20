---
title: 架构设计
description: Forge 的系统组件、数据流与关键设计决策
---

## 系统架构

```
┌─────────────────────────────────────────────────────────┐
│  User Interface                                         │
│  ┌──────────────────┐    ┌───────────────────────────┐  │
│  │  Feishu / DingTalk │   │  Admin Web UI (/admin)    │  │
│  │  Bot               │   │  Registry · Audit · Config│  │
│  └────────┬───────────┘   └──────────┬────────────────┘  │
└───────────┼──────────────────────────┼──────────────────┘
            │ webhook                  │ HTTP
┌───────────▼──────────────────────────▼──────────────────┐
│  Forge Backend (FastAPI)                                 │
│                                                          │
│  ┌───────────────────────────────────────────────────┐   │
│  │  Agent Loop (agent/agent.py)                      │   │
│  │  query mode:  NL → LLM → Forge JSON → SQL → review│  │
│  │  define mode: NL → LLM → metric → save            │   │
│  └──────────────┬────────────────────────────────────┘   │
│                 │                                         │
│  ┌──────────────▼────────┐  ┌─────────────────────────┐  │
│  │  LLM Client           │  │  Forge Compiler         │  │
│  │  Anthropic / OpenAI   │  │  JSON Schema validation  │  │
│  │  Tool use / Struct Out│  │  → deterministic SQL     │  │
│  └───────────────────────┘  └─────────────────────────┘  │
│                                                          │
│  ┌───────────────────────┐  ┌─────────────────────────┐  │
│  │  Registry             │  │  Audit Log              │  │
│  │  · structural layer   │  │  forge_audit.db         │  │
│  │  · semantic layer     │  │  · query history        │  │
│  └───────────────────────┘  └─────────────────────────┘  │
│                                                          │
│  ┌───────────────────────────────────────────────────┐   │
│  │  Database Connection                              │   │
│  │  forge sync → structural registry                 │   │
│  │  execute approved SQL → return results            │   │
│  └───────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

---

## 关键设计决策

### Forge JSON 作为中间表示

LLM 不直接写 SQL，而是生成结构化 JSON（Forge DSL），经 JSON Schema 校验后编译。

- **Structured Output 兼容**：token 级别生成约束
- **编译前校验**：幻觉字段名在编译阶段被拒绝
- **确定性**：同样的 Forge JSON 永远产生同样的 SQL

### 两类错误边界

| 错误类型 | 定义 | Forge 能解决 |
|---|---|---|
| 生成错误 | 推理正确，SQL 翻译出错 | 是（DSL 约束 + Structured Output） |
| 业务逻辑错误 | 指标定义歧义 | 是（Registry 语义层） |
| 算法逻辑错误 | 不知道该用什么算法 | 否（超出能力边界） |

### Registry 两层管理

- **结构层**（`tables`）：`forge sync` 自动生成，不手动编辑
- **语义层**（`metrics`）：对话式维护，业务人员用自然语言定义指标

### Human-in-the-loop

所有 SQL 在执行前展示给用户审核。飞书交互卡片提供确认/取消按钮。审计日志记录每次查询及其结果。

---

## 数据流：查询模式

```
1. 用户在飞书发送消息
2. feishu.py 接收 im.message.receive_v1 事件
3. agent.py:process(user_id, text)
4. 组装 Session 历史
5. llm.py:call(history) 发送到 LLM：
   - Forge JSON Schema（作为 tool definition）
   - Registry 上下文（表结构 + 指标定义）
6. LLM 调用 generate_forge_query 工具，输出 Forge JSON
7. compiler.py:compile_query(forge_json) 产出 SQL
8. 飞书发送交互卡片，展示 SQL
9. 用户点击确认 → 执行 SQL → 返回结果
   用户点击取消 → 记录取消
10. audit.py 记录完整交互
```

## 数据流：定义模式

```
1. 用户描述指标（"复购率是指..."）
2. LLM 调用 define_metric 工具
3. agent.py:_save_metric() 写入 Registry 语义层
4. 后续查询可按名称引用该指标
```

---

> 本页为精简版。完整内容参见 [docs/architecture.md](https://github.com/shisuidata/Forge/blob/main/docs/architecture.md)。
