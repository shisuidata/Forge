# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

---

## Pi 平台改造计划治理

涉及 Pi、Forge 执行层、拾穗 DATA Skills、Web/飞书/钉钉渠道或职责迁移的任务时：

1. 开始前必须阅读 `docs/platform-architecture.md` 和 `docs/pi-forge-integration-plan.md`。
2. 用户确认新需求或方向变化后，先更新计划文档，再修改代码。
3. 完成一个步骤后，回写计划中的当前状态、验证结果、遗留风险和下一步。
4. Pi 是唯一主 Orchestrator；Forge 是可信执行层。不得新增双写任务状态或第二套主调度流程。
5. 若实现与计划冲突，先暂停并澄清，不能靠临时兼容继续堆叠职责。

## 产品定位

**Forge 是一个面向数据团队的 AI 查询 Agent，私有化部署，让弱模型也能生成可信 SQL。**

### 解决的问题

SQL 生成错误分两类：

| 错误类型 | 定义 | Forge 能解决 |
|---|---|---|
| 生成错误 | 推理正确，但翻译成 SQL 时出错（忘写 OVER、JOIN 无类型等） | ✅ DSL 约束 + Structured Output |
| 业务逻辑错误 | 指标定义歧义（复购率的分母是什么） | ✅ Registry 语义层 |
| 算法逻辑错误 | 模型不知道该用 CTE 解决这个问题 | ❌ 超出 Forge 能力边界 |

Forge 的核心价值：**把生成错误降到接近零，通过 Registry 消灭业务逻辑错误。**

### 完整工作流

```
用户自然语言
  ↓
Forge Agent（理解意图）
  ↓ Structured Output
Forge JSON       ← 格式由 JSON Schema 强制约束，生成错误物理上不可能
  ↓ 确定性编译
SQL              ← 人工审核，审核者看到的 = 实际执行的
  ↓ 用户确认
Forge 直连数据库执行，展示结果
```

---

## 产品形态

**独立产品，私有化部署，用户自配 LLM。**

```
私有化部署的 Forge Agent
    ├── 对话界面（Web）
    ├── 自配 LLM（Codex API / 本地模型 / 任意兼容 OpenAI 接口的模型）
    ├── 直连内网数据库
    │     ├── forge sync  → 自动同步表结构到 Registry
    │     └── 执行已审核的 SQL，展示结果
    └── Registry（组织知识库，随使用积累）
          ├── 结构层：表、字段（forge sync 自动生成）
          └── 语义层：业务指标定义（对话式维护）
```

**Registry 是产品的核心资产**——记录组织的数据结构和业务语义，用得越多越准确。

### Registry 两层设计

- **结构层**：`forge sync --db <connection>` 直连数据库自动生成，无需手动维护
- **语义层**：通过对话定义业务指标，AI 提取结构，用户确认后写入

```
用户：复购率是指下过 2 次及以上订单的用户，除以所有下过至少 1 次订单的用户
Forge：分子 = 订单数 >= 2 的用户数，分母 = 订单数 >= 1 的用户数，是否正确？
用户：对
Forge：已保存，以后"复购率"直接可用
```

### Agent 的两个模式

- **查询模式**：自然语言 → Forge JSON → SQL → 审核 → 执行 → 展示结果
- **定义模式**：自然语言定义指标 → AI 提取结构 → 用户确认 → 写入 Registry

---

## 技术实现

**语言**：Python

**Forge DSL 格式**：JSON（不是自定义文本语法）

**生成方式**：Structured Output（token 级别约束）

**为什么是 JSON + Structured Output**：
- LLM 对 JSON 生成准确率最高
- JSON Schema 约束枚举值/必填字段，在生成时就已强制
- Parser 零成本

---

## 当前状态与路线图

### 已完成 ✅

**Forge DSL 核心**
- `forge/schema.json`：Forge DSL 格式定义（JSON Schema）
- `forge/compiler.py`：Forge JSON → SQL 确定性编译器（支持 window/CTE/anti-join 等）
- `forge/cli.py`：CLI 入口（compile / sync / sync-staging）
- 53 个编译器测试用例

**Registry 三层语义架构**（`registry/data/`）
- `schema.registry.json`：结构层，`forge sync` 自动生成
- `metrics.registry.yaml`：指标语义层，对话式维护
- `disambiguations.registry.yaml`：业务歧义消除规则（新）
- `field_conventions.registry.yaml`：字段使用约定（新）
- `registry/staging_sync.py`：staging → registry 自动合并（新）

**Agent 对话循环**
- `agent/agent.py`：查询模式 / 定义模式 / 澄清轮次（新） / 提案模式
- `agent/session.py`：对话状态 + `IntentSpec` 澄清中间状态（新）
- `agent/llm.py`：歧义规则 + 字段约定 → 自动注入 system prompt（新）
- `agent/feishu.py`：飞书长连接 Bot
- `forge/cache.py`：查询缓存（两阶段确认）

**其他**
- `forge sync`：直连数据库生成结构层
- `web/router.py`：Web API
- `tests/accuracy/`：40 题 EA 基准，当前 ~72%（Method K，大 Schema）

### 待建设
- [ ] Web 界面（Admin UI）
- [ ] 数据库直连执行 + 结果展示（`forge/executor.py` 已有雏形）
- [ ] `forge sync-staging` 定时轮询（cron / systemd timer）
- [ ] EA 继续提升（目标 80%+）
- [ ] 多轮对话记忆（跨会话 Session 持久化）

---

## 测试用例（`tests/text-to-sql-failures/`）

Forge 真正的靶心（生成错误 + 业务逻辑错误）：

| 案例 | 错误类型 | Forge 是否解法 |
|---|---|---|
| A1 LEFT JOIN vs INNER | 生成错误 | ✅ |
| A2 NOT IN NULL 陷阱 | 生成错误 | ✅ |
| B1 WHERE vs HAVING 混淆 | 生成错误 | ✅ |
| D1 复购率指标歧义 | 业务逻辑错误 | ✅ Registry |
| E2 窗口函数语法错误 | 生成错误 | ✅ |
| B2 与自身均值比较 | 算法逻辑错误 | ❌ |
| E1 每组取 TopN | 算法逻辑错误 | ❌ |

### 测试 Schema

```sql
users        (id, name, city, created_at, is_vip)
orders       (id, user_id, status, total_amount, created_at)
order_items  (id, order_id, product_id, quantity, unit_price)
products     (id, name, category, cost_price)
```
