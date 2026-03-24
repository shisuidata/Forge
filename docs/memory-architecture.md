# Forge Agent 记忆架构设计

> 设计思路：借鉴数仓分层理论，结合认知科学的记忆分类，构建 AI Agent 的三层记忆系统。
> 本文档持续记录设计讨论过程。

## 0. 命名体系

| 层 | 全称 | 缩写 | 认知科学类比 | 职责 |
|---|---|---|---|---|
| 轨迹层 | **E**pisodic **M**emory **S**tore | **EMS** | 情景记忆 — 经历过的事 | 完整的对话轨迹，全量保留 |
| 知识层 | **S**emantic **M**emory **P**ool | **SMP** | 语义记忆 — 知道的知识 | 从轨迹中提炼的结构化知识 |
| 上下文层 | **W**orking **M**emory **B**uffer | **WMB** | 工作记忆 — 正在用的信息 | 按场景实时裁剪，注入 LLM |

> 命名决策（2026-03-24）：参考数仓分层思想但不复用数仓术语（ODS/DWD/ADS），
> 改用认知科学的记忆分类，概念自解释且无歧义。

---

## 1. 问题分析

### 1.1 当前架构的痛点

| 痛点 | 表现 | 根因 |
|---|---|---|
| Session 溢出 | 20 条限制，旧消息被丢弃 | 只有一层"短期记忆" |
| Session 污染 | LLM 模仿 `[SQL ready for review]` 等内部标记 | 所有消息不加区分地塞入 history |
| 上下文冲突 | 查询模式不需要看到指标定义过程 | 所有场景共享同一个 history |
| 知识不累积 | 用户纠正过的错误下次还会犯 | 没有"长期记忆"，重启即丢失 |
| 无法回溯 | 无法查看完整的对话链路 | audit 只记录 SQL 结果，不记录推理过程 |

### 1.2 核心洞察

> "记忆管理和数据管理是同一个问题——原始数据保留完整性，中间层提炼价值，应用层按需裁剪。"

---

## 2. 三层记忆架构

```
┌─────────────────────────────────────────────────────────┐
│                  WMB 工作记忆层                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ 查询模式  │  │ 定义模式  │  │ 管理助手  │  ... 更多场景 │
│  │ 注入：    │  │ 注入：    │  │ 注入：    │              │
│  │ 相关表结构 │  │ 历史纠错  │  │ 全量指标  │              │
│  │ 相关指标   │  │ 已有指标  │  │ 变更日志  │              │
│  │ 用户偏好   │  │ 命名规范  │  │ 用户偏好  │              │
│  └──────────┘  └──────────┘  └──────────┘              │
├─────────────────────────────────────────────────────────┤
│                  SMP 语义记忆层                           │
│                                                         │
│  ┌─ 用户画像 ─┐  ┌─ 纠错记录 ─┐  ┌─ 确认事实 ─┐        │
│  │ 常用表      │  │ 错误→修正   │  │ 指标定义    │        │
│  │ 查询偏好    │  │ 触发条件    │  │ 歧义消解    │        │
│  │ 时间范围    │  │ 发生频率    │  │ 字段约定    │        │
│  └────────────┘  └────────────┘  └────────────┘        │
│                                                         │
│  提炼规则：对话结束后异步提取 → 结构化存储                   │
├─────────────────────────────────────────────────────────┤
│                  EMS 情景记忆层                           │
│                                                         │
│  每轮对话的完整记录（不丢弃、不修改）：                       │
│  - user message（原始输入）                               │
│  - assistant response（含 tool_call、reasoning）          │
│  - system injections（编译错误、lint 结果）                │
│  - action results（SQL、执行结果、用户反馈）               │
│  - timestamps + session_id + user_id                    │
│                                                         │
│  存储：SQLite / 文件系统，按 session_id 分组               │
└─────────────────────────────────────────────────────────┘
```

---

## 3. 各层详细设计

### 3.1 EMS 情景记忆层

**原则**：全量保留，只追加不修改，是所有上层记忆的事实来源。

**存储模型**：

```sql
CREATE TABLE memory_ems (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id  TEXT NOT NULL,           -- 一次连续对话的唯一 ID
    user_id     TEXT NOT NULL,           -- 用户标识
    seq         INTEGER NOT NULL,        -- 本 session 内的消息序号
    role        TEXT NOT NULL,           -- user / assistant / system / tool
    content     TEXT NOT NULL,           -- 消息内容
    tool_name   TEXT,                    -- 工具调用名（generate_forge_query / define_metric 等）
    tool_input  TEXT,                    -- 工具输入 JSON
    tool_output TEXT,                    -- 工具输出（SQL / 错误信息）
    action      TEXT,                    -- 动作类型：sql_review / approved / cancelled / error
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc')),

    UNIQUE(session_id, seq)
);

-- 按 session 查询完整对话链
CREATE INDEX idx_ems_session ON memory_ems(session_id, seq);
-- 按用户查询所有 session
CREATE INDEX idx_ems_user ON memory_ems(user_id, created_at);
```

**写入时机**：每次 `agent.process()` / `approve()` / `cancel()` 调用时同步写入。

**与现有 audit 的关系**：
- `audit_log` 只记录查询级别的摘要（一行一个查询）
- `memory_ems` 记录消息级别的完整对话（一行一条消息）
- audit 是 EMS 的聚合视图，可以保留也可以改为从 EMS 生成

### 3.2 SMP 语义记忆层

**原则**：从 EMS 提炼结构化事实，去除噪声，所有场景可共享。

**知识类别**：

| 类别 | 结构 | 提炼规则 | 示例 |
|---|---|---|---|
| **用户画像** | `{user_id, preferences}` | 统计用户常用表、常问的维度、偏好的时间范围 | "该用户 80% 的查询涉及 orders 表" |
| **纠错记录** | `{trigger, wrong, correct, count}` | 当用户取消 SQL 并重新描述时，记录"错误→正确"的映射 | "用户说'销售额'时模型用了 gmv，用户纠正为 pay_gmv" |
| **确认事实** | `{key, value, confirmed_at}` | 从 cache_verify 的查询中提取已验证的业务规则 | "复购率 = 下单≥2次用户数 / 总下单用户数" |
| **会话摘要** | `{session_id, summary, topics}` | 对话结束后用 LLM 生成一句话摘要 | "用户查询了武汉地区的订单金额分布，确认了 CASE WHEN 分桶逻辑" |

**存储模型**：

```sql
CREATE TABLE memory_smp (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT NOT NULL,
    category    TEXT NOT NULL,           -- user_profile / correction / confirmed_fact / session_summary
    key         TEXT NOT NULL,           -- 知识点的唯一标识
    value       TEXT NOT NULL,           -- JSON 结构化内容
    source_sessions TEXT,                -- 来源 session_id 列表（溯源）
    confidence  REAL DEFAULT 1.0,        -- 置信度（0-1），多次确认时递增
    created_at  TEXT NOT NULL DEFAULT (datetime('now','utc')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now','utc')),

    UNIQUE(user_id, category, key)
);
```

**提炼时机**：
- **实时提炼**：approve/cancel/cache_verify 时立即提取简单事实
- **异步提炼**：对话结束后（30s 无新消息）用 LLM 生成会话摘要和纠错记录
- **定期提炼**：每日聚合用户画像（常用表、偏好维度等）

### 3.3 WMB 工作记忆层

**原则**：不存储，每次 LLM 调用前实时构建。从 SMP 按场景裁剪，注入 system prompt。

**场景定义**：

```python
# 每个场景定义自己需要的记忆切片
SCENE_CONFIGS = {
    "query": {
        "inject": [
            "user_profile.frequent_tables",     # 用户常用表（提高检索权重）
            "user_profile.time_preference",     # 默认时间范围偏好
            "corrections.recent(5)",            # 最近 5 条纠错（防止重复犯错）
            "confirmed_facts.relevant(query)",  # 与当前查询相关的已确认事实
        ],
        "history_strategy": "last_2_turns",     # 只保留最近 2 轮对话
    },
    "define": {
        "inject": [
            "confirmed_facts.all_metrics",      # 所有已确认的指标定义
            "corrections.metric_related",       # 指标相关的纠错记录
            "user_profile.naming_convention",   # 用户的命名偏好
        ],
        "history_strategy": "full_session",     # 保留完整定义对话
    },
    "admin_assist": {
        "inject": [
            "confirmed_facts.all",              # 全量已确认事实
            "session_summaries.recent(10)",     # 最近 10 个会话摘要
            "corrections.all",                  # 全量纠错记录
        ],
        "history_strategy": "last_3_turns",
    },
}
```

**构建流程**：

```python
def build_context(scene: str, user_id: str, current_query: str) -> str:
    """从 SMP 层按场景配置构建注入上下文。"""
    config = SCENE_CONFIGS[scene]
    parts = []
    for inject_key in config["inject"]:
        knowledge = smp_store.query(user_id, inject_key, current_query)
        if knowledge:
            parts.append(knowledge)
    return "\n\n".join(parts)
```

---

## 4. 数据流转

```
用户发消息
    ↓
┌─ EMS 写入 ──────────────────────────────────────────┐
│ memory_ems.insert(session_id, seq, role, content...) │
└─────────────────────────────────────────────────────┘
    ↓
┌─ WMB 构建上下文 ────────────────────────────────────┐
│ context = build_context("query", user_id, message)   │
│ system_prompt = base_prompt + registry + context     │
└─────────────────────────────────────────────────────┘
    ↓
LLM 调用
    ↓
┌─ EMS 写入 ──────────────────────────────────────────┐
│ memory_ems.insert(session_id, seq, role, response...) │
└─────────────────────────────────────────────────────┘
    ↓
用户 approve / cancel / verify
    ↓
┌─ SMP 实时提炼 ──────────────────────────────────────┐
│ if action == "cache_verify":                         │
│     smp_store.upsert("confirmed_fact", ...)          │
│ if action == "cancelled" and retried:                │
│     smp_store.upsert("correction", ...)              │
└─────────────────────────────────────────────────────┘
    ↓
对话结束（30s 无新消息）
    ↓
┌─ SMP 异步提炼 ──────────────────────────────────────┐
│ summary = LLM.summarize(ems.get_session(session_id)) │
│ corrections = LLM.extract_corrections(session)       │
│ smp_store.upsert("session_summary", summary)         │
│ smp_store.upsert_batch("correction", corrections)    │
└─────────────────────────────────────────────────────┘
```

---

## 5. 与现有系统的关系

| 现有模块 | 记忆架构中的位置 | 改动 |
|---|---|---|
| `agent/session.py` (Session.history) | 被 WMB 层替代 | history 不再作为唯一上下文来源 |
| `agent/audit.py` (audit_log) | EMS 的聚合视图 | 可保留，或改为从 EMS 生成 |
| `forge/cache.py` (查询缓存) | SMP confirmed_facts 的一种 | 合并入 SMP 层 |
| Registry (metrics/disambiguations) | SMP 层的持久化输出 | 不变，但可被 SMP 自动补充 |

---

## 6. 讨论记录

### 2026-03-24 首次讨论

**用户提出**：记忆应该用数仓分层理论进行分层。

**核心观点**：
1. 原始记忆层保留所有沟通记录、思考和动作（EMS）
2. 公共层提炼分析有价值和重要的记忆（SMP）
3. 面向不同 AI 使用场景的再次生产（WMB）
4. 用户强调"有绝对的控制权"——AI 辅助但人类确认

**设计决策**：
- EMS 用 SQLite 存储，全量保留
- SMP 提炼分两种：实时（approve/cancel 时）和异步（对话结束后 LLM 总结）
- WMB 不存储，每次调用前实时构建
- 现有 session.history 的 20 条限制由 WMB 的 history_strategy 替代

---

## 7. 设计决策（2026-03-24 确认）

| 问题 | 决策 | 备注 |
|---|---|---|
| EMS 保留策略 | **默认无限保留**，可配置滚动 | `forge.yaml` 加 `memory.ems.retention_days` |
| SMP 提炼模型 | **默认用产品配置的 LLM**，可选配独立模型 | `forge.yaml` 加 `memory.extract_model`（可选） |
| 多用户知识共享 | **组织级 + 个人级分离，实时互补** | 见 7.1 |
| 迁移策略 | **直接替换 session.py** | 不做并行过渡 |

### 7.1 组织记忆 vs 个人记忆

```
┌─ SMP 语义记忆层 ─────────────────────────────────┐
│                                                   │
│  ┌─ org（组织级）────────────────────────────────┐ │
│  │ 业务事实：复购率定义、销售额口径               │ │
│  │ 纠错记录：gmv vs pay_gmv 的区别              │ │
│  │ 字段约定：order_status 过滤规则               │ │
│  │                                               │ │
│  │ 特点：所有用户可读，任何用户的确认都会写入     │ │
│  └───────────────────────────────────────────────┘ │
│                                                   │
│  ┌─ user（个人级）──────────────────────────────┐ │
│  │ 查询偏好：常用表、默认时间范围                 │ │
│  │ 交互习惯：喜欢简洁回复 or 详细解释            │ │
│  │ 个人纠错：该用户特有的表达→意图映射           │ │
│  │                                               │ │
│  │ 特点：仅本人可读写，不影响其他用户             │ │
│  └───────────────────────────────────────────────┘ │
│                                                   │
│  互补机制：                                        │
│  - 个人确认的业务事实 → 自动提升到组织级           │
│  - 组织级知识 → 自动注入所有用户的 WMB 上下文      │
│  - 冲突时个人级优先（个人可覆盖组织默认值）         │
└───────────────────────────────────────────────────┘
```

### 7.2 待实现细项

- [ ] EMS 表 + 写入层（替换 session.py）
- [ ] SMP 表 + org/user 双层存储
- [ ] 实时提炼器（approve/cancel/verify 触发）
- [ ] 异步提炼器（对话结束后 LLM 总结）
- [ ] WMB 场景构建器（替换现有 system prompt 拼装）
- [ ] forge.yaml 配置项
- [ ] 从 session.py 迁移 agent.py / feishu.py / router.py 的调用方
