# Forge

**面向数据团队的 AI 查询 Agent —— 用自然语言提问，得到确定性 SQL。**

[English](README.md)

---

Forge 以飞书/钉钉 Bot 的形式接入你的团队。数据团队成员用自然语言提问，Forge 生成 SQL 并展示审核，确认后直接执行。

```
用户（飞书）          → "统计每个城市 VIP 用户的平均客单价"
Forge Agent          → 通过 Structured Output 生成 Forge JSON
Forge 编译器         → 确定性编译：Forge JSON → SQL
用户审核 SQL         → 点击 ✅ 执行
数据库               → 返回结果
```

## 为什么不直接用 Text-to-SQL？

| 问题 | Forge 的解法 |
|---|---|
| 字段名/表名幻觉 | Schema Registry —— 只有已注册的实体才能编译通过 |
| JOIN 类型推断错误 | 强制显式声明，不存在默认值 |
| NOT IN 遇 NULL 静默失败 | `anti` join 是唯一的反连接原语，NOT IN 不存在 |
| WHERE 与 HAVING 混淆 | `filter` 和 `having` 是独立字段，编译器强制位置 |
| 业务指标定义歧义 | Registry 语义层 —— 「复购率」定义一次，全团队共用 |
| 方言差异 | 编译器处理方言，DSL 本身无方言 |

**Forge 针对的是生成错误**（模型推理正确，但翻译成 SQL 时出错）。Structured Output 在 token 级别约束生成，格式错误的查询物理上不可能出现。

## 架构

```
飞书 / 钉钉              ← 用户交互（查询 + 指标定义）
后台管理页面             ← 数据工程师：Registry 管理、审计日志、系统配置
        ↓
Forge 后端（FastAPI，私有化部署）
  ├── Agent 循环         ← 查询模式 + 指标定义模式
  ├── LLM 客户端         ← Anthropic 或任意 OpenAI 兼容模型
  ├── Forge 编译器       ← Forge JSON → SQL（确定性）
  ├── Registry           ← 数据库结构 + 业务指标（随使用持续积累）
  └── 数据库连接         ← forge sync + 查询执行
```

私有化部署，自带 LLM API Key，数据不出内网。

## 快速开始

### 1. 安装

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
pip install -e .
```

### 2. 配置

```bash
cp .env.example .env
# 编辑 .env：填入 FEISHU_APP_ID、LLM_API_KEY、DATABASE_URL
```

### 3. 同步数据库表结构

```bash
forge sync
# 或指定连接：forge sync --db postgresql://user:pass@host/db
```

### 4. 启动服务

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 5. 配置飞书 Webhook

在[飞书开放平台](https://open.feishu.cn)将事件订阅地址设置为：
```
https://your-server/webhook/feishu
```

详见 [docs/feishu-setup.md](docs/feishu-setup.md)。

## Forge DSL

Forge JSON 由 LLM 生成（人类无需手写），确定性编译为 SQL。

```json
{
  "scan": "orders",
  "joins": [{"type": "left", "table": "users", "on": {"left": "orders.user_id", "right": "users.id"}}],
  "filter": [{"col": "orders.status", "op": "eq", "val": "completed"}],
  "group":  ["users.city"],
  "agg":    [{"fn": "avg", "col": "orders.total_amount", "as": "avg_value"}],
  "select": ["users.city", "avg_value"],
  "sort":   [{"col": "avg_value", "dir": "desc"}]
}
```

编译结果：

```sql
SELECT users.city, AVG(orders.total_amount) AS avg_value
FROM orders
LEFT JOIN users ON orders.user_id = users.id
WHERE orders.status = 'completed'
GROUP BY users.city
ORDER BY avg_value DESC
```

## 项目结构

```
forge/          — 编译引擎（JSON Schema + 确定性编译器）
agent/          — Agent 循环、LLM 客户端、飞书 Bot、会话、审计日志
web/            — 后台管理 UI（Registry 管理、审计日志、系统配置）
tests/          — 编译器测试 + SQL 失败案例设计靶心
main.py         — FastAPI 入口
config.py       — 环境变量配置
docs/           — 架构文档、部署指南
```

## 当前状态

🚧 早期开发阶段 —— 编译引擎已完成，Agent + 飞书 Bot 骨架已建立
