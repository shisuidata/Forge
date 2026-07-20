---
title: "Day 6 · 从原型到产品"
description: PostgreSQL 支持、认证鉴权、数据权限、Pipeline E2E、Web Admin 完整落地
---

> 2026-03-25。这一天把认证、数据库、权限、Pipeline 几个"迟早要做"的事一口气补齐了。

## 主题：生产就绪

之前 Forge 是一个能跑的原型——单进程、SQLite、无认证、无权限。这一天把它变成可以真正私有化部署的服务。六件事：

---

### 1. SQLite -> PostgreSQL

原来的代码直接用 SQLite 原生 API，换 PostgreSQL 时全部炸了：占位符（`?` vs `%s`）、自增主键（`lastrowid` vs `RETURNING id`）、BOOLEAN 默认值、DDL 事务行为。

解法是 `_UnifiedConn` 包装器，对上层暴露统一接口，屏蔽方言差异。DDL 改为逐条提交，"already exists" 直接跳过。

### 2. 认证鉴权

HMAC-SHA256 签名的无状态 cookie：

```
登录 → 用 admin_password 对 "user_id:timestamp" 签名 → httponly cookie
请求 → 验签 + 过期检查（7天TTL）→ 放行
```

一个容易漏的细节：浏览器 AJAX 请求带的是 session cookie 而不是 API Key header，`require_api_auth` 要额外检查 session cookie。

### 3. 数据权限：信息输入端过滤

不在 WHERE 里拼条件（容易被绕过），而是在 retriever 向量检索时只从 allowed_tables 取表。LLM 压根看不到被限制的表的 schema，编译器也会拒绝无权限表。

```
user → 查 team_table_acl → 只看到有权限的表 schema → LLM 无法引用无权限表
```

不需要运行时动态 WHERE，不存在绕过路径。

### 4. Pipeline E2E

Pipeline 已在 `agent/pipeline.py` 实现了几周，但从没接上 API。这次把两端接通：

- `/api/chat` 检测到 analyze/visualize 意图时调 `pipeline_runner.run()`
- `/api/approve` 执行完 SQL 后把结果注入 WMB（Working Memory Buffer），调 `runner.resume()`

WMB 本来设计给跨轮次状态传递，在这里正好用于跨 endpoint 的 pipeline 暂停/恢复。

### 5. Web Admin

新增 6 个管理页面：登录、记忆管理、团队管理、团队成员、文档导入、设置。Auth 配置和 Memory DB 配置直接写入 `forge.yaml`，改完即生效，无需重启。

### 6. EA 准确率：67.5% -> 70.0%

ANTI JOIN `scan` 方向修复验证有效。提升主要来自 ANTI JOIN 分类：60% -> 80%（+20pp）。

---

## 架构转变

做完这一天，Forge 从单用户原型变成多用户服务。变化的核心不是功能数量，而是**数据隔离边界**从代码约定变成了数据库约束。

两个设计的共同点：把安全性放在了比业务逻辑更低的层，使得上层代码写错了也不会造成数据泄露。

---

> 本页为精简版。完整内容参见 [docs/devlog/day6_2026-03-25.md](https://github.com/shisuidata/Forge/blob/main/docs/devlog/day6_2026-03-25.md)。
