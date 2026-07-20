---
title: 五分钟上手
description: 从零开始体验 Forge 的完整查询流程
---

## 一键 Demo

```bash
# 安装后运行 demo 脚本
bash scripts/demo-setup.sh
```

这会：
1. 生成一个 200 表的电商数仓 Demo 数据库
2. 自动执行 `forge sync` 同步表结构到 Registry
3. 启动交互式配置向导

## 启动服务

```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

访问 `http://localhost:8000`，你会看到 Chat 界面。

## 第一个查询

在对话框输入：

> 各城市的订单总额是多少

Forge 会：
1. 通过 Registry 理解"订单总额"对应哪个表和字段
2. 生成结构化的 Forge JSON
3. 编译为 SQL 供你审核
4. 你确认后直接执行，展示结果

## 定义一个业务指标

输入：

> 复购率是指下过 2 次及以上订单的用户数，除以所有下过至少 1 次订单的用户数

Forge 会提取结构化定义，你确认后写入 Registry。从此"复购率"在全组织有统一口径。

## 管理后台

访问 `http://localhost:8000/admin`：

- **概览**：系统状态 + 最近查询
- **结构层**：`forge sync` 生成的表和字段信息
- **指标库**：原子指标 + 衍生指标管理
- **语义规则**：歧义消除规则 + 字段使用约定
- **查询审计**：所有查询的完整日志

## 运行测试

```bash
# 编译器 + API 测试
pytest tests/ -v

# Playwright E2E 测试（需服务运行）
FORGE_BASE_URL=http://localhost:8000 pytest tests/test_e2e.py -v
```

## 做一次交付前 smoke

```bash
bash scripts/production-smoke.sh
```

本地开发配置可能会因为默认密码、raw SQL 开启或未确认只读账号而失败。这个失败是有价值的：它说明当前环境不能直接当生产交付环境。
