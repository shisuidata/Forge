---
title: 安装部署
description: 从隔离合成数据安装体验 Forge，区分本地评估与生产接入
---

## 推荐入口：本地合成 Quickstart

Forge 仍处于早期开发阶段。先在本机观察 **Evaluate → Enforce → Explain**，不要从配置客户数据库、共享 API Key 或公开运行服务开始。

环境要求：Git、Bash、Python **3.11 或更新版本**。安装依赖需要网络；这条路径不需要 Node.js、Docker、LLM、Embedding、Pi、已有数据库或 `.env`。

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
bash scripts/bootstrap-dev.sh
source .venv/bin/activate
forge quickstart
```

`bootstrap-dev.sh` 创建 `.venv` 并安装当前源码与开发依赖。若默认 `python3` 版本不满足要求，可指定：

```bash
PYTHON_BIN=python3.11 bash scripts/bootstrap-dev.sh
```

Quickstart 创建隔离合成 SQLite、合成身份凭证与本地服务，经 v2 授权链展示实际 SQL 并等待批准；合成身份不代表已验证真实身份分离。它不连接客户数据，也不调用付费模型。`--agent` 另外演示有界 Agent 授权与撤销。查看 Dashboard 和保留演示工件，请继续[五分钟上手](/guides/quickstart/)。

## 版本与安装证据

上述命令安装公开仓库当前源码，不代表已经发布 PyPI 包或稳定 Release。[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-22.md)记录了公开 `main` 提交 `e50b0a0` 的 wheel 与 sdist 构建、全新虚拟环境安装和合成 Quickstart；它不是对之后的提交、每次新克隆或当前部署的认证。

## 实际集成前先确认边界

**按当前接口接入。** 公开 `main` 提供 v1 Evaluate、`/api/v2/governance/*` 与 v2 Enforce / Explain：独立主体凭证、有界授权、指定审核人与撤销；v1 执行与原始 SQL 执行入口已退休（410）。不要照旧教程启用共享凭证或绕过审核，接口对照见[架构参考](/reference/architecture/)。

正式接入需要独立评估身份、用途、任务、数据源与表范围、只读数据库角色、审核人分离、SQL/hash 绑定、超时、结果上限、审计和网络隔离。配置项或 `doctor` 显示通过，都不等于安全认证。公开版本契约可查[仓库当前状态](https://github.com/shisuidata/Forge/blob/main/docs/current-project-state.md)与[治理契约](https://github.com/shisuidata/Forge/blob/main/docs/governance-contracts.md)。

:::caution[尚不能按生产产品交付]
2026-10-04 评估所列两项 P0 安全发布阻断已修复，但修复只有仓库测试与合成环境验证；真实数据库只读角色、真实身份与客户数据、独立安全审核都未验证。**不要将 Forge 用于真实生产或共享多租户数据访问。** 本地合成演示、兼容测试、只读声明或 Compose 模板均不能替代这些验收；没有真实客户采用或生产安全认证可由此推导。
:::

本页不提供将 Forge API、Pi 或客户数据库直接暴露到公网的命令。官网是静态文档站，发布官网不意味着部署或公开 Runtime。
