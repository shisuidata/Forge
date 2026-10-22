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

公开源码的 Quickstart 创建隔离合成 SQLite 与本地服务，展示实际 SQL 并等待批准；它使用 v1 演示路径并关闭隔离服务的入口认证，不代表已验证真实身份分离。它不连接客户数据，也不调用付费模型。查看 Dashboard 和保留演示工件，请继续[五分钟上手](/guides/quickstart/)。公开 CLI 尚无 `--agent` 参数。

## 版本与安装证据

上述命令安装公开仓库当前源码，不代表已经发布 PyPI 包或稳定 Release。[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-14.md)记录了 2026-10-04 本地含未提交修改工作树的独立 wheel 安装与合成 Quickstart；它不是对公开 `main`、每次新克隆或当前部署的认证，也没有验证 sdist。

## 实际集成前先确认边界

**公开源码与本地候选不能混用。** 本站审阅的公开 `main` 提供 v1 Evaluate / Enforce / Explain 与普通 Quickstart。维护者本地未提交候选才包含 `/api/v2/governance/*`、v2 Enforce / Explain、独立主体凭证、有界授权和指定审核人；v1/原始 SQL 执行入口退休（410）也只描述该本地候选。不要将本地评估报告当作从 GitHub 克隆就能获得这些能力的发布说明，更不要照旧教程启用共享凭证或绕过审核。

正式接入需要独立评估身份、用途、任务、数据源与表范围、只读数据库角色、审核人分离、SQL/hash 绑定、超时、结果上限、审计和网络隔离。配置项或 `doctor` 显示通过，都不等于安全认证。公开版本契约可查[仓库当前状态](https://github.com/shisuidata/Forge/blob/main/docs/current-project-state.md)与[治理契约](https://github.com/shisuidata/Forge/blob/main/docs/governance-contracts.md)，但这些公开文件不自动包含本地未发布变更。

:::caution[尚不能按生产产品交付]
最新公开测试摘要仍记录两项未在该报告中关闭的 P0 安全发布阻断：Direct SQL 授权检查不完整、Pi 跨主体事件去重隔离缺口。**不要将该快照用于真实生产或共享多租户数据访问。** 本地合成演示、兼容测试、只读声明或 Compose 模板均不能替代修复与回归验收；没有真实客户采用或生产安全认证可由此推导。
:::

本页不提供将 Forge API、Pi 或客户数据库直接暴露到公网的命令。官网是静态文档站，发布官网不意味着部署或公开 Runtime。
