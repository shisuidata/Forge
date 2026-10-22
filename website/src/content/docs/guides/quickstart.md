---
title: 五分钟上手
description: 无需 LLM、Pi 或客户数据库，体验 Evaluate → Enforce → Explain
---

## 先体验运行时，而不是配置模型

`forge quickstart` 创建隔离的合成 SQLite 数据与真实本地 Forge 服务，用一条 **Direct SQL 候选**串起 Evaluate → Enforce → Explain，并检查同一 QueryRun 的 Dashboard 投影。

**不需要** API Key、LLM、Embedding 服务、Pi、Forge JSON 知识、已有数据库或 `.env`。这不是自然语言 Agent 演示，也不会连接客户数据库。安装依赖需要网络，首次安装耗时取决于本地环境。

## 1. 安装公开源码

准备 Git、Bash 和 Python **3.11 或更新版本**，然后执行：

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
bash scripts/bootstrap-dev.sh
source .venv/bin/activate
forge quickstart
```

`bootstrap-dev.sh` 创建 `.venv`，更新 pip，并安装当前源码及开发依赖。如果默认 `python3` 不符合版本要求，可在安装时显式指定，例如：

```bash
PYTHON_BIN=python3.11 bash scripts/bootstrap-dev.sh
```

上面的命令以公开仓库当前源码为入口。Quickstart 用合成身份凭证走 v2 授权链：Agent 准备查询，指定的合成审核人批准后执行；合成身份不代表真实身份或审核人分离已经验收。不要将其服务公开或替换为真实客户数据。本页引用的测试摘要对应提交 `e50b0a0`，不等于对之后提交或每次新克隆的复测。

## 2. 阅读 SQL，再决定是否批准

默认流程会在终端展示实际 SQL，并等待你确认。请先阅读候选和审核提示，不批准就不会执行该候选。

| 阶段 | 观察什么 |
|---|---|
| 拒绝写入候选 | Evaluate 返回 `readonly_violation`，展示失败关闭路径 |
| 评估只读候选 | Evaluate 检查合成 Direct SQL，但评估本身不执行 SQL |
| 准备与审核 | Enforce 创建绑定授权、资源范围和 SQL 的 QueryRun，等待批准 |
| 执行 | 批准后执行合成查询，展示结果行数限制 |
| 解释 | Explain 回读同一 QueryRun 的实际 SQL、执行状态、Evidence 完整性和限制 |

`verified` 表示对应证据完整性检查通过，**不表示业务答案已被证明正确**。Explain 也会明确展示语义正确性未证明、实时数据缺少快照、授权不等于语义正确、结果可能截断等限制。

## 3. 保持服务，查看 Dashboard

```bash
forge quickstart --serve
```

完成演示后，命令会保持隔离服务运行，并打印实际 Dashboard 地址。使用终端给出的地址，不要假设固定端口。浏览结束后回到终端按 Enter 退出。

Dashboard 是同一 QueryRun 的只读投影，不是另一份执行真相源。该入口用于观察运行记录，不是完整自然语言聊天工作台。默认临时目录会在退出后清理；若要保留演示数据库、QueryRun、日志和摘要，使用一个空目录：

```bash
forge quickstart --workdir .forge/quickstart-proof --serve
```

## 4. 有界 Agent 授权演示

```bash
forge quickstart --agent
```

`--agent` 在同一套隔离合成数据上演示 v2 有界授权：Agent 是独立凭证主体，管理员签发限定任务、数据源和表范围的授权；自主子集内的查询由 Agent 直接执行，范围外的资源由指定的真人审核人批准；授权撤销后，新的准备请求返回 409 `mandate_inactive`。

它演示的是授权边界，既不证明模型理解能力，也不替代真实企业身份与数据库权限验收。

## 可选：用于合成环境的自动化回执

```bash
forge quickstart --yes --json
```

`--yes` 只用于命令自行创建的本地合成数据，跳过交互式确认；**不要把它当作真实部署的自动批准方案**。真实接入必须另行验证身份认证、创建者与审核人分离，以及审核 hash 的绑定；公开合成演示本身不证明这些生产条件。`--json` 需要与 `--yes` 一起使用，不能与 `--serve` 组合。

如果独立运行过这条路径，可以通过 [Quickstart 体验报告](https://github.com/shisuidata/Forge/issues/new?template=quickstart-adoption.yml)主动提交脱敏后的 `run_receipt`、全新克隆安装耗时和首次失败或困惑点。Forge 不自动发送遥测；回执 checksum 用于检测内容变化与去重，不是身份认证。

## 这条路径证明了什么

它让你观察本地合成数据上的受控 HTTP 查询链路，不证明任意 SQL 都安全、任意数据库都兼容、业务答案正确，或产品已经生产就绪。

[最新公开测试摘要](https://github.com/shisuidata/Forge/blob/main/docs/archive/engineering/latest-test-report-2026-10-22.md)记录了 **2026-10-22** 对公开 `main` 提交 `e50b0a0` 的验证：Python **1049 passed / 26 skipped**（仓库检出）、Pi **226 passed**，远端 CI 五项通过，从 wheel 与 sdist 全新安装后普通与 `--agent` Quickstart 均通过；Pi 的测试成绩不意味着这条 Quickstart 依赖 Pi。

2026-10-04 评估所列两项 P0 安全发布阻断（Direct SQL 授权检查与 Pi 跨主体事件去重隔离）已在该版本修复并有回归测试。这些仍是仓库测试与合成环境验证：真实数据库只读角色、真实身份与客户数据、独立安全审核都未验证，不要把它当作生产或共享多租户安全的认证，也不要接入真实客户数据来替代安全验收。

继续阅读：[Forge 介绍](/guides/introduction/) · [基准测试与实验边界](/reference/benchmarks/) · [仓库当前状态](https://github.com/shisuidata/Forge/blob/main/docs/current-project-state.md)。
