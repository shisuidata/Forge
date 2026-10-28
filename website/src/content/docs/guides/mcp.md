---
title: 接入你的 Agent（MCP）
description: 用 forge mcp 让 Claude Code 等 MCP 客户端在 Forge 授权内查询，范围外由人审核
---

## 它做什么

`forge mcp` 让 Claude Code、Cursor 等 MCP 客户端，以 Agent 自己的身份使用 Forge。它在你的电脑上以 stdio 运行，只调用 Forge 的受治理 v2 API：

- Agent 可以查看自己的授权范围、提交 SQL、执行 Forge 判定为 `ready` 的查询、读取结果与证据、取消尚未执行的查询。
- **没有审批工具。** 超出自主范围的查询会停在 `review_required`，只能由指定的真人在 Agent 会话之外批准。
- 被拒的查询会说明哪一道检查没有通过，模型可以据此修改 SQL 再提交。

MCP Server 由你自己部署和运行。本项目不托管任何服务，数据不会离开你的环境。

## 1. 安装

先按[五分钟上手](/guides/quickstart/)安装公开源码，然后在同一个虚拟环境里安装可选的 MCP 依赖：

```bash
cd Forge
source .venv/bin/activate
pip install -e ".[mcp]"
```

PyPI 上同名的 `forge` 是另一个无关项目，请从源码目录安装。

## 2. 用合成数据体验

```bash
claude mcp add forge-demo -- forge mcp --demo
```

`--demo` 会为每次会话新建一个只含合成数据的本地 Forge 服务（目录 `~/.forge/mcp-demo`，每次启动重建），并给 Agent 签发一份 12 小时的授权：

| 表 | Agent 能做什么 |
|---|---|
| `numbers` | 授权内自主执行 |
| `secrets` | 可以提交，但必须等人批准 |

在 Claude Code 里让 Agent “列出 numbers 表的数字，再统计 secrets 表的行数”。第一条会直接返回结果；第二条会停下来，并请你在**自己的终端**运行：

```bash
forge review --demo qr_<query-run-id>
```

`forge review` 会显示实际 SQL、请求者、目的和范围，输入 `y` 后只批准这一次。批准后让 Agent 再读取一次结果即可。关闭客户端时，演示服务随之停止。

`forge review` 没有交互式终端时拒绝运行，因此 Agent 的非交互 shell 不能替你批准。演示模式下合成审核人密钥与 Agent 在同一台机器上，有文件访问能力的 Agent 理论上仍能读到它；因为数据是合成的才可以接受。正式部署时，审核人凭证不要放在 Agent 所在的机器上。

## 3. 接入你自己的 Forge 服务

管理员先为 Agent 签发凭证和授权（Mandate），步骤见 GitHub 上的 [README：Connect your own agent](https://github.com/shisuidata/Forge#connect-your-own-agent-governed-api-v2)。然后把 Agent 凭证和授权交给 MCP 进程：

```bash
claude mcp add forge \
  -e FORGE_BASE_URL=https://forge.internal \
  -e FORGE_API_KEY=<Agent 凭证> \
  -e FORGE_MANDATE_ID=md_<授权> \
  -- forge mcp
```

客户端会把这些值保存在自己的配置文件里，请按密钥管理。审核人用自己的凭证批准：

```bash
FORGE_BASE_URL=https://forge.internal FORGE_API_KEY=<审核人凭证> forge review qr_<id>
```

## 边界

- 查询结果会返回给 Agent，进入模型上下文。敏感表不要放进自主范围，或不要放进授权。
- Forge 的只读检查不能替代数据库权限；数据源必须使用只读账号。
- 已实测：Claude Code 2.1.312 + MCP Python SDK 2.3.0（2026-10-28，合成演示）。Cursor 等其他客户端尚未实测。
- 这是维护者自己的验证，不是外部采用证据。如果你独立试过，欢迎通过 [Quickstart 回执表单](https://github.com/shisuidata/Forge/issues/new?template=quickstart-adoption.yml)告诉我们卡在哪里。
