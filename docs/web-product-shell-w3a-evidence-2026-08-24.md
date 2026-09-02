# W3A Web Product Shell Evidence（2026-08-24）

> Verdict: automated gates PASS · user IA/interaction gate pending

## Scope

W3A 只验证产品地图、桌面 Product Shell、页面关系、状态语言和核心人工旅程。不修改生产 Jinja、Pi/Forge Runtime、Task/Approval/Report/Registry Store，也不实现 M1A/H6。

## Artifact

- Source: `tools/web-product-shell-prototype/`
- Commit: `821065f`
- Atlas: `http://preview.internal.invalid:18006/`
- Remote revision: `/srv/forge/previews/web-shell-821065f/`
- Service: `forge-web-shell-preview.service`
- Binding: LAN only `preview.internal.invalid:18006`

## Product map

| Area | Route | Key states/actions |
|---|---|---|
| 工作台 | `#/workspace` | 待审批、待补充、失败恢复、进行中、最近报告 |
| 新建任务 | `#/new` | 问题、交付物、范围；只创建浏览器 fixture |
| 任务 | `#/tasks` | 状态筛选、搜索、跨渠道列表 |
| 任务详情 | `#/tasks/:id?tab=` | 概览、数据与 SQL、分析、报告、活动记录 |
| 报告 | `#/reports` | ready/rendering 筛选、报告库 |
| 报告详情 | `#/reports/:id` | 版本、数据期间、质量、格式、来源、正文预览 |
| 数据资产 | `#/data?tab=` | 概览、结构、指标与语义、草案与发布 |
| 管理 | `#/admin` | 组织/审计、运行配置、诊断分组 |

Fixture 固定覆盖 `waiting_approval / needs_input / analyzing / rendering / completed / failed`；原型控制可显示 querying/offline。所有页面固定显示演示边界，未实现功能只显示说明或 disabled。

## Interaction gate

Playwright 覆盖：

1. 工作台待处理/进行中/最近报告与一级导航；
2. 新建演示任务 → needs_input Task Detail；
3. Task list 搜索；
4. 深链接打开 SQL tab；
5. SQL dialog 展示任务、数据源、范围、系统限制、完整 SQL、4 项检查和无生产副作用说明；未勾选时确认 disabled；
6. 演示确认 → querying 状态，不发生产请求；
7. 原型控制切换失败状态；
8. back/forward/reload 后深链接仍可恢复；
9. Report Library → Report Detail；
10. 数据资产 tabs、管理页、键盘 focus。

结果：

- 1440×900：PASS
- 1600×1000：PASS
- Console/Page errors: 0
- Horizontal overflow: 0
- Production requests: 0；源码无 `fetch/XMLHttpRequest/WebSocket`
- CSP：`default-src 'self'`，无 inline style、无 CDN

Screenshots:

- `/tmp/forge-web-shell-atlas-1600/01-workspace.png`
- `/tmp/forge-web-shell-atlas-1600/02-sql-approval.png`
- `/tmp/forge-web-shell-atlas-1600/03-report-detail.png`
- `/tmp/forge-web-shell-atlas-1600/04-admin.png`

## Automated regression

- Product shell tests: `5 passed`
- Product shell build: JS `38.97 kB / 13.29 kB gzip`、CSS `29.81 kB / 6.46 kB gzip`
- Product shell npm audit: `0 vulnerabilities`
- Python full suite: `564 passed / 24 skipped`（Existing Web targeted: `19 passed`)
- Pi Orchestrator typecheck: PASS
- Pi Orchestrator: `103 passed`
- `git diff --check`: PASS

## Atlas isolation

远端三个静态构建文件与本地固定 build 一致。部署后：

- `forge-web-shell-preview.service`: active
- `forge-report-preview.service`: active
- `forge-m41-api.service`: active
- `forge-m41-pi.service`: active
- Production source: clean `d2b0fd9`

未读取/修改 Secret、Identity Map、Registry、数据库 URL/凭证。回滚只需停止并 disable preview service，删除 `web-shell-current` symlink、固定 revision 目录和 user unit。

## Human gate

用户需要逐页给出：

- `PASS`：信息架构和交互方向可进入 W3B；
- `CHANGE`：明确页面、区域、状态或操作；
- `REMOVE`：不应进入生产 Shell 的对象或功能。

重点不是像素微调，而是：

1. 一级入口是否符合使用顺序；
2. 工作台是否真正回答“现在要处理什么”；
3. Task Detail 是否能承载完整任务而不成为卡片墙；
4. 审批对象、风险和下一步是否清楚；
5. Report/Data/Admin 是否属于同一个产品；
6. 是否存在看起来可用但实际不该出现的功能。

用户通过前，W3B production shell、Task Detail projection 和 Report Library API 均不开始。
