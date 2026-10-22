# 最新测试与验证报告（公开摘要）

> **验证日期：2026-10-22。受测版本：公开 `main` 提交 [`e50b0a0ab71d9459e48f2c4fd5c13770d5672c06`](https://github.com/shisuidata/Forge/commit/e50b0a0ab71d9459e48f2c4fd5c13770d5672c06)（2026-10-22）。**
>
> 本次验证在该提交的 `git archive` 导出目录中运行，不含本地未提交文件；远端 CI 对同一提交单独运行。测试通过不构成生产就绪、共享多租户安全、独立安全审核或当前部署实例的认证。

## 结论

[2026-10-04 评估](latest-test-report-2026-10-14.md)记录的两项 **P0 安全发布阻断**和全部 **P1** 缺口已在公开 `main` 修复，每项修复都先用合成反例复现、修复后同一反例通过。同期另修复控制台 AI 面板的 HTML 注入、审批与执行租约到期的滞留状态，以及修复过程中引入的一个读路径锁竞争。

这些修复只有仓库测试与隔离合成环境验证。**真实 PostgreSQL/MySQL 只读角色、真实身份与客户数据、独立安全审核和外部采用仍未验证**；不要据此将 Forge 用于真实生产或共享多租户数据访问。

## 已修复项

本节说明风险和修复边界，不公开可直接利用的输入或逐行定位。

| 原级别 | 问题 | 修复后行为 | 修复提交 |
|---|---|---|---|
| P0 | Direct SQL 校验对象与实际执行文本可能不一致 | 只读校验按目标数据源方言做完整解析，只接受单条查询，拒绝写入、锁定、任意位置的 DML/DDL 及已知副作用函数；资源授权复用同一份解析结果；执行前按真实数据源方言再校验一次。Assurance 升为 `query-assurance-v13` | [`46286ad`](https://github.com/shisuidata/Forge/commit/46286ad) |
| P0 | Pi 渠道事件去重缺少主体作用域 | 去重键加入受信主体作用域；同一事件 ID 输入不同返回 409；返回已有任务前重新核对归属；异步投影查询同样按主体隔离 | [`46286ad`](https://github.com/shisuidata/Forge/commit/46286ad) |
| P1 | 认证后撤销凭证，仍可提交 Mandate 变更 | 签发、撤销、读取 Mandate 在同一治理事务内重新核对凭证或渠道绑定，失效返回 `credential_revoked`（401） | [`691003c`](https://github.com/shisuidata/Forge/commit/691003c) |
| P1 | 注销只清 Cookie，旧会话仍可用 | 注销时撤销服务端 Session，复制出的旧 token 立即失效 | [`691003c`](https://github.com/shisuidata/Forge/commit/691003c) |
| P1 | 报告发布结果不确定时重新生成内容 | 报告只生成一次，已保存的报告包是唯一来源；恢复时先查询发布状态，已发布直接采用，确认未发布才原样重发；Forge 接受同一报告包重发作为显式重试 | [`7417172`](https://github.com/shisuidata/Forge/commit/7417172) |
| P1 | 重启恢复忽略远端失败/取消终态 | 远端失败、取消、过期都会收敛到对应的 Task 终态，并保留失败码，不重新审批或执行 | [`e751ce9`](https://github.com/shisuidata/Forge/commit/e751ce9) |
| P1 | 查询准备阶段的中断与其他阶段不一致 | 调用方中断与超时记为可重试，只有 Forge 明确失败才终止 | [`d9bea6f`](https://github.com/shisuidata/Forge/commit/d9bea6f) |
| P1 | 旧控制台依赖外部 CDN；Dashboard 展示退休入口 | Tailwind 与 marked 改为随包本地资源，wheel/sdist 均包含；Dashboard 显示当前 v2 Enforce/Explain 入口 | [`3cb09f2`](https://github.com/shisuidata/Forge/commit/3cb09f2) |
| 新发现 | 控制台 AI 面板把模型回复作为原始 HTML 插入 | 改为白名单渲染：只保留 Markdown 格式元素与安全链接，属性与原始 HTML 不进入页面；Accuracy Lab 同步转义动态文本 | [`dd5fe1d`](https://github.com/shisuidata/Forge/commit/dd5fe1d) |
| P2 | 审批 TTL 与执行租约到期后状态滞留 | 读取时把审批过期落库为 `expired` 并移出审核待办，把租约过期的执行落库为 `failed`，不重放 SQL；Pi 接受 `expired` 并收敛 Task | [`447f901`](https://github.com/shisuidata/Forge/commit/447f901)、[`e50b0a0`](https://github.com/shisuidata/Forge/commit/e50b0a0) |

升级影响：

- Pi 状态库自动从 schema v4 迁移到 v5，迁移后旧版 Pi 无法打开，升级前应备份。
- 旧 Assurance 版本下准备的 QueryRun 会因版本漂移失败关闭，需要重新 prepare 和审批。

## 已实际验证的结果

以下均为 2026-10-22 对受测提交的实际运行结果。

| 项目 | 实际结果 | 不能据此推导 |
|---|---|---|
| 远端 CI | [运行 39747886386](https://github.com/shisuidata/Forge/actions/runs/39747886386)：test、wheel、sdist、compatibility、pi-contracts 五项通过 | CI 中 PostgreSQL/MySQL 兼容任务是合成兼容检查，不是真实只读角色验收 |
| Python 全套（导出目录） | **1043 passed / 32 skipped** | 跳过不等于通过 |
| Python 全套（同一提交的仓库检出） | **1049 passed / 26 skipped** | 多出的 6 项依赖 `.git` 元数据（4 项）和不随仓库分发的本地 Mini-Dev 数据（2 项） |
| Pi TypeScript | typecheck 通过 | 类型正确不等于授权正确 |
| Pi 测试 | **226 passed / 0 failed**，Node 24 与 CI 所用的 Node 22 均通过 | 未覆盖的合成反例仍可能暴露缺陷 |
| wheel 与 sdist | 两者均从导出目录构建，分别安装到全新虚拟环境，在非源码目录运行 package smoke 通过（含随包静态资源检查与合成 Quickstart） | 不是已发布软件包或 Release |
| Quickstart（已安装 wheel） | 普通模式：写入被 `readonly_violation` 拒绝，人工审核链 completed，Explain `verified`。`--agent`：自主与指定审核人两链 completed，撤销后新 prepare 返回 409 `mandate_inactive` | 无模型、无 Pi，不是自然语言 Agent 体验验收 |
| 禁外网真实浏览器 | 登录页与 Dashboard 只加载本地资源，无外部请求；Dashboard 高 818px（修复前同方法约 23619px，含 1264px SVG） | 只覆盖这两个页面和 1280×800 视口 |
| AI 面板注入 | 真实 Chromium 中 7 类 HTML/脚本/链接注入全部失败关闭，正常 Markdown 格式保留 | 不是全站 XSS 审计 |

Quickstart 的 Explain 同时给出 `semantic_correctness_unproven`、`live_data_no_snapshot`、`authorization_is_not_semantic_correctness`、`result_truncated` 四项限制；`verified` 只表示证据链校验通过，不表示业务语义正确。

## 修复过程中的 CI 失败

`7417172` 到 `e575455` 之间的远端 CI 曾经失败，原因有两个：

- 一项 Pi 测试依赖事件循环中的非引用计时器，在 Node 22 上失败，由 [`e575455`](https://github.com/shisuidata/Forge/commit/e575455) 改为模拟时钟。
- `447f901` 的到期落库在每次读取时都会获取写锁，可能与事件循环线程上的治理事务互相等待，CI 观察到 `database is locked`。[`e50b0a0`](https://github.com/shisuidata/Forge/commit/e50b0a0) 改为先只读探测、需要时以自动提交写入，并补了持锁读取的回归测试。

这两处失败均已修复，受测提交的 CI 全部通过；中间提交的 CI 失败记录保留不改。

## 仍未关闭的范围

- 只读校验中的副作用函数是一份拒绝清单，不可能列全；**真实数据库只读角色仍是最终边界**，真实 PostgreSQL/MySQL 角色和会话权限未验证。
- 查询准备被中断后，用新幂等键重试可能在 Forge 留下一条无人跟踪的待审 QueryRun；它无法在未经审批或领取的情况下执行，并会在审批 TTL 到期后自动变为 `expired`。是否在新建时作废同任务旧记录，属于待定的产品决策。
- 2026-10-04 记录的工作台低对比度节点未处理；全页面键盘、屏幕阅读器与移动设备检查未完成。
- 当前部署实例未更新，本报告不涉及任何部署环境。
- 真实身份与权限迁移、客户数据、真实模型与完整聊天旅程、负载与高可用、备份恢复、独立渗透测试均未验证。
- 非维护者独立完成 Golden Path、外部采用与独立安全审核门禁仍未通过。

## 阅读与公开边界

本文与 2026-10-04 的历史摘要并存，不覆盖其结论；该历史摘要记录的是当时的本地工作树状态。本文排除了凭证、会话值、内部网络与部署信息、个人路径、可直接利用的输入和逐行漏洞定位。

事实导航：[当前项目状态](../../current-project-state.md) · [2026-10-04 历史摘要](latest-test-report-2026-10-14.md) · [产品北极星](../../product-north-star.md) · [职责边界](../../platform-architecture.md) · [文档索引](../../README.md)
