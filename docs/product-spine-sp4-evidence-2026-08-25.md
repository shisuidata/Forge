# Product Spine SP4 Evidence（2026-08-25）

> Requirement: `REQ-2026-08-25-017` · Scope: Real Product Pages · Verdict: PASS for SP5 entry

## 页面

- `/workspace`：真实 WorkspaceProjection；
- `/chat`：Conversation list/detail、连续 Task entries、发送消息和 typed action；
- `/tasks`：真实 scope-aware TaskSummary list；
- `/tasks/{task_run_id}`：Plan、SQL Review、Presentation、Artifact/Evidence、relations、activity、typed actions；
- `/reports`：ReportStore library、status filter、cursor；
- `/data`：Registry/Metric counts 与现有维护入口。

所有页面使用 `product_base.html` 和本地 `/static/product/*`，不使用 fixture、CDN、inline business script 或第二前端 Store。

## 真实操作边界

- Chat message 继续走 `/api/pi/chat/messages`；
- Task action 继续走 `/api/pi/chat/tasks/{id}/actions`；
- Query approval 从 Task Detail 读取精确 `query_run_id/sql_hash/assurance_report_hash`；
- `request_supplement` 因参数尚未进入 Product Projection，明确 disabled，不猜 payload；
- 无 `conversation_id` 的旧/direct Task 明确 disabled，不绕过 ChannelEvent；
- Markdown 使用本地 allowlisted DOM renderer，无 `innerHTML/eval`；链接只允许站内路径或 HTTPS，并设置 external noopener。

## 视觉门禁

桌面 Playwright：

- 1440×900、1600×1000；
- Workspace/Chat/Task list/Task detail/Reports/Data 全 route；
- 0 console/page error；
- 0 横向溢出；
- SQL Review dialog 可开关；
- Chat 真实发送请求 body 具有 `web_conv_*` Conversation ID；
- back/deep link、local assets 可用。

首轮任务详情视觉审查发现三个阻断：主审批操作只在侧栏、pending step 被标为“可用”、多层状态无区分。已修正：

- SQL Review 内容路径增加主审批按钮；
- pending 显示“未开始”；
- 删除 active-nav 无语义绿点；
- Task/Step/Activity 使用不同权重；
- 收口 Assurance/Artifact 等用户文案。

复审结果：无 P0；后续文案 P1 已同步修正“取消任务或离开不会执行 SQL”和“当前输出”。截图：`/tmp/forge-sp4-task-detail.png`。

## 验证

- Python：`583 passed / 24 skipped`；
- Product/Web/Docs 定向：`33 passed`；
- Pi：`114 passed`；
- strict typecheck：PASS；
- npm audit：0 vulnerabilities；
- JS syntax check：PASS；
- Playwright routes/dialog/chat send/viewport：PASS；
- Web content/no-CDN/no-dead-create-form：PASS；
- `git diff --check`：PASS。

## 未做

- 未在 Atlas 部署；
- 浏览器门禁使用真实页面代码和 Product API contract fixture interception，不是完整 Pi/Forge live chain；
- 尚未运行真实模型、真实只读 QueryRun、真实 Report exporter；
- 未验证 Atlas refresh/restart 和独立状态恢复。

这些属于 SP5，不得以 SP4 自动测试冒充真实 Golden Journey。

## Verdict

SP4 PASS for SP5 entry。允许进入 SP5 集成门禁与持续测试环境；生产/Atlas 是否替换仍需 SP5 证据和用户人工门禁。
