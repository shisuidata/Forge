# Product Spine SP5 集成门禁证据（2026-08-25）

> Requirement：[`REQ-2026-08-25-017`](requirements-pool.md#req-2026-08-25-017短期-product-spine-底层优先实施计划)<br>
> 原始 Candidate：`product-spine-5dcd4715941a`；性能修复：`product-spine-d0aa8c9e3a0e`；Table 最终修复：`product-spine-6a23e71276e5`
> URL：`http://192.168.8.10:18007/`<br>
> 自动化/真实链路结论：**PASS**<br>
> 用户 Atlas 门禁：**CHANGE 已修复，等待复验**

## 1. 范围与安全边界

本轮验证真实运行以下主链：

```text
Conversation
→ Pi Task / Plan / StageAttempt
→ Forge QueryRun / Assurance / SQL Review
→ 人工测试批准
→ 只读 SQLite 执行
→ QueryResult / Analysis / Report
→ ReportStore / HTML / PDF / PPTX
→ Conversation / Task / Report Library Product Projection
```

候选环境保持以下隔离：

- Pi state、QueryRun DB、Report DB、Registry copy、Artifact Store 与候选源码均位于独立 candidate root；
- 测试数据源复制到 candidate 内，SHA-256 为 `5dff83ed45ee54df6319a04c98408159b31e1572643d936ecb0125f083d32b5b`，mode `0400`，无 WAL/SHM；
- API 为 `192.168.8.10:18007`，Pi 只监听 `127.0.0.1:14311`；
- Web 正式认证已开启，匿名访问 `/workspace` 返回 302；
- 未读取、回显或复制 Secret；未修改生产 Registry、Identity Map、数据库凭证或状态库；
- `forge-m41-api.service`、`forge-m41-pi.service` 与候选两个 service 最终均为 active。

本轮批准只授权 candidate 只读测试数据上的精确 SQL。批准不代表分析结论必然正确。

## 2. Live Gate 发现并修复的问题

SP4 的 fixture/browser 门禁不能发现以下真实集成缺陷。SP5 逐项定位 root cause 后做了最小修复：

1. **内网 HTTP 下 Chat 无法发送**
   `crypto.randomUUID()` 在非 Secure Context 不可用，Chat 在绑定 submit listener 前失败，浏览器退化为 `GET /chat?`。改为 `crypto.getRandomValues()` 生成 128-bit ID，不使用 `Math.random()`。
2. **Conversation 轮询停在瞬时 `ready`**
   Query preparation 中间状态会短暂投影为 `ready` 且没有 action；旧逻辑只在 `running` 时继续轮询，导致 SQL Review 已进入真相源但页面看不到。现在仅当 `ready` 且存在 enabled action 时停止，否则继续轮询。
3. **报告链接在刷新后退化为纯文本**
   Pi Presentation 在内网使用同源绝对 HTTP URL，安全 Markdown Renderer 只允许站内路径和 HTTPS。现在只把 `origin === location.origin` 的 URL规范化为站内路径；外部 HTTP 仍拒绝。
4. **成功 Attempt 被误标为失败**
   历史成功记录可能保存空字符串 error。Store 现在把空白错误规范化为 `null`；Projection 对旧数据也只在 error 非空时生成 `safe_error` 和 redaction。
5. **真实长 SQL 撑破 Task Detail Grid**
   嵌套 Grid 子项的 intrinsic width 将页面撑到 1725px。增加 `.layout-grid > * { min-width: 0; }`；页面不再横向溢出，SQL 仍在代码块内部滚动。
6. **完成态历史 SQL 被误写成“没有权限”**
   完成后的 ReviewRequest 仍用于追溯，但已不存在 `approve_query` action。页面现在显示“审核已结束，仅供追溯”，不再把动作终止误解为权限故障。

## 3. 最终三次连续 Golden Journey

固定问题：

> 统计各渠道类型的订单数、总GMV和平均客单价（已完成订单），分析主要差异并生成完整报告。

该问题来自版本化 40 题测试集，输出 6 行渠道类型结果，可确定性复算。三次连续主链运行于同一 Execution/Contract 源码和 candidate 独立只读数据副本；随后只修改完成态历史 SQL 展示和 Conversation 列表切换后的只读轮询判定。最终 `5dcd4715941a` 对 Product Shell 定向测试和全部 12 个桌面路由重新执行门禁，未改变任何写路径、Action 或 QueryRun。

| Run | TaskRun | QueryRun | Report | Review 可见 | Analysis 可见 | Report 可见 | 总耗时 | SQL 执行 |
|---|---|---|---|---:|---:|---:|---:|---:|
| 1 | `tr_0af461cc281144ce9549856b7590ba3c` | `qr_5a12ff78f6954c61a3cdf91769883a5c` | `rp_27fc8c6ee19fc56d72f465ab3f4f1a89` | 28.190s | 138.413s | 227.425s | 239.589s | 11ms / 6 rows |
| 2 | `tr_c214d9eb695a4a379bd41a651da9ac70` | `qr_d55918eb767f4085b21dc41175b33c30` | `rp_a6e35449eb059ebf1aec9241df224db6` | 30.130s | 103.189s | 221.981s | 235.117s | 9ms / 6 rows |
| 3 | `tr_cfd61f1c42d04539bc8f3fb135eff47a` | `qr_dbe5668357bb4bb993a48f6b077a5220` | `rp_1cbc1e6cf85c15da60f085543bf75f1c` | 31.978s | 118.168s | 179.165s | 194.106s | 8ms / 6 rows |

每个 Run 均满足：

- 1 个 QueryRun、1 个 `query.completed` Event；
- Query preparation、query execution、analysis、report 共 4 个 StageAttempt，全部 `succeeded`；
- SQL hash 与 Assurance Report hash 持久化，QueryRun `completed`；
- Task `completed`，Report `published`；
- PDF/PPTX 均 `ready`；
- 浏览器 0 console/page error；
- 同一消息首次 202、精确重投 200，未创建第二 Task/QueryRun；
- SQL 批准首次 202；完成后重投过期 action 返回 409，失败关闭且未重放 SQL。

## 4. Restart、offline 与浏览器门禁

### Restart

- 在等待 SQL 审批时重启 Pi，原 Conversation/Task/Review 恢复，未重新 prepare SQL；随后从同一页面完成执行、分析和报告。
- 最终三次 Run 后同时重启 Pi/API；重启前后 Task/Event/Attempt/Artifact/QueryRun/Report 的有界证据 JSON 字节一致。
- 重启不增加 QueryRun、`query.completed`、StageAttempt 或 Report。

### Offline / partial

临时停止 candidate Pi 后：

- `/api/product/workspace` 仍返回 200；
- `projection_meta.availability=partial`；
- dependency `pi_orchestrator=offline`；
- Report Library 与 Data 仍可投影；
- 页面明确显示“工作台部分可用 / 任务运行时暂不可用”；
- 0 console/page error，0 横向溢出；
- 恢复 Pi 后两个 candidate service 均为 active。

### Desktop / assets

对 1440×900 与 1600×1000 的以下 6 条路由逐一验证：

- `/workspace`
- `/chat?conversation=...`
- `/tasks`
- `/tasks/{task_run_id}`
- `/reports`
- `/data`

共 12 个页面检查点：HTTP 200、0 external request、0 console/page error、0 horizontal overflow。Product Shell 保持 0 CDN、无 inline business script、无 `innerHTML/eval/localStorage` 业务状态。

## 5. 失败关闭证据与反证

SP5 没有只保留成功样本：

1. “4–6 月各月、各渠道销售额增长”问题的模型候选连续 3 次引用未定义计算别名，Forge `registry_acl_alias` Gate 拒绝；两个 Task 均 failed、QueryRun 未执行。该结果证明 Assurance fail-closed，但也说明模型对复杂时间×渠道问题仍不稳定。
2. “不同品类销售额”可成功查询，但一次 Analysis 因数据质量风险返回 `incomplete`，系统没有生成 Report。这是正确 Safe Abstention，不应通过降低质量门禁让它伪装成功。
3. 当前 Product Projection 尚未开放补查参数，`request_supplement` 仍明确 disabled；因此 incomplete 的新 UI 恢复体验仍是后续 Product UX/Contract 缺口。
4. SQL action 完成后的精确重投由 Web 的 stale-action gate 返回 409，而不是返回原 200/202 Presentation；安全不变量通过，但若希望用户看到“已处理”而非冲突，需要单独设计幂等 UX，不应放宽执行门禁。

一条固定 Golden Journey 和三次连续成功不能证明开放世界正确，也不能把 Runtime Governance Coverage 从 0% 提升为已实现。

## 6. 自动化验证

最终本地回归：

- Python：`583 passed / 24 skipped`；
- Pi TypeScript：`115 passed`；
- strict TypeScript typecheck：PASS；
- npm audit：0 vulnerabilities；
- JS syntax：PASS；
- `git diff --check`：PASS；
- Product 双桌面 viewport：12/12 PASS；
- candidate auth/restart/offline/read-only datasource：PASS。

## 7. Verdict 与下一步

SP5 自动化与真实集成门禁通过，Product Spine 已具备可持续人工测试抓手。当前仍不能宣称产品最终通过，因为用户尚未对 Atlas 页面给出主观门禁结果。

下一步：

1. 用户访问 `http://192.168.8.10:18007/`，按页面和真实 Journey 给出 `PASS / CHANGE / REMOVE`；
2. 将用户反馈按 P0/P1/P2 写回需求池；
3. 用户门禁通过后，再根据真实最大阻力选择 M1A、G1、Q1、H6 或 Product UX P1；
4. 未经新批准，不并行开放 Agent Runtime、多人治理、Economics、Reusable Report 或 H5 生产 Renderer。

## 8. Atlas P0 性能修复

用户首次 Atlas 体验明确返回“网站性能非常差”。真实浏览器基线确认该反馈：

| Product API | 修复前 | 修复后稳定值 |
|---|---:|---:|
| Workspace | 15,727 ms | 258.8 ms |
| Conversations | 20,970 ms | 134.1 ms |
| Tasks | 28,985 ms | 87.0 ms |
| Reports | 并发超过 30 s | 54.5 ms |
| Data Summary | 并发超过 30 s | 103.2 ms |

分段测量中，Pi 原始 `/v1/tasks` 与 `/v1/conversations` 只需 `55.9 ms` 和 `44.4 ms`。根因不在网络、SQLite 或 Pi Projection，而在 Python Contract 边界：`validate_contract` 对每个 Task/Report 都重新执行 `validator_for`、完整 `check_schema` 和 validator construction。100 条 Task + 50 份 Report 的回归样本单独耗时 `12.83s`。

修复采用按 Contract name 缓存已检查 validator；每个 instance 仍完整执行 shape validation，Product semantic/scope/redaction/Evidence/Assurance 均未减少。性能回归测试要求同一批 150 个 Product Projection 在 2 秒内完成，修复后测试进程 `0.18s` 完成。

真实候选复验：

| 页面 | DOM ready | 严格渲染完成 |
|---|---:|---:|
| Workspace | 37.2 ms | 586.9 ms |
| Chat | 21.9 ms | 198.6 ms |
| Tasks | 53.5 ms | 163.3 ms |
| Task Detail | 18.5 ms | 83.7 ms |
| Reports | 17.7 ms | 98.5 ms |
| Data | 19.0 ms | 147.3 ms |

严格渲染完成以各页 skeleton/“正在读取”内容被真实 DOM 数据替换为准，不以 HTTP response header 到达冒充页面完成。六页均为 0 console/page error、0 横向溢出。

性能修复源码发布为 `product-spine-d0aa8c9e3a0e`，URL 仍为 `http://192.168.8.10:18007/`。它复用原 candidate 的独立 Pi/QueryRun/Report/Registry/Artifact 状态与 mode `0400` 只读测试数据，只切换 API source root；生产 Forge/Pi 未替换。契约、Governance、Product BFF 与 Shell 定向回归 `41 passed`。

自动性能门禁结论为 **PASS**；用户 Atlas 复验仍为 **PENDING**。

## 9. Atlas P0 Conversation 查询结果 Table 修复

用户真实页面已显示“查询结果”“可用”和“共 107 行”，但没有结果 Table。链路检查结果：

```text
QueryResultArtifact
→ ChannelPresentation.table：存在
→ Conversation/Task Product Projection presentation.table：存在
→ Task Detail renderPresentation：可显示
→ Conversation renderConversation：只渲染 Markdown/fields，Table 丢失
```

根因位于 Web DOM Renderer，不在 Pi、QueryResult、Contract 或 Product Projection。`renderConversation()` 已改为复用现有 `renderPresentation()`；该函数统一渲染 Markdown、fields、有界 Table 和 truncated notice，原重复 fields 分支已删除。

浏览器行为回归使用 `ready_for_analysis` QueryResult fixture 固定 2 列、2 行与 truncated notice；修复前在 `.conversation-entry table.data-table` 断言失败，修复后通过。

真实候选复验新建 Conversation `web_conv_66e18f83b37539de3637d8699df5b422`，Task `tr_c0c65389a9a344e9b711cfa68909f6eb` 完成 SQL 审核和一次只读执行。Product Projection 与实际 DOM 结果：

- 总结果 `107` 行；
- `category_name / total_sales` 两列；
- Conversation 显示前 `20` 行有界预览；
- `truncated=true`，页面显示“结果已截断”；
- 0 横向溢出。

最终源码/缓存候选 `product-spine-6a23e71276e5` 已切换至稳定 URL。六个 Product 页面将资源版本统一提升为 `product-pages.js?v=2`；全新正常缓存浏览器确认加载 v2，并显示 20 行 Table 与截断提示。候选继续复用原独立 Pi/QueryRun/Report/Registry/Artifact 状态和 mode `0400` 只读测试数据；生产 Forge/Pi 未替换。浏览器行为回归从失败转为通过，页面/缓存契约 `11 passed`，JS syntax PASS。

自动 Table 门禁结论为 **PASS**；用户 Atlas 复验仍为 **PENDING**。

## 10. 完整未来 Product Shell 扩展

用户在真实 Atlas 基础上确认选择“完整未来产品壳”。Candidate `product-spine-2ceffbcf1600` 增加：

- 工作：工作台、对话、任务、交付；
- 信任：数据资产、治理与审计；
- 接入：Agents & Apps；
- 系统：管理；
- 全局 Search、Inbox、Workspace context；
- Decision、Evidence/Assurance、Policy/Mandate、Audit；
- Agent Clients、Data Runtime API、Agent Activity；
- Quality/Freshness、Conflict/Proposal、Reusable Deliverable、Outcome/Feedback；
- 统一 Product Shell 404/403/offline。

现有真相源通过链接或 Product BFF 接入；未来能力只显示 `planned/blocked`、依赖阶段和 disabled 原因。没有 fixture 业务记录、Agent Credential、通用 Decision、Policy 写入、Outcome Ledger 或前端业务状态库。

验证：

- 23 个有效/状态 Route 和 404 产品页；
- 1440×900、1600×1000、390px，0 横向溢出；
- 有效页面 0 console/page error；
- 移动导航 8 个一级入口，drawer 可打开；
- CSS `v4`、Product Shell JS `v2`、Product Pages JS `v3` 正常加载；
- 定向 `17 passed`、QueryResult Table 浏览器行为 `1 passed`、JS syntax PASS；
- 真实 107 行 QueryResult 主链保持 20 行 Table 与截断提示。

自动门禁为 **PASS**；用户 Atlas 门禁仍为 **PENDING**。Runtime Governance Coverage 仍为 0%。

## 11. Product Chat 任务状态侧栏回归修复

用户指出完整 Product Shell 的 `/chat` 没有任务状态侧边栏。检查确认：

```text
Conversation Projection：存在最新关联 Task
Task Detail Projection：存在 Plan / Action / Artifact / Activity
Product Chat Template：只有对话列表 + Conversation 两列
Product Pages JS：没有读取或渲染 Task Detail
```

修复后：

- 桌面右侧常驻“当前任务”；
- 显示 Task 状态、计划完成数、最多 12 个 PlanStep；
- 显示当前 Action、Artifact/Evidence 摘要、最近 Activity；
- 切换 Conversation 与 polling 自动跟随最新 Task，并用 request epoch 防止旧响应覆盖；
- 移动端使用右侧 Drawer，支持 backdrop、背景滚动锁、`aria-expanded`、Escape 关闭和 focus return；
- 页面不读取 raw Store、不推进 Task、不暴露 Prompt/hidden CoT/Secret。

真实 Conversation `web_conv_66e18f83b37539de3637d8699df5b422` 展示完成态任务、4 个 PlanStep、Artifact/Evidence 与最近 Activity。1440×900 桌面常驻侧栏无溢出；390×844 Drawer 打开/关闭状态、backdrop 和滚动锁通过；有效路径 0 console/page error。

最终 candidate：`product-spine-1de35ae9acc3`；CSS `v5`，Product Pages JS `v5`。定向 `17 passed`，Product Conversation 浏览器行为 `2 passed`，JS syntax PASS。

自动门禁为 **PASS**；用户 Atlas 复验仍为 **PENDING**。

## 12. Task Sidebar 轮询闪烁修复

用户指出侧栏会持续刷新闪烁。根因为 `loadConversationTask()` 每轮都执行：

```text
已有 Sidebar
→ 替换成 loading
→ fetch 同一 Task Detail
→ replaceChildren 重建全部 DOM
```

修复采用语义 fingerprint：

- fingerprint 只包含实际展示和 Action 行为相关的 Task、Plan、Action、Artifact、Activity、Review；
- 排除每次请求都会变化但不影响 UI 的 `projection_meta.generated_at`；
- 同一 Task + 相同 fingerprint 直接 no-op，不改变 Sidebar DOM；
- Projection 真实变化时才重绘，并恢复原 `scrollTop`；
- 切换到不同 Task 时仍显示首次 loading；
- 刷新失败且已有有效状态时继续显示最后状态，通过 aria-live 提示刷新失败；
- request epoch 继续丢弃切换 Conversation 后返回的旧响应。

浏览器回归在至少两次相同轮询之间写入 DOM stability probe，确认节点未被替换；随后注入新的 Activity，确认 Sidebar 更新且 scroll 保持。QueryResult Table、桌面常驻侧栏、移动 Drawer/backdrop/Escape 回归均通过。

最终 candidate：`product-spine-beb59d1a56f7`；Product Pages JS `v6`。资源/页面契约 `11 passed`，Product Conversation 浏览器行为 `2 passed`，JS syntax PASS。

自动门禁为 **PASS**；用户 Atlas 复验仍为 **PENDING**。
