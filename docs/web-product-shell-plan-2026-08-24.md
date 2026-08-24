# Forge Web Product Shell 实施蓝图（W3）

> Status: W3A planning · Requirement: REQ-2026-08-24-014 · Scope: desktop first

## 1. 目的

先建立一个可以被用户持续人工测试、指出问题和决定方向的产品框架，再把 Governance、Reusable Report、Economics 等能力逐步接入。Web 是现有 Task、Artifact、QueryRun、Report 和 Registry 的交互投影，不拥有第二套业务状态。

## 2. 当前页面问题

当前 20 个 Jinja 模板并非完全不可用，但还没有形成一个产品：

- `base.html` 的一级导航平铺约 16 个入口，日常任务、管理员功能和开发诊断混在一起；
- `/admin/dashboard` 以表数、指标数、系统连接为中心，不能回答用户“我现在要处理什么”；
- `/chat`、`/tasks`、Admin 和独立 Report 使用不同视觉与交互语法；
- `/tasks` 在一个页面内同时承担创建、列表、事件流、SQL 审批、结果、分析和报告，无法深链接到一个任务；
- 报告只能通过已知 URL 打开，没有 Report Library；
- 页面级 CSS/JS 多，Tailwind 与 Marked 使用 CDN，缺少可版本化的本地设计 token 与组件状态；
- 许多内部词直接进入界面，例如 TaskRun、StageAttempt、Artifact、Pipeline，增加理解成本。

## 3. 产品对象与信息架构

```text
Forge
├── 工作台
│   ├── 待我处理
│   ├── 进行中的任务
│   ├── 最近报告
│   └── 当前阻断
├── 新建任务
│   ├── 问题与目标
│   ├── 数据范围/口径澄清
│   └── 交付物选择
├── 任务
│   ├── 全部任务
│   └── 任务详情
│       ├── 概览/计划
│       ├── 数据与 SQL
│       ├── 分析
│       ├── 报告
│       └── 活动记录
├── 报告
│   ├── 报告库
│   └── 报告详情/下载/分享
├── 数据资产
│   ├── Registry Studio
│   ├── 数据结构
│   ├── 指标与语义
│   ├── 知识源
│   └── 草案与发布
└── 管理
    ├── 团队
    ├── 审计
    ├── 模型与 Skills
    ├── 数据源与渠道
    ├── 系统设置
    └── 诊断
```

一级导航固定为：`工作台 / 新建任务 / 任务 / 报告 / 数据资产`。`管理` 位于导航底部并使用二级页面；Pipeline、Session、Memory、Architecture 不作为普通用户一级入口。

## 4. Route 兼容矩阵

| 产品页面 | R0 原型 | 生产目标 | 现有来源/兼容 |
|---|---|---|---|
| 工作台 | `/workspace` | `/workspace`，根路径登录后进入 | 聚合 `/admin/dashboard`、Task list、Report projection；旧 `/admin/dashboard` 保留跳转期 |
| 新建任务 | `/new` | `/chat` | 复用现有 Chat/ChannelEvent；不再额外提供第二个复杂创建表单 |
| 任务列表 | `/tasks` | `/tasks` | 复用现有跨渠道 task list/flow API |
| 任务详情 | `/tasks/:id` | `/tasks/{task_run_id}` | 新增只读聚合 projection 与现有 action 入口；Pi Store 仍是真相源 |
| 报告库 | `/reports` | `/reports` | 新增 ReportStore scoped list projection |
| 报告详情 | `/reports/:id` | `/reports/{report_id}` | 复用现有 detail/technical/download/share |
| 数据资产 | `/data` | `/data` 聚合入口 | 兼容 `/admin/registry-studio|schema|metrics|semantic|staging|knowledge` |
| 管理 | `/admin` | `/admin` 聚合入口 | 兼容 teams/audit/settings 与诊断页面 |

R0 路由只在隔离原型内存在，不占用生产路径。W3B 才决定最终 redirect/alias。

## 5. 核心人工测试旅程

### Journey A：提出问题并获得报告

```text
工作台 → 新建任务 → 输入问题/交付物
→ 需要补充口径（如有）
→ 查看任务计划
→ 审核 SQL
→ 查看查询结果
→ 查看分析中的观察/判断/限制
→ 打开报告
→ 返回任务或报告库
```

每一步必须回答四个问题：现在发生了什么、系统依据什么、用户需要做什么、如果失败怎么恢复。

### Journey B：处理等待项

```text
工作台“待我处理” → 等待审批/等待补充的任务
→ 查看风险和影响 → 批准/拒绝/补充 → 回到任务进度
```

### Journey C：恢复失败任务

```text
任务筛选“失败” → 任务详情
→ 失败阶段、已完成内容、未发生的副作用
→ 合法的重试/修改需求入口
```

### Journey D：查找已交付报告

```text
报告库 → 按状态/时间/任务筛选 → 报告详情
→ 数据范围、来源、限制、revision → HTML/PDF/PPTX/分享
```

## 6. 页面状态 Contract

所有页面只实现适用状态，但同一状态必须使用一致语义：

| 状态 | 用户看到的内容 | 操作规则 |
|---|---|---|
| `loading` | 正在读取哪个对象，不伪造进度百分比 | 保留页面骨架，阻止重复副作用 |
| `empty` | 当前为空的原因和唯一合理下一步 | 不使用营销插画或口号 |
| `ready` | 核心对象、状态、证据和主操作 | 主操作每页最多一个视觉优先级 |
| `partial` | 已完成内容、缺失内容及影响 | 不把部分结果标为完成 |
| `needs_input` | 缺少的具体信息、为什么需要 | 只请求必要字段 |
| `waiting_approval` | 审批对象、风险、hash/有效期的业务解释 | 批准与拒绝清楚分离 |
| `failed` | 失败阶段、已发生/未发生副作用、恢复方式 | 不显示原始 Secret/stack trace |
| `forbidden` | 没有权限的资源与申请路径 | 不能靠前端隐藏冒充授权 |
| `offline` | 哪个依赖不可用、是否可安全重试 | 不自动重放 SQL/Action |

## 7. Visual / Interaction Direction

方向：**calm analytical workbench**，而不是 SaaS Landing Page、卡片墙或开发监控台。

- 中性暖灰背景、白色工作面、深色正文；绿色仅用于主要操作/已验证来源，琥珀用于审批，红色用于失败；
- 使用明确的文档层级、细分隔线和适中密度，避免渐变、纹理、超大 Hero、大圆角和过多胶囊标签；
- 一级导航稳定，详情页使用对象标题、状态、关键元数据和局部 tabs；
- 任务内容采用连续工作记录，不把每个阶段做成彩色营销卡片；
- 表格、SQL、分析、报告拥有各自合适的阅读模式，但共享相同按钮、状态和 spacing token；
- 强调、链接、Callout 继续遵守 REQ-012；内容遵守“专业不等于术语密度”；
- 动效只用于 drawer/dialog、状态更新和 focus，不承担信息传达；支持 reduced motion。

## 8. W3A 原型技术边界

建议建立 `tools/web-product-shell-prototype/`：

- 本地固定依赖，无 CDN；不修改生产 Pi/Forge package；
- 只使用版本化 fixture 展示多种 Task/Report 状态，页面固定显示“演示数据”；
- 所有副作用操作只能切换原型视图或 disabled，不能调用 Atlas 生产 API；
- 共享一套 token/component CSS，覆盖上述全部一级页面和 Task Detail；
- 构建后发布 Atlas 独立目录与端口，用户逐页验收；
- 用户通过 IA/交互门禁后再提出 W3B 生产改造 diff，不直接照搬 fixture state。

## 9. W3A 验收清单

- 一级导航、对象名称和页面关系无需解释即可理解；
- 从任何一级页两次以内到达新建任务、待审批任务、失败任务和最近报告；
- Task Detail 能连续展示计划、审批、结果、分析、报告和活动，但不堆成单页卡片墙；
- 已实现、演示和未开放能力视觉上不会混淆；
- 1440×900、1600×1000 无横向溢出，键盘 focus、dialog/drawer、back/forward 和深链接可用；
- 0 console/page error；无外部 CDN；无 slogan；
- 用户逐页给出 `PASS / CHANGE / REMOVE`，在通过前不改生产 Web shell。

## 10. 风险与反证

- 若 R0 原型只验证静态截图、不能完整走 Journey A/B/C/D，则不能证明交互框架可用；
- 若为了“页面齐全”添加大量未来功能入口，会造成虚假产品完成度；
- 若 W3B 需要复制 Task 状态才能匹配原型，说明 W3A 与 Contract 脱节，必须修改原型而不是新增 Web Store；
- 若用户仍难以指出当前状态和下一步，则视觉完成度不能算 PASS；
- 若 W3 长期阻塞 M1A，企业多用户开放必须继续失败关闭，不得用漂亮前端掩盖 Runtime Governance Coverage=0。
