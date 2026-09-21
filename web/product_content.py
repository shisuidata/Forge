"""Product Shell capability/state content: pure data plus template rendering.

Moved from web/router.py (REQ-2026-09-21-069). Bodies unchanged; symbols made
public because shell page routes consume them across modules.
"""
from __future__ import annotations

from fastapi import Request

from web.templates import templates


def capability(
    domain: str,
    title: str,
    status: str,
    status_label: str,
    copy: str,
    dependency: str,
    action_label: str,
    href: str | None = None,
    features: tuple[str, ...] = (),
) -> dict:
    return {
        "domain": domain,
        "title": title,
        "status": status,
        "status_label": status_label,
        "copy": copy,
        "dependency": dependency,
        "action_label": action_label,
        "href": href,
        "features": features,
    }


PRODUCT_SURFACES = {
    "governance": {
        "active": "governance",
        "page_title": "治理与审计",
        "page_eyebrow": "决策、证据与责任",
        "capability_kicker": "Shared Trust & Data Foundation",
        "capability_title": "治理与审计",
        "capability_status": "partial",
        "capability_status_label": "部分可用",
        "capability_copy": "查看精确审批、Evidence、Assurance 和 Action 记录；未进入 Runtime 的 Policy 与 Mandate 明确保持关闭。",
        "capability_boundary": "查询审批与现有 Audit 可用；通用 Decision、Policy PEP 与职责分离未实现。",
        "capability_dependency": "M1A Runtime Trust / M3 Coordination",
        "capability_notice": "Contract Coverage 为 100% 不等于 Runtime Governance Coverage。当前 Runtime Coverage 仍为 0%。",
        "capabilities": (
            capability("Decision", "Decision Inbox", "partial", "部分可用", "集中查看等待确认的精确 Action。当前只接真实 SQL Approval。", "通用 DecisionRequest/Record：M3", "查看决策边界", "/governance/decisions", ("SQL hash 审批", "等待确认任务", "过期与失效")),
            capability("Evidence", "Evidence & Assurance", "partial", "部分可用", "从结论回到 QueryResult、来源、范围、截断与 Assurance。", "平台级质量指标：Q1", "查看 Evidence 边界", "/governance/evidence", ("Query Evidence", "Assurance lineage", "限制与失败")),
            capability("Audit", "Action Audit", "available", "可用", "查看查询、审批、执行和现有领域 Action 的审计记录。", "现有 Forge Audit Store", "打开审计", "/governance/audit", ("查询审计", "执行状态", "时间与责任主体")),
            capability("Policy", "Policy & Mandate", "blocked", "未接入 Runtime", "定义 Principal、Mandate、Policy、资源范围和例外处理。", "M1A 生产 PEP", "查看未开放边界", "/governance/policies", ("PrincipalContext", "DelegatedMandate", "Default Deny")),
        ),
    },
    "governance/decisions": {
        "active": "governance", "page_title": "Decision Inbox", "page_eyebrow": "精确操作确认",
        "capability_kicker": "Human Accountable", "capability_title": "Decision Inbox",
        "capability_status": "partial", "capability_status_label": "查询审批可用",
        "capability_copy": "Decision 只代表有权主体批准精确 Action，不代表批准结论正确。",
        "capability_boundary": "当前只投影 SQL Approval；Registry、Report、Policy 等通用 Decision 尚未统一。",
        "capability_dependency": "M3 Participant / Decision Runtime", "capability_notice": "审批对象变化、身份变化、Registry revision 变化或过期都会使原批准失效。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            capability("Query", "SQL Decision", "available", "可用", "从工作台或任务详情审核精确 SQL 与 Assurance hash。", "现有 Forge QueryRun", "查看待确认", "/workspace", ("精确 SQL", "只读执行", "过期失败关闭")),
            capability("Coordination", "通用 Decision History", "planned", "规划中", "按 Action、Decision Owner、scope 和 revision 查询统一决策记录。", "M3 DecisionRecord", "等待 Decision Runtime", None, ("Decision Request", "Decision Record", "职责分离")),
        ),
    },
    "governance/evidence": {
        "active": "governance", "page_title": "Evidence & Assurance", "page_eyebrow": "来源、范围与限制",
        "capability_kicker": "Assurance", "capability_title": "Evidence & Assurance",
        "capability_status": "partial", "capability_status_label": "查询链可用",
        "capability_copy": "把 QueryResult、分析、报告和审批重新连接到来源、语义、执行与限制。",
        "capability_boundary": "Query Evidence 已进入任务详情；跨 Artifact Evidence Library 与质量指标尚未统一。",
        "capability_dependency": "Q1 Platform Assurance", "capability_notice": "Evidence 证明特定来源、时间和语义下的结果，不自动成为无作用域事实。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            capability("Task", "Task Evidence", "available", "可用", "查看 QueryResult、Analysis、Report 的 Artifact 与 Evidence 引用。", "现有 Pi/Forge Product Projection", "打开任务", "/tasks", ("Artifact lineage", "QueryResult preview", "失败与限制")),
            capability("Navigation", "Evidence Drawer", "planned", "规划中", "在 Conversation、Task 和 Deliverable 中原地查看来源、范围、行和限制，不离开当前上下文。", "Q1 Evidence Index", "等待 Evidence Drawer", None, ("Contextual source", "Claim/Evidence split", "Deep link")),
            capability("Assurance", "Query Assurance", "partial", "部分可用", "审查 SQL、Registry、Policy、Model 与执行 revision。", "现有 QueryRun/Audit", "打开审计", "/admin/audit", ("SQL hash", "Registry revision", "执行状态")),
            capability("Quality", "Assurance Metrics", "planned", "规划中", "统一展示 Coverage、Clarification、Safe Abstention、Silent Error 和 Evidence Coverage。", "Q1 Quality Contract", "等待 Quality Runtime", None),
        ),
    },
    "governance/audit": {
        "active": "governance", "page_title": "Action Audit", "page_eyebrow": "谁依据什么做了什么",
        "capability_kicker": "Audit", "capability_title": "Action Audit",
        "capability_status": "available", "capability_status_label": "可用",
        "capability_copy": "现有审计继续由领域 Store 持有；Product Shell 只组织入口，不复制记录。",
        "capability_boundary": "Query Audit 已可用；跨 Registry、Model、Report 的统一资源权限仍在演进。",
        "capability_dependency": "现有 Audit Store / 后续统一 Resource Policy", "capability_notice": None,
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            capability("Query", "查询审计", "available", "可用", "查看待确认、已执行、取消和错误记录。", "Forge Audit Store", "打开查询审计", "/admin/audit", ("状态筛选", "SQL 与时间", "执行结果")),
            capability("System", "跨领域 Action Audit", "planned", "规划中", "统一检索 Registry、Model、Skill Policy、Report 分享与 Agent Action。", "M1A Resource Policy / M3 Decision", "等待统一 Audit Index", None),
        ),
    },
    "governance/policies": {
        "active": "governance", "page_title": "Policy & Mandate", "page_eyebrow": "授权与资源范围",
        "capability_kicker": "Runtime Governance", "capability_title": "Policy & Mandate",
        "capability_status": "blocked", "capability_status_label": "Runtime 未接入",
        "capability_copy": "未来由 Principal、task-scoped Mandate、Policy、Binding 和 Default Deny 共同约束 Agent Action。",
        "capability_boundary": "Schema/Contract 已评审；生产 PEP 尚未执行，Runtime Governance Coverage 为 0%。",
        "capability_dependency": "M1A Runtime Trust Foundation", "capability_notice": "本页面不会创建 Mandate、修改 Policy 或生成看似有效的授权记录。",
        "back_href": "/governance", "back_label": "治理与审计",
        "capabilities": (
            capability("Identity", "Principal & Membership", "blocked", "未开放", "区分 actor、accountable principal、organization 和 workspace。", "M1A Identity Boundary", "等待身份 Runtime", None),
            capability("Delegation", "Delegated Mandate", "blocked", "未开放", "绑定 task、purpose、audience、capability、resource scope 和 expiry。", "M1A Mandate Store", "等待 Mandate Runtime", None),
            capability("Policy", "Policy Decision", "blocked", "未开放", "生产 PEP 对每个受支持 Action 返回 allow、deny 或 conditional。", "M1A Policy PEP", "等待 Policy Runtime", None),
        ),
    },
    "runtime": {
        "active": "runtime", "page_title": "Agents & Apps", "page_eyebrow": "受控数据能力接入",
        "capability_kicker": "Agent-facing Trusted Data Runtime", "capability_title": "Agents & Apps",
        "capability_status": "blocked", "capability_status_label": "执行未开放",
        "capability_copy": "其他 Agent 将通过 Principal、Mandate、Task、Policy 和 Evidence 使用 Forge，而不是获得裸 SQL 或超级 Token。",
        "capability_boundary": "外部 prepare-query 安全边界保留；Agent Task execute、credential 与 Human takeover 尚未开放。",
        "capability_dependency": "M1A Runtime Trust → R1 Agent Data Runtime", "capability_notice": "当前页面只固定产品对象和边界，不创建 Agent Client，不回显 Credential，不开放执行。",
        "capabilities": (
            capability("Client", "Agent Clients", "blocked", "未开放", "登记 Client、Owner、Purpose、Workspace 与 expiry。", "M1A Principal/Mandate", "查看 Client 边界", "/runtime/clients", ("Owner", "Purpose", "Workspace scope")),
            capability("API", "Data Task API", "blocked", "未开放", "提交受治理的数据任务并等待 human clarification/Decision。", "R1 Agent Data Runtime", "查看 API 边界", "/runtime/tools", ("Data Task", "Approval wait", "Structured Artifact")),
            capability("Operations", "Agent Activity", "blocked", "未开放", "查看调用、失败、Evidence、人工接管和重复抑制。", "R1 Agent Consumer", "查看活动边界", "/runtime/activity", ("Task lineage", "Failure reason", "Human takeover")),
        ),
    },
    "runtime/clients": {
        "active": "runtime", "page_title": "Agent Clients", "page_eyebrow": "Owner、Purpose 与 Mandate",
        "capability_kicker": "Agent Access", "capability_title": "Agent Clients",
        "capability_status": "blocked", "capability_status_label": "未开放",
        "capability_copy": "Agent Client 不是 API Key 列表，而是绑定 Principal、Owner、Purpose、Mandate 和资源范围的受托客户端。",
        "capability_boundary": "没有 Agent Client Store；任何创建、轮换和撤销 Action 均不可用。",
        "capability_dependency": "M1A Principal/Mandate Store", "capability_notice": "不会用示例 Client 或假 Token 填充页面。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            capability("Client", "Client Registry", "blocked", "未开放", "登记 Owner、Purpose、Workspace、expiry 和状态。", "M1A Client Store", "等待 Client Runtime", None),
            capability("Credential", "Credential Lifecycle", "blocked", "未开放", "只允许创建、轮换和撤销；Secret 永不回显。", "M1A Secret Boundary", "等待 Credential Runtime", None),
        ),
    },
    "runtime/tools": {
        "active": "runtime", "page_title": "Data Runtime API", "page_eyebrow": "受治理的 Task 与 Artifact",
        "capability_kicker": "Runtime Contract", "capability_title": "Data Runtime API",
        "capability_status": "partial", "capability_status_label": "准备查询可用",
        "capability_copy": "当前外部边界只能安全准备待审核查询；完整 Agent Data Task 闭环尚未开放。",
        "capability_boundary": "`/api/prepare-query` 不执行 SQL、不批准 Action、不返回结果集。",
        "capability_dependency": "R1 Agent Data Runtime", "capability_notice": "现有接口不会被放宽成通用 execute API。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            capability("Query", "Prepare Query", "available", "可用", "生成待审核 SQL，不执行、不批准、不返回结果集。", "现有 Forge API", "查看 API Contract", "/docs", ("Bounded context", "SQL preview", "No execution")),
            capability("Task", "Data Task", "blocked", "未开放", "Agent 提交 Purpose、Deliverable 并等待澄清与 Decision。", "R1 Agent Runtime", "等待 Task API", None),
            capability("Artifact", "Structured Result", "blocked", "未开放", "消费 QueryResult、Analysis、Report reference 与 Evidence。", "R1 Artifact API", "等待 Artifact API", None),
        ),
    },
    "runtime/activity": {
        "active": "runtime", "page_title": "Agent Activity", "page_eyebrow": "调用、失败与人工接管",
        "capability_kicker": "Operations", "capability_title": "Agent Activity",
        "capability_status": "blocked", "capability_status_label": "无真实消费者",
        "capability_copy": "未来展示 Agent 发起的同一 Task、Decision、Artifact、Evidence 和 Outcome，不建立第二套活动日志。",
        "capability_boundary": "当前没有已注册 Agent Client 或 Agent Golden Journey。",
        "capability_dependency": "R1 真实 Agent Consumer", "capability_notice": "没有真实调用，因此保持空态，不生成示例活动。",
        "back_href": "/runtime", "back_label": "Agents & Apps",
        "capabilities": (
            capability("Activity", "Task Activity", "blocked", "未开放", "按 Client、Task、Action 和状态查看真实调用。", "R1 Agent Runtime", "等待真实调用", None),
            capability("Takeover", "Human Takeover", "blocked", "未开放", "人在 needs_input、waiting_decision 或 failed 时接管同一 Task。", "R1 Human Handoff", "等待 Handoff Contract", None),
        ),
    },
    "inbox": {
        "active": "", "page_title": "待办与通知", "page_eyebrow": "需要你处理的事项",
        "capability_kicker": "Inbox", "capability_title": "待办与通知",
        "capability_status": "partial", "capability_status_label": "任务待办可用",
        "capability_copy": "汇总需要补充、等待确认和失败恢复的真实 Task；通用通知订阅尚未实现。",
        "capability_boundary": "当前待办来自 Pi Task/Query Approval，不新增通知状态库。",
        "capability_dependency": "现有 Workspace Projection / 后续 Notification Contract", "capability_notice": "通知只提示真实对象，不复制或推进 Task 状态。",
        "capabilities": (
            capability("Decision", "等待确认", "available", "可用", "查看等待 SQL Approval 的精确 Action。", "现有 QueryRun/Workspace", "打开待确认", "/workspace", ("SQL Approval", "expiry", "只读执行")),
            capability("Task", "需要补充与失败恢复", "available", "可用", "查看 needs_input、failed 和 partial Task。", "现有 Pi Task Store", "打开任务", "/tasks", ("needs_input", "failed", "partial")),
            capability("Notification", "通知订阅", "planned", "规划中", "按角色、资源和风险订阅 Decision、Conflict、Quality 与 Agent 事件。", "Notification Contract", "等待通知 Runtime", None),
        ),
    },
    "manage": {
        "active": "manage", "page_title": "管理", "page_eyebrow": "部署与运行设置",
        "capability_kicker": "System", "capability_title": "管理中心",
        "capability_status": "available", "capability_status_label": "现有入口可用",
        "capability_copy": "统一组织 Workspace、Model、Skill、Channel、Database 和系统状态；写入继续由现有 Admin/Domain Store 持有。",
        "capability_boundary": "当前仍是单用户私有部署；本页不把旧 Admin 状态复制到 Product Shell。",
        "capability_dependency": "现有 Forge Admin", "capability_notice": None,
        "capabilities": (
            capability("Workspace", "Team & Workspace", "partial", "部分可用", "查看团队与成员入口；多 Workspace Policy 尚未进入 Runtime。", "现有 Team Admin / M1B", "管理团队", "/admin/teams"),
            capability("Models", "Model & Skill", "available", "可用", "配置模型、Skill 与兼容性边界。", "现有 Settings/Model Control", "打开设置", "/admin/settings"),
            capability("Connections", "Channel & Database", "available", "可用", "配置飞书、数据库和服务连接。", "现有 Settings", "打开连接设置", "/admin/settings"),
            capability("Readiness", "System Readiness", "available", "可用", "查看依赖、最近查询与运行诊断。", "现有 Dashboard", "打开系统诊断", "/admin/dashboard"),
            capability("Audit", "Audit", "available", "可用", "查看查询和执行审计记录。", "现有 Audit Store", "打开审计", "/admin/audit"),
        ),
    },
    "search": {
        "active": "", "page_title": "搜索", "page_eyebrow": "跨对象查找",
        "capability_kicker": "Global Navigation", "capability_title": "全局搜索",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "未来按权限查找 Conversation、Task、Deliverable、Data Asset、Evidence 和 Agent Client。",
        "capability_boundary": "当前没有统一、scope-aware 的跨 Store Search Index。",
        "capability_dependency": "跨对象只读索引与 Resource Policy", "capability_notice": "搜索框不会在没有索引和权限门禁时伪造结果。",
        "capabilities": (
            capability("Work", "Work Search", "planned", "规划中", "Conversation、Task、Decision 和 Deliverable。", "Product Search Index", "等待搜索索引", None),
            capability("Trust", "Trust Search", "planned", "规划中", "Data Asset、Evidence、Policy、Audit 和 revision。", "Resource Policy / Search Index", "等待信任索引", None),
        ),
    },
    "data/quality": {
        "active": "data", "page_title": "Quality & Freshness", "page_eyebrow": "数据质量与时效",
        "capability_kicker": "Data Trust", "capability_title": "Quality & Freshness",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "展示 Data Asset 的质量状态、freshness、影响任务和可采用边界。",
        "capability_boundary": "当前没有统一 Quality Contract 或 Runtime 指标。",
        "capability_dependency": "G1 Data Trust / Q1 Quality Contract", "capability_notice": "不会用静态绿灯冒充真实质量。",
        "back_href": "/data", "back_label": "数据资产",
        "capabilities": (
            capability("Quality", "Quality Status", "planned", "规划中", "规则、结果、时间和影响范围。", "Quality Contract", "等待 Quality Runtime", None),
            capability("Freshness", "Freshness", "planned", "规划中", "数据快照、迟到和过期状态。", "Datasource Snapshot Contract", "等待 Freshness Runtime", None),
        ),
    },
    "data/conflicts": {
        "active": "data", "page_title": "Conflict & Proposal", "page_eyebrow": "语义缺口与修订",
        "capability_kicker": "Data Stewardship", "capability_title": "Conflict & Proposal",
        "capability_status": "partial", "capability_status_label": "审核入口可用",
        "capability_copy": "保留多个有作用域的 Claim，通过 Proposal、Review 和 Registry revision 处理冲突。",
        "capability_boundary": "Staging/Knowledge 审核可用；通用 Claim/ConflictSet/Impact 尚未实现。",
        "capability_dependency": "G1 Claim/Conflict Runtime", "capability_notice": "新规则不会静默覆盖旧 revision，也不会自动继承旧审批。",
        "back_href": "/data", "back_label": "数据资产",
        "capabilities": (
            capability("Staging", "Registry Staging", "available", "可用", "审核并提升结构与语义候选。", "现有 Staging Store", "打开 Staging", "/admin/staging"),
            capability("Knowledge", "Knowledge Review", "available", "可用", "审核知识候选和来源。", "现有 Knowledge Store", "打开知识审核", "/admin/knowledge"),
            capability("Revision", "Diff & Revision Viewer", "partial", "部分可用", "比较 Registry Draft/Revision 和确定性 diff；后续扩展到 Policy 与 Deliverable Definition。", "现有 Registry Studio / G1 Impact", "打开 Registry Studio", "/admin/registry-studio", ("Draft vs revision", "Dangerous diff", "Rollback lineage")),
            capability("Conflict", "Conflict Set", "planned", "规划中", "比较 Claim、scope、来源、影响和修订 Decision。", "G1 Conflict Runtime", "等待 Conflict Runtime", None),
        ),
    },
    "deliverables/reusable": {
        "active": "deliverables", "page_title": "Reusable Deliverables", "page_eyebrow": "定义、运行与修订",
        "capability_kicker": "Deliverable", "capability_title": "Reusable Deliverables",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "把已确认的语义查询、判断标准和交付方式保存为版本化定义，再创建不可变 Run。",
        "capability_boundary": "现有 Report revision 可用；Definition、SemanticQuerySpec 和 Run History 尚未实现。",
        "capability_dependency": "H6 Reusable Deliverables", "capability_notice": "保存模板不会隐式获得自动调度或免审批执行权。",
        "back_href": "/deliverables", "back_label": "交付",
        "capabilities": (
            capability("Report", "Report Library", "available", "可用", "查看现有不可变报告与导出。", "现有 Report Store", "打开报告", "/reports"),
            capability("Definition", "Reusable Definition", "planned", "规划中", "版本化 SemanticQuery、Criteria、Skill 和 Delivery policy。", "H6 Definition Contract", "等待 Definition Runtime", None),
            capability("Run", "Run History", "planned", "规划中", "比较不可变 Run、数据快照和 revision lineage。", "H6 ReportRun", "等待 Run Runtime", None),
        ),
    },
    "deliverables/outcomes": {
        "active": "deliverables", "page_title": "Outcome & Feedback", "page_eyebrow": "采用、纠错与复用",
        "capability_kicker": "Learning Loop", "capability_title": "Outcome & Feedback",
        "capability_status": "planned", "capability_status_label": "规划中",
        "capability_copy": "记录交付是否被采用、修正和复用，并形成受审核的知识或规则 Proposal。",
        "capability_boundary": "当前没有统一 Outcome Ledger；反馈不能自动污染组织知识。",
        "capability_dependency": "Q1 Outcome Record / G1 Proposal", "capability_notice": "不会用点击量或消息数冒充可信 Outcome。",
        "back_href": "/deliverables", "back_label": "交付",
        "capabilities": (
            capability("Outcome", "Outcome Acceptance", "planned", "规划中", "记录采用、拒绝、覆盖和责任主体。", "Q1 Outcome Record", "等待 Outcome Runtime", None),
            capability("Feedback", "Correction Proposal", "planned", "规划中", "把失败或修正转为 Registry/Policy/Test Proposal。", "G1 Proposal Runtime", "等待 Proposal Runtime", None),
        ),
    },
}


PRODUCT_STATE_PAGES = {
    "not_found": {
        "active": "", "page_title": "页面未找到", "page_eyebrow": "未知 Route",
        "state": "empty", "state_label": "未找到", "state_kicker": "404",
        "state_title": "这里没有可用的产品页面",
        "state_copy": "该 Route 不属于当前 Product Map，或对象已经被移除。系统不会猜测相似资源，也不会泄漏不可见对象是否存在。",
        "state_impact": "只影响当前页面；不会改变 Conversation、Task、Approval 或 Report。",
        "state_next_step": "返回工作台，或通过主导航进入已定义的产品面。",
        "primary_href": "/workspace", "primary_label": "返回工作台",
        "secondary_href": "/search", "secondary_label": "打开搜索",
    },
    "forbidden": {
        "active": "", "page_title": "没有权限", "page_eyebrow": "Policy 拒绝",
        "state": "forbidden", "state_label": "没有权限", "state_kicker": "403",
        "state_title": "当前身份不能访问这个范围",
        "state_copy": "Forge 会失败关闭且不披露资源是否存在。权限不能从 URL、Prompt 或请求体自行扩大。",
        "state_impact": "当前资源或 Action 不可见、不可执行；其他已授权页面不受影响。",
        "state_next_step": "返回有权访问的工作区；未来由合法 Membership/Policy 流程申请访问。",
        "primary_href": "/workspace", "primary_label": "返回工作台",
        "secondary_href": "/governance/policies", "secondary_label": "查看 Policy 边界",
    },
    "offline": {
        "active": "", "page_title": "依赖不可用", "page_eyebrow": "安全降级",
        "state": "offline", "state_label": "依赖不可用", "state_kicker": "Offline",
        "state_title": "部分产品能力暂时不可用",
        "state_copy": "页面会保留仍可读取的交付与数据资产，并明确标注受影响能力；高风险 Action 不会自动重放。",
        "state_impact": "依赖该 Runtime 的对话、任务或写入暂不可用；已发布的不可变交付可能仍可读取。",
        "state_next_step": "安全刷新当前页面；若依赖恢复，页面会重新从正式真相源读取。",
        "primary_href": "/workspace", "primary_label": "检查工作台",
        "secondary_href": "/manage", "secondary_label": "查看系统状态",
    },
}


def product_state_page(request: Request, state_key: str, status_code: int = 200):
    state = PRODUCT_STATE_PAGES[state_key]
    return templates.TemplateResponse(
        request,
        "product_state.html",
        dict(state),
        status_code=status_code,
    )


def product_surface_page(request: Request, surface_key: str):
    surface = PRODUCT_SURFACES.get(surface_key)
    if surface is None:
        return product_state_page(request, "not_found", 404)
    return templates.TemplateResponse(
        request,
        "product_capability.html",
        dict(surface),
    )
