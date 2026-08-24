import { activity, assets, reports, tasks, workspace } from "./data.js";

const app = document.getElementById("app");
const modalRoot = document.getElementById("modal-root");
const taskOverrides = new Map();
let prototypePanelOpen = false;
let toastTimer;

const statusMeta = {
  waiting_approval: ["等待审批", "amber"],
  needs_input: ["等待补充", "blue"],
  analyzing: ["分析中", "violet"],
  querying: ["查询中", "blue"],
  rendering: ["生成报告", "violet"],
  completed: ["已完成", "green"],
  failed: ["失败", "red"],
  ready: ["可查看", "green"],
  offline: ["依赖不可用", "gray"],
};

const navItems = [
  ["workspace", "工作台", "workspace", "M4 5h16v14H4z M9 5v14 M4 10h16"],
  ["new", "新建任务", "new", "M12 5v14M5 12h14"],
  ["tasks", "任务", "tasks", "M5 6h14M5 12h14M5 18h9"],
  ["reports", "报告", "reports", "M7 3h8l4 4v14H7z M15 3v5h4 M10 13h6M10 17h6"],
  ["data", "数据资产", "data", "M4 6c0-2 16-2 16 0s-16 2-16 0zm0 0v6c0 2 16 2 16 0V6m-16 6v6c0 2 16 2 16 0v-6"],
];

function icon(path, className = "nav-icon") {
  return `<svg class="${className}" fill="none" stroke="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.7" d="${path}"/></svg>`;
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>'"]/g, (character) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" })[character]);
}

function parseRoute() {
  const raw = (location.hash || "#/workspace").slice(1);
  const [pathname, queryString = ""] = raw.split("?");
  return { pathname: pathname || "/workspace", query: new URLSearchParams(queryString) };
}

function routeHref(pathname, query = {}) {
  const params = new URLSearchParams(Object.entries(query).filter(([, value]) => value));
  return `#${pathname}${params.size ? `?${params}` : ""}`;
}

function activeArea(pathname) {
  if (pathname.startsWith("/tasks")) return "tasks";
  if (pathname.startsWith("/reports")) return "reports";
  if (pathname.startsWith("/data")) return "data";
  if (pathname.startsWith("/admin")) return "admin";
  if (pathname === "/new") return "new";
  return "workspace";
}

function getTask(id) {
  const task = tasks.find((item) => item.id === id) ?? tasks[0];
  const override = taskOverrides.get(task.id);
  if (!override) return task;
  const meta = statusMeta[override.status] ?? [override.status, "gray"];
  return { ...task, ...override, statusLabel: meta[0] };
}

function badge(status, label) {
  const meta = statusMeta[status] ?? [label ?? status, "gray"];
  return `<span class="status-badge ${meta[1]}"><i></i>${escapeHtml(label ?? meta[0])}</span>`;
}

function pageContext(pathname) {
  if (pathname === "/workspace") return ["工作台", "当前需要处理的任务与最近交付"];
  if (pathname === "/new") return ["新建任务", "说明问题、范围和需要的交付物"];
  if (pathname === "/tasks") return ["任务", "查看所有渠道中的数据任务"];
  if (pathname.startsWith("/tasks/")) return ["任务详情", "查看状态、审批、结果和交付物"];
  if (pathname === "/reports") return ["报告", "查找已生成和正在生成的报告"];
  if (pathname.startsWith("/reports/")) return ["报告详情", "查看报告版本、来源和下载状态"];
  if (pathname.startsWith("/data")) return ["数据资产", "管理结构、指标、规则和发布草案"];
  if (pathname.startsWith("/admin")) return ["管理", "团队、审计和运行配置"];
  return ["未找到页面", "返回工作台继续"];
}

function shell(content, route) {
  const area = activeArea(route.pathname);
  const [title, description] = pageContext(route.pathname);
  return `
    <div class="prototype-notice" role="note">
      <span><b>交互原型</b> · 当前页面使用演示数据，不连接生产查询、审批或配置。</span>
      <button id="prototype-controls" type="button" aria-expanded="${prototypePanelOpen}">查看状态样本</button>
    </div>
    <div class="app-shell">
      <aside class="app-sidebar" aria-label="产品导航">
        <a class="product-mark" href="#/workspace" aria-label="Forge 工作台"><span>F</span><b>Forge</b></a>
        <nav class="primary-nav">
          <p>工作</p>
          ${navItems.slice(0, 4).map(([key, label, path, svg]) => `
            <a href="#/${path}" class="${area === key ? "active" : ""}" ${area === key ? 'aria-current="page"' : ""}>
              ${icon(svg)}<span>${label}</span>${key === "tasks" ? '<small class="nav-count">6</small>' : ""}
            </a>`).join("")}
          <p>资产</p>
          ${navItems.slice(4).map(([key, label, path, svg]) => `
            <a href="#/${path}" class="${area === key ? "active" : ""}" ${area === key ? 'aria-current="page"' : ""}>${icon(svg)}<span>${label}</span></a>`).join("")}
        </nav>
        <nav class="secondary-nav">
          <a href="#/admin" class="${area === "admin" ? "active" : ""}">${icon("M12 15.5A3.5 3.5 0 1012 8a3.5 3.5 0 000 7.5z M19.4 15a1.7 1.7 0 00.34 1.88l.05.05-2 3.46-.08-.02a1.7 1.7 0 00-1.82.22l-1.06.61a1.7 1.7 0 00-.83 1.69V23h-4v-.11a1.7 1.7 0 00-.83-1.69l-1.06-.61a1.7 1.7 0 00-1.82-.22l-.08.02-2-3.46.05-.05A1.7 1.7 0 004.6 15v-1.22a1.7 1.7 0 00-1-1.55L3.54 12l2-3.46.08.02a1.7 1.7 0 001.82-.22l1.06-.61A1.7 1.7 0 009.33 6V5.9h4V6a1.7 1.7 0 00.83 1.69l1.06.61a1.7 1.7 0 001.82.22l.08-.02 2 3.46-.05.05a1.7 1.7 0 00-.34 1.88z")}<span>管理</span></a>
          <div class="user-summary"><span>当</span><div><b>${workspace.user}</b><small>${workspace.name}</small></div></div>
        </nav>
      </aside>
      <section class="app-frame">
        <header class="app-header">
          <div><p>${escapeHtml(description)}</p><h1>${escapeHtml(title)}</h1></div>
          <div class="header-actions">
            <button class="icon-button" type="button" aria-label="通知，当前没有新通知">${icon("M18 8a6 6 0 10-12 0c0 7-3 7-3 8h18c0-1-3-1-3-8 M10 20h4", "")}</button>
            ${["workspace", "tasks"].includes(area) ? `<a class="primary-button compact" href="#/new">${icon("M12 5v14M5 12h14", "")}新建任务</a>` : ""}
          </div>
        </header>
        <main id="main-content" class="main-content" tabindex="-1">${content}</main>
      </section>
    </div>
    ${renderPrototypePanel(route)}
    <div id="toast" class="toast" role="status" aria-live="polite"></div>
  `;
}

function sectionHeader(title, description = "", action = "") {
  return `<div class="section-heading"><div><h2>${title}</h2>${description ? `<p>${description}</p>` : ""}</div>${action}</div>`;
}

function renderWorkspace() {
  const attention = tasks.filter((task) => ["waiting_approval", "needs_input", "failed"].includes(getTask(task.id).status));
  const ongoing = tasks.filter((task) => ["analyzing", "rendering"].includes(getTask(task.id).status));
  return `
    <div class="page-lead workspace-lead">
      <div><p class="eyebrow">2026 年 8 月 24 日</p><h2>待处理事项 <span>${attention.length}</span></h2><p>先处理审批和信息缺口，再查看进行中的分析与报告。</p></div>
    </div>
    <div class="workspace-layout">
      <section class="content-section attention-section">
        ${sectionHeader("待我处理", "审批、补充信息和失败恢复")}
        <div class="work-list">
          ${attention.map((task) => {
            const current = getTask(task.id);
            return `<a class="work-row" href="${routeHref(`/tasks/${task.id}`, { tab: task.status === "waiting_approval" ? "sql" : "overview" })}">
              <div class="work-main"><h3>${escapeHtml(task.title)}</h3><p>${escapeHtml(current.current)}</p></div>
              <div class="work-status">${badge(current.status)}</div>
              <time>${escapeHtml(task.updated)}</time><span class="row-arrow">→</span>
            </a>`;
          }).join("")}
        </div>
      </section>
      <aside class="workspace-side">
        <section class="side-section">
          ${sectionHeader("任务概况")}
          <dl class="metric-list"><div><dt>进行中</dt><dd>2</dd></div><div><dt>等待处理</dt><dd>3</dd></div><div><dt>本周完成</dt><dd>8</dd></div></dl>
        </section>
        <section class="side-section queue-breakdown">
          ${sectionHeader("待处理分布")}
          <p><span>SQL 审批</span><b>1</b></p><p><span>补充信息</span><b>1</b></p><p><span>失败恢复</span><b>1</b></p>
        </section>
      </aside>
    </div>
    <section class="content-section section-gap">
      ${sectionHeader("进行中的任务", "页面只显示真实阶段，不估算完成百分比", '<a class="text-link" href="#/tasks">查看全部任务</a>')}
      <div class="progress-list">
        ${ongoing.map((task) => `<a href="#/tasks/${task.id}" class="progress-row"><div><h3>${escapeHtml(task.title)}</h3><p>${escapeHtml(task.current)} · ${escapeHtml(task.updated)}</p></div>${badge(task.status)}<progress class="progress-track" aria-label="计划完成 ${task.progress}%" max="100" value="${task.progress}">${task.progress}%</progress><span>→</span></a>`).join("")}
      </div>
    </section>
    <section class="content-section section-gap">
      ${sectionHeader("最近报告", "已生成和正在生成的交付物", '<a class="text-link" href="#/reports">打开报告库</a>')}
      <div class="report-rows">${reports.slice(0, 3).map(reportRow).join("")}</div>
    </section>
  `;
}

function renderNewTask() {
  return `
    <div class="narrow-page">
      <div class="page-lead simple"><div><p class="eyebrow">新任务</p><h2>先说明要解决的问题</h2><p>不需要写 SQL。请补充时间范围、关键口径和希望得到的交付物。</p></div></div>
      <form id="new-task-form" class="task-brief-form">
        <section class="form-section">
          <div class="form-number">1</div>
          <div class="form-body"><label for="task-question">问题与使用场景</label><p>说明你想知道什么，以及结果将用于什么判断。</p><textarea id="task-question" required rows="5" placeholder="例如：比较本月各渠道支付销售额，找出主要差异，并生成一份可以分享的业务报告。"></textarea></div>
        </section>
        <section class="form-section">
          <div class="form-number">2</div>
          <div class="form-body"><label>需要的交付物</label><p>可以多选；SQL 仍会在执行前单独等待批准。</p><div class="choice-grid">
            <label><input type="checkbox" name="deliverable" value="result" checked><span><b>查询结果</b><small>数据表与单位</small></span></label>
            <label><input type="checkbox" name="deliverable" value="analysis" checked><span><b>分析</b><small>观察、判断与限制</small></span></label>
            <label><input type="checkbox" name="deliverable" value="report" checked><span><b>报告</b><small>HTML、PDF、PPTX</small></span></label>
          </div></div>
        </section>
        <section class="form-section">
          <div class="form-number">3</div>
          <div class="form-body"><label for="task-constraints">范围与限制（可选）</label><p>例如时间范围、数据口径、不可使用的字段或截止时间。</p><textarea id="task-constraints" rows="3" placeholder="例如：按自然月统计，只计算已支付订单。"></textarea></div>
        </section>
        <div class="form-footer"><p><b>演示操作：</b>提交后只会创建浏览器内的任务样本，不会查询数据库。</p><button class="primary-button" type="submit">创建演示任务 <span>→</span></button></div>
      </form>
    </div>
  `;
}

function renderTasks(route) {
  const selectedStatus = route.query.get("status") ?? "all";
  const rows = selectedStatus === "all" ? tasks : tasks.filter((task) => getTask(task.id).status === selectedStatus);
  const counts = { all: tasks.length, waiting_approval: 1, needs_input: 1, analyzing: 1, failed: 1, completed: 1 };
  return `
    <div class="page-lead"><div><p class="eyebrow">全部渠道</p><h2>${rows.length} 个任务</h2><p>按需要处理、进行中、完成和失败状态查找任务。</p></div><a class="primary-button" href="#/new">新建任务</a></div>
    <div class="filter-bar" role="toolbar" aria-label="任务筛选">
      ${[["all","全部"],["waiting_approval","等待审批"],["needs_input","等待补充"],["analyzing","进行中"],["failed","失败"],["completed","已完成"]].map(([value,label]) => `<a href="${routeHref("/tasks", { status: value === "all" ? "" : value })}" class="${selectedStatus === value ? "active" : ""}">${label}<span>${counts[value] ?? 0}</span></a>`).join("")}
      <label class="search-field">${icon("M21 21l-4.35-4.35M19 11a8 8 0 11-16 0 8 8 0 0116 0", "")}<input id="task-search" type="search" placeholder="搜索任务" aria-label="搜索任务"></label>
    </div>
    <section class="table-section">
      <table class="data-table task-table"><thead><tr><th>任务</th><th>状态</th><th>当前步骤</th><th>渠道</th><th>更新时间</th><th><span class="sr-only">打开</span></th></tr></thead>
      <tbody id="task-table-body">${rows.map(taskTableRow).join("")}</tbody></table>
      ${rows.length ? "" : renderEmpty("没有符合条件的任务", "调整筛选条件后重试。")}
    </section>
  `;
}

function taskTableRow(task) {
  const current = getTask(task.id);
  return `<tr data-search="${escapeHtml(`${task.title} ${task.question}`.toLowerCase())}"><td><a href="#/tasks/${task.id}"><b>${escapeHtml(task.title)}</b><small>${escapeHtml(task.question)}</small></a></td><td>${badge(current.status)}</td><td>${escapeHtml(current.current)}</td><td>${escapeHtml(task.channel)}</td><td>${escapeHtml(task.updated)}</td><td><a class="row-open" href="#/tasks/${task.id}" aria-label="打开 ${escapeHtml(task.title)}">→</a></td></tr>`;
}

function renderTaskDetail(route, id) {
  const task = getTask(id);
  const tab = route.query.get("tab") ?? "overview";
  const tabs = [["overview","概览"],["sql","数据与 SQL"],["analysis","分析"],["report","报告"],["activity","活动记录"]];
  return `
    <div class="object-header">
      <div class="object-title"><a class="back-link" href="#/tasks">← 返回任务</a><div><p>${escapeHtml(task.id)} · ${escapeHtml(task.channel)}</p><h2>${escapeHtml(task.title)}</h2></div></div>
      <div class="object-actions">${badge(task.status)}${task.status === "waiting_approval" ? `<a class="primary-button amber" href="${routeHref(`/tasks/${task.id}`, {tab:"sql"})}">审核 SQL</a>` : ""}</div>
    </div>
    ${task.status === "failed" ? `<div class="state-banner error"><div><b>任务在查询阶段停止</b><p>${escapeHtml(task.failure)}</p></div><button type="button" data-demo-action="retry">预览重试状态</button></div>` : ""}
    ${task.status === "needs_input" ? `<div class="state-banner info"><div><b>需要补充复购时间窗口</b><p>请选择“下单后 90 天内再次购买”，或说明其他范围。</p></div><button type="button" data-demo-action="input">预览补充表单</button></div>` : ""}
    <nav class="object-tabs" aria-label="任务详情">${tabs.map(([key,label]) => `<a class="${tab === key ? "active" : ""}" href="${routeHref(`/tasks/${task.id}`, {tab:key})}">${label}${key === "sql" && task.status === "waiting_approval" ? "<i></i>" : ""}</a>`).join("")}</nav>
    <div class="detail-layout">
      <section class="detail-main">${renderTaskTab(task, tab)}</section>
      <aside class="object-inspector">
        <h3>任务信息</h3>
        <dl><div><dt>状态</dt><dd>${escapeHtml(task.statusLabel)}</dd></div><div><dt>数据范围</dt><dd>${escapeHtml(task.scope)}</dd></div><div><dt>数据源</dt><dd>${escapeHtml(task.datasource ?? "演示数据源")}</dd></div><div><dt>创建时间</dt><dd>${escapeHtml(task.created ?? "2026-08-24")}</dd></div><div><dt>负责人</dt><dd>${escapeHtml(task.owner)}</dd></div></dl>
        <h3>当前限制</h3><p>这是交互原型。页面操作不会执行 SQL，也不会写入任务或审计记录。</p>
      </aside>
    </div>
  `;
}

function renderTaskTab(task, tab) {
  if (tab === "sql") return renderSqlTab(task);
  if (tab === "analysis") return renderAnalysisTab(task);
  if (tab === "report") return renderTaskReportTab(task);
  if (tab === "activity") return renderActivityTab(task);
  return renderOverviewTab(task);
}

function renderOverviewTab(task) {
  return `
    <section class="detail-section">
      ${sectionHeader("任务目标")}
      <p class="lead-copy">${escapeHtml(task.question)}</p>
    </section>
    <section class="detail-section">
      ${sectionHeader("计划与进度", "每一步完成后才会进入下一步")}
      <ol class="plan-list">${(task.plan ?? []).map(([title,status,detail], index) => `<li class="${status}"><span>${status === "completed" ? "✓" : index + 1}</span><div><h3>${escapeHtml(title)}</h3><p>${escapeHtml(detail)}</p></div><small>${stepLabel(status)}</small></li>`).join("")}</ol>
    </section>
    <section class="detail-section evidence-summary">
      ${sectionHeader("已有依据")}
      <dl><div><dt>问题范围</dt><dd>${escapeHtml(task.scope)}</dd></div><div><dt>数据来源</dt><dd>${escapeHtml(task.datasource ?? "待确认")}</dd></div><div><dt>尚未发生</dt><dd>${task.status === "waiting_approval" ? "SQL 尚未执行，分析与报告尚未生成" : "根据当前计划继续推进"}</dd></div></dl>
    </section>`;
}

function stepLabel(status) {
  return ({completed:"已完成",waiting_approval:"等待审批",needs_input:"等待补充",running:"进行中",failed:"失败",waiting:"未开始"})[status] ?? status;
}

function renderSqlTab(task) {
  if (!task.sql) return renderEmpty("尚无可审核 SQL", task.status === "needs_input" ? "补充业务口径后才会准备查询。" : "此任务当前没有查询步骤。");
  return `
    <section class="detail-section sql-review-head">
      <div><p class="eyebrow">执行前审核</p><h2>确认数据库将要执行的内容</h2><p>批准对象绑定当前 SQL、数据源和检查结果。修改需求后必须重新生成并审核。</p></div>${task.status === "waiting_approval" ? badge("waiting_approval") : badge(task.status)}
    </section>
    <section class="sql-scope"><dl><div><dt>数据源</dt><dd>${escapeHtml(task.datasource)}</dd></div><div><dt>范围</dt><dd>${escapeHtml(task.scope)}</dd></div><div><dt>返回上限</dt><dd>200 行</dd></div><div><dt>有效期</dt><dd>剩余 27 分钟</dd></div></dl></section>
    <section class="sql-block"><header><span>只读 SQL</span><button type="button" id="copy-sql">复制</button></header><pre><code>${escapeHtml(task.sql)}</code></pre></section>
    <section class="check-list"><h3>执行检查</h3>${task.checks.map((check) => `<p><span>✓</span>${escapeHtml(check)}</p>`).join("")}</section>
    <footer class="approval-footer"><div><b>演示审批</b><p>确认操作只改变当前浏览器中的任务样本。</p></div><button class="secondary-button" type="button" data-demo-action="reject">拒绝并修改需求</button><button class="primary-button amber" type="button" id="open-approval">批准只读执行</button></footer>
  `;
}

function renderAnalysisTab(task) {
  if (!["analyzing", "rendering", "completed"].includes(task.status)) return renderEmpty("分析尚未开始", "查询结果通过审核并执行后，这里才会显示有数据来源的观察和判断。", "查看任务计划", routeHref(`/tasks/${task.id}`, {tab:"overview"}));
  return `
    <section class="detail-section analysis-document">
      <div class="analysis-header"><div><p class="eyebrow">分析草稿</p><h2>当前数据支持三项观察</h2></div>${task.status === "analyzing" ? badge("analyzing") : badge("completed", "分析完成")}</div>
      <article><h3>观察 1 · 头部品类差距很小</h3><p>排名第一的品类销售额为 <strong>753.2K</strong>，第二名为 <strong>721.4K</strong>，相差 31.8K（4.4%）。</p><p class="source-line">数据来源：品类销售查询，第 1—2 行</p></article>
      <article><h3>观察 2 · 四月低于目标，五月恢复</h3><p>四月销售额低于目标 6.4%；五月和六月连续高于目标。当前只有两个月恢复数据，不能据此判断长期趋势。</p><p class="source-line">数据来源：月度销售查询，第 4—6 行</p></article>
      <article class="analysis-limit"><h3>限制</h3><p>现有结果不包含利润、库存和活动成本，只能说明销售额差异，不能证明具体增长原因。</p></article>
    </section>`;
}

function renderTaskReportTab(task) {
  const report = reports.find((item) => item.taskId === task.id) ?? reports[0];
  if (!task.reportId && !["rendering","completed"].includes(task.status)) return renderEmpty("报告尚未生成", "分析完成并通过报告 Contract 后，这里会出现报告版本和下载状态。", "查看分析状态", routeHref(`/tasks/${task.id}`, {tab:"analysis"}));
  return `<section class="detail-section">${sectionHeader("报告交付", "报告版本不会原地覆盖", `<a class="text-link" href="#/reports/${report.id}">打开报告详情</a>`)}<div class="report-delivery"><div><p class="eyebrow">${escapeHtml(report.period)}</p><h3>${escapeHtml(report.title)}</h3><p>${escapeHtml(report.summary)}</p></div>${badge(report.status, report.statusLabel)}<dl><div><dt>版本</dt><dd>${escapeHtml(report.revision)}</dd></div><div><dt>可用格式</dt><dd>${escapeHtml(report.formats.join(" · "))}</dd></div><div><dt>质量</dt><dd>${escapeHtml(report.quality)}</dd></div></dl></div></section>`;
}

function renderActivityTab() {
  return `<section class="detail-section">${sectionHeader("活动记录", "按发生顺序展示，不包含模型隐藏推理或敏感信息")}<ol class="activity-list">${activity.map(([time,title,detail]) => `<li><time>${time}</time><span></span><div><h3>${title}</h3><p>${detail}</p></div></li>`).join("")}</ol></section>`;
}

function renderReports(route) {
  const selected = route.query.get("status") ?? "all";
  const rows = selected === "all" ? reports : reports.filter((report) => report.status === selected);
  return `
    <div class="page-lead"><div><p class="eyebrow">Report Library</p><h2>${rows.length} 份报告</h2><p>查看报告版本、数据期间、生成状态和可用格式。</p></div><span class="disabled-action" aria-disabled="true" title="可复用报告将在 H6 实现">保存的报告定义 · 尚未开放</span></div>
    <div class="filter-bar"><a class="${selected === "all" ? "active" : ""}" href="#/reports">全部<span>${reports.length}</span></a><a class="${selected === "ready" ? "active" : ""}" href="${routeHref("/reports",{status:"ready"})}">可查看<span>2</span></a><a class="${selected === "rendering" ? "active" : ""}" href="${routeHref("/reports",{status:"rendering"})}">生成中<span>1</span></a></div>
    <section class="content-section report-library">${rows.map(reportRow).join("")}${rows.length ? "" : renderEmpty("没有符合条件的报告", "调整筛选条件后重试。")}</section>
  `;
}

function reportRow(report) {
  return `<a href="#/reports/${report.id}" class="report-row"><div class="report-file">R</div><div><h3>${escapeHtml(report.title)}</h3><p>${escapeHtml(report.period)} · ${escapeHtml(report.formats.join(" / "))}</p></div>${badge(report.status, report.statusLabel)}<time>${escapeHtml(report.updated)}</time><span>→</span></a>`;
}

function renderReportDetail(id) {
  const report = reports.find((item) => item.id === id) ?? reports[0];
  return `
    <div class="object-header report-object"><div class="object-title"><a class="back-link" href="#/reports">← 返回报告库</a><div><p>${escapeHtml(report.period)} · Revision ${escapeHtml(report.revision)}</p><h2>${escapeHtml(report.title)}</h2></div></div><div class="object-actions">${badge(report.status, report.statusLabel)}<button class="secondary-button" type="button" data-demo-action="share" ${report.status !== "ready" ? "disabled" : ""}>分享设置</button><button class="primary-button" type="button" data-demo-action="open-report" ${report.status !== "ready" ? "disabled" : ""}>打开报告</button></div></div>
    <section class="report-delivery-bar"><dl><div><dt>HTML</dt><dd>可查看</dd></div><div><dt>PDF</dt><dd>${report.formats.includes("PDF") ? "可下载" : "生成中"}</dd></div><div><dt>PPTX</dt><dd>${report.formats.includes("PPTX") ? "可下载" : "生成中"}</dd></div><div><dt>来源任务</dt><dd><a href="#/tasks/${report.taskId}">${escapeHtml(report.taskId)}</a></dd></div></dl><p>演示报告只用于验证产品框架。</p></section>
    <div class="report-detail-layout">
      <article class="report-paper">
        <header><p>报告正文预览</p><dl><div><dt>数据期间</dt><dd>${escapeHtml(report.period)}</dd></div><div><dt>数据质量</dt><dd>${escapeHtml(report.quality)}</dd></div><div><dt>版本</dt><dd>${escapeHtml(report.revision)}</dd></div></dl></header>
        <section><h2>执行摘要</h2><p>${escapeHtml(report.summary)}</p></section>
        <section><h2>关键结论</h2><ol><li><span>01</span><p>排名靠前的两个品类相差 31.8K，现有差距不足以支持只向第一名配置资源。</p></li><li><span>02</span><p>四月低于目标，五月和六月恢复；当前数据不能证明恢复由什么具体动作造成。</p></li><li><span>03</span><p>四月至六月新增 174K，其中直营增加 87K，占 50%。</p></li></ol></section>
        <aside><b>当前限制</b><p>报告不包含利润、库存和活动成本，只说明销售额差异。</p></aside>
      </article>
    </div>
  `;
}

function renderData(route) {
  const tab = route.query.get("tab") ?? "overview";
  const tabs = [["overview","概览"],["schema","数据结构"],["metrics","指标与语义"],["drafts","草案与发布"]];
  return `
    <div class="page-lead"><div><p class="eyebrow">Registry revision 42</p><h2>数据资产</h2><p>结构、指标、规则和草案从同一 Registry 投影。</p></div><button class="secondary-button" type="button" data-demo-action="sync">同步结构（演示）</button></div>
    <nav class="object-tabs data-tabs">${tabs.map(([key,label]) => `<a class="${tab === key ? "active" : ""}" href="${routeHref("/data",{tab:key === "overview" ? "" : key})}">${label}</a>`).join("")}</nav>
    ${renderDataTab(tab)}
  `;
}

function renderDataTab(tab) {
  if (tab === "schema") return `<section class="table-section">${sectionHeader("数据结构", "最近同步的表和字段", '<span class="subtle-label">24 张表</span>')}<table class="data-table"><thead><tr><th>物理表</th><th>业务名称</th><th>规模</th><th>更新时间</th><th></th></tr></thead><tbody>${assets.tables.map((row) => `<tr><td><code>${row[0]}</code></td><td>${row[1]}</td><td>${row[2]}</td><td>${row[3]}</td><td><button class="row-open" data-demo-action="inspect">→</button></td></tr>`).join("")}</tbody></table></section>`;
  if (tab === "metrics") return `<section class="table-section">${sectionHeader("指标与语义", "业务定义、公式和当前状态", '<button class="text-link button-link" data-demo-action="new-metric">新增指标（演示）</button>')}<table class="data-table"><thead><tr><th>指标</th><th>定义</th><th>粒度</th><th>状态</th></tr></thead><tbody>${assets.metrics.map((row) => `<tr><td><b>${row[0]}</b></td><td><code>${row[1]}</code></td><td>${row[2]}</td><td>${badge(row[3] === "已发布" ? "ready" : "needs_input",row[3])}</td></tr>`).join("")}</tbody></table></section>`;
  if (tab === "drafts") return `<section class="table-section">${sectionHeader("草案与发布", "草案经过差异检查和人工审核后才会成为新版本")}<table class="data-table"><thead><tr><th>草案</th><th>内容</th><th>状态</th><th>更新时间</th><th></th></tr></thead><tbody>${assets.drafts.map((row) => `<tr><td><code>${row[0]}</code></td><td>${row[1]}</td><td>${badge(row[2] === "等待审核" ? "waiting_approval" : "needs_input",row[2])}</td><td>${row[3]}</td><td><button class="row-open" data-demo-action="draft">→</button></td></tr>`).join("")}</tbody></table></section>`;
  return `
    <div class="asset-summary"><dl><div><dt>数据表</dt><dd>${assets.counts.tables}</dd></div><div><dt>业务指标</dt><dd>${assets.counts.metrics}</dd></div><div><dt>语义规则</dt><dd>${assets.counts.rules}</dd></div><div><dt>待处理草案</dt><dd>${assets.counts.drafts}</dd></div></dl></div>
    <div class="data-overview-grid"><section class="content-section">${sectionHeader("最近变更", "当前 Registry 的结构和语义变化")}<div class="change-list"><p><time>19:30</time><span>新增渠道字段约定草案</span><b>等待审核</b></p><p><time>15:10</time><span>确认 orders → customers 关系</span><b>需要补充</b></p><p><time>昨天</time><span>发布支付销售额指标修订</span><b>已发布</b></p></div></section><aside class="workspace-side"><section class="side-section"><h2>发布状态</h2><p class="healthy-line"><i></i>Revision 42 正在使用</p><p>2 个草案尚未影响查询或报告。</p></section></aside></div>`;
}

function renderAdmin() {
  const groups = [
    ["组织与审计", [["团队与成员","当前为单团队演示配置","可查看"],["查询审计","审批、执行和失败记录","可查看"]]],
    ["运行配置", [["模型与 Skills","阶段绑定和质量门禁","可查看"],["数据源","只读连接与执行上限","可查看"],["渠道","Web、飞书与钉钉连接状态","可查看"]]],
    ["诊断", [["任务运行记录","内部阶段与恢复信息","管理员"],["Memory 诊断","会话和候选知识记录","管理员"],["架构全景","当前职责和部署关系","管理员"]]],
  ];
  return `
    <div class="page-lead"><div><h2>系统与组织设置</h2><p>团队、审计、运行配置和诊断入口集中在这里。</p></div></div>
    <div class="admin-groups">${groups.map(([title,items]) => `<section><h2>${title}</h2>${items.map(([name,description,access]) => `<button type="button" data-demo-action="admin"><span><b>${name}</b><small>${description}</small></span><em>${access}</em><i>→</i></button>`).join("")}</section>`).join("")}</div>
    <aside class="state-banner warning"><div><b>企业多用户授权尚未开放</b><p>当前仅提供单用户演示配置，不能在这里开放跨团队访问。</p></div></aside>
  `;
}

function renderNotFound() {
  return renderEmpty("页面不存在", "当前原型没有这个页面。", "返回工作台", "#/workspace");
}

function renderEmpty(title, description, actionLabel = "", href = "") {
  return `<div class="empty-state"><span>◇</span><h2>${escapeHtml(title)}</h2><p>${escapeHtml(description)}</p>${actionLabel ? `<a class="secondary-button" href="${href}">${escapeHtml(actionLabel)}</a>` : ""}</div>`;
}

function renderPrototypePanel(route) {
  const currentTask = route.pathname.startsWith("/tasks/") ? getTask(route.pathname.split("/")[2]) : null;
  return `<button class="prototype-backdrop ${prototypePanelOpen ? "open" : ""}" type="button" aria-label="关闭状态样本"></button><aside class="prototype-panel ${prototypePanelOpen ? "open" : ""}" aria-label="原型状态样本"><header><div><p>原型控制</p><h2>查看页面状态</h2></div><button id="close-prototype-panel" type="button" aria-label="关闭">×</button></header><section><h3>快速跳转</h3><a href="#/workspace">工作台</a><a href="#/tasks/tr_sales_channel?tab=sql">等待 SQL 审批</a><a href="#/tasks/tr_repurchase_definition">等待补充</a><a href="#/tasks/tr_inventory_failure">失败任务</a><a href="#/reports/rp_category_h1">报告详情</a></section>${currentTask ? `<section><h3>当前任务状态（演示）</h3><div class="state-buttons">${["waiting_approval","needs_input","analyzing","completed","failed","offline"].map((status) => `<button type="button" data-prototype-status="${status}" data-task-id="${currentTask.id}" class="${currentTask.status === status ? "active" : ""}">${statusMeta[status][0]}</button>`).join("")}</div></section>` : ""}<footer>状态切换只存在于浏览器内存，刷新后恢复固定 fixture。</footer></aside>`;
}

function renderRoute() {
  const route = parseRoute();
  let content;
  if (route.pathname === "/workspace") content = renderWorkspace();
  else if (route.pathname === "/new") content = renderNewTask();
  else if (route.pathname === "/tasks") content = renderTasks(route);
  else if (route.pathname.startsWith("/tasks/")) content = renderTaskDetail(route, route.pathname.split("/")[2]);
  else if (route.pathname === "/reports") content = renderReports(route);
  else if (route.pathname.startsWith("/reports/")) content = renderReportDetail(route.pathname.split("/")[2]);
  else if (route.pathname.startsWith("/data")) content = renderData(route);
  else if (route.pathname.startsWith("/admin")) content = renderAdmin();
  else content = renderNotFound();
  app.innerHTML = shell(content, route);
  bindInteractions(route);
  document.body.dataset.ready = "true";
  document.title = `${pageContext(route.pathname)[0]} · Forge Prototype`;
}

function bindInteractions(route) {
  document.getElementById("prototype-controls")?.addEventListener("click", () => { prototypePanelOpen = true; renderRoute(); });
  document.getElementById("close-prototype-panel")?.addEventListener("click", () => { prototypePanelOpen = false; renderRoute(); });
  document.querySelector(".prototype-backdrop")?.addEventListener("click", () => { prototypePanelOpen = false; renderRoute(); });
  document.querySelectorAll(".prototype-panel a").forEach((link) => link.addEventListener("click", () => { prototypePanelOpen = false; }));
  document.querySelectorAll("[data-prototype-status]").forEach((button) => button.addEventListener("click", () => {
    const status = button.dataset.prototypeStatus;
    taskOverrides.set(button.dataset.taskId, { status, current: statusMeta[status][0] });
    prototypePanelOpen = false;
    renderRoute();
    showToast(`已切换为“${statusMeta[status][0]}”演示状态`);
  }));
  document.getElementById("new-task-form")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const question = document.getElementById("task-question").value.trim();
    if (!question) return;
    taskOverrides.set("tr_repurchase_definition", { title: question, question, status: "needs_input", current: "确认时间范围和指标口径" });
    location.hash = "/tasks/tr_repurchase_definition";
    setTimeout(() => showToast("演示任务已创建，未写入生产 Store"), 40);
  });
  document.getElementById("task-search")?.addEventListener("input", (event) => {
    const value = event.target.value.trim().toLowerCase();
    document.querySelectorAll("#task-table-body tr").forEach((row) => { row.hidden = !row.dataset.search.includes(value); });
  });
  document.getElementById("copy-sql")?.addEventListener("click", async () => {
    const sql = document.querySelector(".sql-block code")?.textContent ?? "";
    await navigator.clipboard.writeText(sql);
    showToast("SQL 已复制");
  });
  document.getElementById("open-approval")?.addEventListener("click", () => openApprovalDialog(route.pathname.split("/")[2]));
  document.querySelectorAll("[data-demo-action]").forEach((button) => button.addEventListener("click", () => handleDemoAction(button.dataset.demoAction, route)));
}

function handleDemoAction(action, route) {
  const messages = {
    reject: "演示：将返回需求修改状态，不会提交真实拒绝记录。",
    retry: "演示：重试入口需要生产权限与幂等检查。",
    input: "演示：这里将打开仅包含必要字段的补充表单。",
    share: "演示：分享设置需要真实报告权限，本原型不会创建链接。",
    open_report: "演示报告详情已在当前页面展示。",
    sync: "演示：结构同步不会连接数据库。",
    inspect: "演示：生产版将在侧栏打开字段详情。",
    new_metric: "演示：生产版将创建 Registry 草案，而不是直接发布。",
    draft: "演示：生产版将打开草案差异与审核记录。",
    admin: "此入口将在 W3C 接入现有管理页面。",
  };
  showToast(messages[action] ?? "这是原型操作，不会产生生产副作用。", true);
}

function openApprovalDialog(taskId) {
  const task = getTask(taskId);
  modalRoot.innerHTML = `<div class="modal-backdrop" data-close-modal></div><dialog open class="approval-dialog" aria-labelledby="approval-title"><header><div><p>演示审批</p><h2 id="approval-title">确认批准这次只读查询</h2></div><button type="button" data-close-modal aria-label="关闭">×</button></header><div class="modal-body"><dl class="approval-object"><div><dt>任务</dt><dd>${escapeHtml(task.title)}</dd></div><div><dt>数据源</dt><dd>${escapeHtml(task.datasource)}</dd></div><div><dt>数据范围</dt><dd>${escapeHtml(task.scope)}</dd></div><div><dt>系统限制</dt><dd>只读；30 秒超时；最多返回 200 行</dd></div></dl><div class="approval-sql"><b>待执行 SQL</b><pre><code>${escapeHtml(task.sql)}</code></pre></div><div class="approval-checks"><b>4 项执行检查已通过</b><p>${task.checks.map(escapeHtml).join(" · ")}</p></div><aside class="demo-operation-note"><b>本次确认只用于体验页面状态</b><p>不会操作真实数据，也不会生成审批或审计记录。</p></aside><label class="approval-confirm"><input id="demo-confirm" type="checkbox">我理解这是演示操作</label></div><footer><button class="secondary-button" type="button" data-close-modal>取消</button><button class="primary-button amber" id="confirm-demo-approval" type="button" disabled>批准查询（演示）</button></footer></dialog>`;
  const close = () => { modalRoot.innerHTML = ""; };
  modalRoot.querySelectorAll("[data-close-modal]").forEach((element) => element.addEventListener("click", close));
  document.getElementById("demo-confirm").addEventListener("change", (event) => { document.getElementById("confirm-demo-approval").disabled = !event.target.checked; });
  document.getElementById("confirm-demo-approval").addEventListener("click", () => {
    taskOverrides.set(taskId, { status: "querying", current: "正在执行只读查询（演示）" });
    close();
    renderRoute();
    showToast("已进入“查询中”演示状态；未连接数据库");
  });
}

function showToast(message, persistent = false) {
  clearTimeout(toastTimer);
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = message;
  toast.classList.add("visible");
  if (!persistent) toastTimer = setTimeout(() => toast.classList.remove("visible"), 3200);
}

window.addEventListener("hashchange", () => { prototypePanelOpen = false; renderRoute(); requestAnimationFrame(() => document.getElementById("main-content")?.focus({ preventScroll: true })); });
document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") return;
  if (modalRoot.innerHTML) modalRoot.innerHTML = "";
  else if (prototypePanelOpen) { prototypePanelOpen = false; renderRoute(); }
});

renderRoute();
