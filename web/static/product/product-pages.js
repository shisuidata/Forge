import { announce, productStateLabel, usageStatusLabel } from "./product-shell.js?v=3";

const page = document.querySelector('[data-product-page]');

async function api(url, options = {}) {
  let response;
  try {
    response = await fetch(url, {
      cache: 'no-store', credentials: 'same-origin', ...options,
      headers: { 'content-type': 'application/json', ...(options.headers || {}) },
    });
  } catch {
    const error = new Error('连接中断，无法确认服务是否已接收。');
    error.deliveryUnknown = true;
    throw error;
  }
  let body;
  try {
    body = await response.json();
    if (!body || typeof body !== "object" || Array.isArray(body)) throw new Error("invalid response");
  } catch {
    const error = new Error('响应不可读取，操作结果未知。');
    error.deliveryUnknown = true;
    error.requestId = response.headers.get('X-Request-ID');
    throw error;
  }
  if (!response.ok) {
    const error = new Error(body?.error || body?.detail?.code || '请求失败');
    error.status = response.status;
    error.code = body?.code || body?.detail?.code;
    error.requestId = body?.request_id || response.headers.get('X-Request-ID');
    error.deliveryUnknown = response.status >= 500;
    throw error;
  }
  return body;
}

// Pending transport identity only: Task state always comes from Pi projections.
const pendingCommands = new Map();
function pendingCommand(key, payload, prefix = 'web_message') {
  const signature = JSON.stringify(payload);
  let command = pendingCommands.get(key);
  try { command ||= JSON.parse(sessionStorage.getItem('forge.pending.' + key) || 'null'); } catch { /* Storage may be disabled. */ }
  if (!command || command.signature !== signature) command = { signature, id: messageId(prefix) };
  pendingCommands.set(key, command);
  try { sessionStorage.setItem('forge.pending.' + key, JSON.stringify(command)); } catch { /* In-memory retry remains available. */ }
  return command;
}
function acknowledgeCommand(key, command) {
  if (pendingCommands.get(key)?.id !== command.id) return;
  pendingCommands.delete(key);
  try { sessionStorage.removeItem('forge.pending.' + key); } catch { /* No persisted state. */ }
}
function diagnosticMessage(error) {
  return [error.message, error.code, error.requestId && ('Request ' + error.requestId)].filter(Boolean).join(' · ');
}

function node(tag, attributes = {}, children = []) {
  const element = document.createElement(tag);
  for (const [key, value] of Object.entries(attributes)) {
    if (value === null || value === undefined) continue;
    if (key === 'class') element.className = value;
    else if (key === 'text') element.textContent = String(value);
    else if (key === 'dataset') Object.assign(element.dataset, value);
    else if (key === 'disabled') element.disabled = Boolean(value);
    else element.setAttribute(key, String(value));
  }
  for (const child of Array.isArray(children) ? children : [children]) {
    if (child instanceof Node) element.append(child);
    else if (child !== null && child !== undefined) element.append(document.createTextNode(String(child)));
  }
  return element;
}

function safeHref(value) {
  if (typeof value !== 'string') return null;
  if (value.startsWith('/') && !value.startsWith('//') && !value.includes('\\')) return value;
  try {
    const parsed = new URL(value, location.origin);
    if (parsed.origin === location.origin) return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    if (parsed.protocol === 'https:') return parsed.href;
  } catch { return null; }
  return null;
}

function appendInline(target, text) {
  const pattern = /(\[[^\]]+\]\([^)]+\)|`[^`]+`)/g;
  let offset = 0;
  for (const match of text.matchAll(pattern)) {
    target.append(document.createTextNode(text.slice(offset, match.index)));
    const token = match[0];
    if (token.startsWith('`')) {
      target.append(node('code', { text: token.slice(1, -1) }));
    } else {
      const parsed = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(token);
      const href = safeHref(parsed?.[2]);
      if (parsed && href) {
        const link = node('a', { class: 'evidence-link', href, text: parsed[1] });
        if (href.startsWith('https://')) {
          link.target = '_blank';
          link.rel = 'noopener noreferrer';
        }
        target.append(link);
      } else target.append(document.createTextNode(token));
    }
    offset = (match.index || 0) + token.length;
  }
  target.append(document.createTextNode(text.slice(offset)));
}

function renderMarkdown(target, markdown) {
  target.replaceChildren();
  const lines = String(markdown || '').split('\n');
  let code = null;
  let list = null;
  for (const raw of lines) {
    if (raw.startsWith('```')) {
      if (code) { target.append(code); code = null; }
      else code = node('pre', {}, [node('code')]);
      list = null;
      continue;
    }
    if (code) {
      code.firstChild.textContent += `${raw}\n`;
      continue;
    }
    const heading = /^(#{2,3})\s+(.+)$/.exec(raw);
    const bullet = /^[-*]\s+(.+)$/.exec(raw);
    const ordered = /^\d+\.\s+(.+)$/.exec(raw);
    const quote = /^>\s?(.*)$/.exec(raw);
    if (heading) {
      list = null;
      const h = node(heading[1].length === 2 ? 'h2' : 'h3');
      appendInline(h, heading[2]); target.append(h);
    } else if (bullet || ordered) {
      const tag = ordered ? 'ol' : 'ul';
      if (!list || list.tagName.toLowerCase() !== tag) { list = node(tag); target.append(list); }
      const item = node('li'); appendInline(item, (bullet || ordered)[1]); list.append(item);
    } else if (quote) {
      list = null;
      const block = node('blockquote'); appendInline(block, quote[1]); target.append(block);
    } else if (raw.trim()) {
      list = null;
      const p = node('p'); appendInline(p, raw); target.append(p);
    } else list = null;
  }
  if (code) target.append(code);
}

function badge(state) {
  return node('span', { class: 'status', 'data-state': state, text: productStateLabel(state) });
}

function formatDate(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '时间未知' : new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false,
  }).format(date);
}

function stateBox(title, copy, action = null) {
  const children = [node('div', { class: 'state-symbol', text: '◇' }), node('h3', { text: title }), node('p', { text: copy })];
  if (action) children.push(action);
  return node('div', { class: 'state-box' }, children);
}

function notice(title, copy, state = 'partial') {
  return node('div', { class: 'notice', 'data-state': state }, [
    node('div', {}, [node('p', { class: 'notice-title', text: title }), node('p', { class: 'notice-copy', text: copy })]),
  ]);
}

function itemList(items) {
  const list = node('ul', { class: 'item-list' });
  for (const item of items) {
    const href = safeHref(item.href) || '#';
    list.append(node('li', { class: 'item-row' }, [
      node('a', { class: 'item-link', href }, [
        node('p', { class: 'item-title', text: item.title }),
        node('div', { class: 'item-meta' }, [formatDate(item.updated_at), item.reason || '']),
      ]),
      badge(item.state || item.display_state),
    ]));
  }
  return list;
}

function renderError(container, error, retry) {
  container.replaceChildren(stateBox(
    error?.status === 401 ? '需要重新登录' : '当前内容不可用',
    error ? diagnosticMessage(error) : "读取失败，请稍后重试。",
    node('button', { class: 'button', type: 'button', text: '重试' }),
  ));
  container.querySelector('button')?.addEventListener('click', retry);
}

async function loadWorkspace() {
  const root = page;
  const stats = root.querySelector('[data-workspace-stats]');
  const attention = root.querySelector('[data-workspace-attention]');
  const running = root.querySelector('[data-workspace-running]');
  const reports = root.querySelector('[data-workspace-reports]');
  const dependencies = root.querySelector('[data-workspace-dependencies]');
  const notices = root.querySelector('[data-page-notices]');
  try {
    const data = await api('/api/product/workspace');
    const statData = [
      ['需要补充', data.counts.needs_input], ['等待确认', data.counts.waiting_decision],
      ['进行中', data.counts.running], ['未完成', data.counts.failed],
    ];
    stats.replaceChildren(...statData.map(([label, value]) => node('div', { class: 'stat' }, [
      node('p', { class: 'stat-label', text: label }), node('p', { class: 'stat-value', text: value }),
      node('p', { class: 'stat-note', text: '真实任务数量' }),
    ])));
    const needs = [...data.needs_input, ...data.waiting_decision];
    attention.replaceChildren(needs.length ? itemList(needs) : stateBox('暂无待办', '当前没有需要补充或确认的任务。'));
    running.replaceChildren(data.running.length ? itemList(data.running) : stateBox('暂无进行中任务', '可以从对话发起新的数据任务。'));
    reports.replaceChildren(data.recent_reports.length ? itemList(data.recent_reports) : stateBox('暂无报告', '完成分析并生成报告后会出现在这里。'));
    dependencies.replaceChildren(data.dependencies.length ? itemList(data.dependencies) : notice('依赖正常', '当前 Product Spine 依赖均可读取。', 'ready'));
    notices.replaceChildren();
    if (data.projection_meta.availability !== 'ready') notices.append(notice('工作台部分可用', '部分依赖或列表达到读取边界；已显示可用内容和影响范围。'));
    announce('工作台已更新');
  } catch (error) {
    for (const container of [attention, running, reports, dependencies]) renderError(container, error, loadWorkspace);
  }
}

let chatConversation = null;
let chatPoll = null;
let conversationEpoch = 0;
let taskPanelEpoch = 0;
let renderedTaskId = null;
let renderedTaskFingerprint = null;
const taskPanel = page?.querySelector('[data-conversation-task-panel]');
const taskPanelBody = page?.querySelector('[data-task-panel-body]');
const taskPanelToggle = page?.querySelector('[data-task-panel-toggle]');
const taskPanelClose = page?.querySelector('[data-task-panel-close]');
const taskPanelBackdrop = document.querySelector('[data-task-panel-backdrop]');

function setConversationTaskPanel(open) {
  if (!(taskPanel instanceof HTMLElement)) return;
  taskPanel.dataset.open = String(open);
  taskPanelToggle?.setAttribute('aria-expanded', String(open));
  if (taskPanelBackdrop instanceof HTMLButtonElement) taskPanelBackdrop.hidden = !open;
  const drawer = window.matchMedia('(max-width: 72rem)').matches;
  document.documentElement.style.overflow = drawer && open ? 'hidden' : '';
  if (drawer && open) taskPanelClose?.focus();
  if (drawer && !open) taskPanelToggle?.focus();
}

function resetConversationTaskPanel(copy = '当前对话还没有关联任务。') {
  taskPanelEpoch += 1;
  renderedTaskId = null;
  renderedTaskFingerprint = null;
  taskPanel?.removeAttribute('data-refresh');
  if (taskPanelBody instanceof HTMLElement) {
    taskPanelBody.replaceChildren(stateBox('暂无当前任务', copy));
  }
}

function randomToken() {
  const bytes = new Uint8Array(16);
  globalThis.crypto.getRandomValues(bytes);
  return Array.from(bytes, (value) => value.toString(16).padStart(2, '0')).join('');
}

function conversationId() {
  return `web_conv_${randomToken()}`;
}
function messageId(prefix = 'web_message') {
  return `${prefix}_${randomToken()}`;
}

async function loadConversationList() {
  const list = page.querySelector('[data-conversation-list]');
  try {
    const data = await api('/api/product/conversations?limit=50');
    if (!data.conversations.length) {
      list.replaceChildren(stateBox('暂无历史对话', '发送第一条消息后会保存在真实任务链中。'));
      return;
    }
    list.replaceChildren(...data.conversations.map((item) => {
      const link = node('a', {
        class: `conversation-link${item.conversation_id === chatConversation ? ' is-active' : ''}`,
        href: item.href,
        dataset: { conversationId: item.conversation_id },
      }, [node('h3', { text: item.title }), node('p', { text: item.latest_message_preview }), node('footer', {}, [formatDate(item.updated_at), badge(item.display_state)])]);
      link.addEventListener('click', async (event) => {
        event.preventDefault();
        const conversation = await selectConversation(item.conversation_id, true);
        if (conversationNeedsPolling(conversation)) resumeConversationPolling();
      });
      return link;
    }));
  } catch (error) { renderError(list, error, loadConversationList); }
}

function actionButton(action, task, review, refresh) {
  const disabled = action.availability !== "enabled";
  const button = node('button', {
    class: action.action_type === 'cancel_task' ? 'button button-danger' : 'button button-primary',
    type: 'button', text: action.label, disabled,
    title: disabled ? (action.reason_code || "当前操作不可用") : "",
  });
  if (!disabled) button.addEventListener('click', async () => {
    let currentReview = review;
    if (action.action_type === 'approve_query' && !currentReview) {
      try { currentReview = (await api(`/api/product/tasks/${encodeURIComponent(task.task_run_id)}`)).detail.review_request; }
      catch (error) { announce(`审核材料不可用：${error.message}`); return; }
    }
    openActionDialog(action, task, currentReview, refresh);
  });
  return button;
}

function taskPanelProjectionFingerprint(detail) {
  const task = detail.task ?? {};
  return JSON.stringify({
    attempts: detail.attempts,
    task: [task.task_run_id, task.title, task.status, task.display_state],
    plan: detail.plan?.steps?.map((step) => [
      step.step_id, step.title, step.capability, step.required, step.status,
    ]) ?? null,
    actions: detail.actions?.map((action) => [
      action.action_type, action.label, action.availability,
      action.reason_code, action.requires_confirmation,
    ]) ?? [],
    artifacts: detail.artifacts?.map((artifact) => [
      artifact.artifact_id, artifact.artifact_type, artifact.title,
      artifact.state, artifact.evidence_refs?.length ?? 0,
    ]) ?? [],
    activity: detail.activity?.map((item) => [
      item.sequence, item.title, item.state, item.created_at,
    ]) ?? [],
    review: detail.review_request
      ? [
          detail.review_request.query_run_id,
          detail.review_request.sql_hash,
          detail.review_request.assurance_report_hash,
          detail.review_request.expires_at,
        ]
      : null,
  });
}


function attemptList(attempts = []) {
  return node('section', { class: 'task-panel-section', 'data-attempt-diagnostics': '' }, [
    node('h3', { text: '阶段诊断' }),
    ...attempts.slice().reverse().map((attempt) => node('div', { class: 'notice', 'data-state': attempt.safe_error ? 'failed' : 'ready' }, [
      node('div', {}, [
        node('p', { class: 'notice-title', text: attempt.stage + ' · ' + attempt.status }),
        node('p', { class: 'notice-copy', text: attempt.attempt_id + (attempt.request_id ? ' · Request ' + attempt.request_id : '') }),
        node('p', { class: 'notice-copy', text: 'Model ' + (attempt.model_revision || '版本未记录') + ' · Policy ' + (attempt.skill_policy_version ?? '未记录') }),
        node('p', { class: 'notice-copy', text: usageStatusLabel(attempt.usage_status) }),
        attempt.safe_error ? node('p', { class: 'notice-copy', text: typeof attempt.safe_error === 'string' ? attempt.safe_error : attempt.safe_error.message }) : null,
      ]),
    ])),
    !attempts.length ? node('p', { class: 'muted-copy', text: '尚无阶段记录；不能据此推断用量为零。' }) : null,
  ]);
}


function renderConversationTaskPanel(detail) {
  if (!(taskPanelBody instanceof HTMLElement)) return;
  const task = detail.task;
  const steps = detail.plan?.steps ?? [];
  const completedSteps = steps.filter((step) => step.status === 'completed').length;
  const summary = node('section', { class: 'task-panel-summary' }, [
    badge(task.display_state),
    node('h3', { text: task.title || '数据任务' }),
    node('p', { text: steps.length ? `计划进度 ${completedSteps}/${steps.length}` : '尚未生成执行计划' }),
    node('a', { class: 'button', href: `/tasks/${task.task_run_id}`, text: '打开任务详情' }),
  ]);

  const plan = node('section', { class: 'task-panel-section' }, [
    node('header', {}, [node('h3', { text: '计划与进度' }), node('span', { text: `${completedSteps}/${steps.length}` })]),
    steps.length
      ? node('ol', { class: 'task-panel-plan' }, steps.slice(0, 12).map((step, index) =>
        node('li', { class: 'task-panel-step', dataset: { status: step.status } }, [
          node('span', { class: 'task-panel-step-marker', text: step.status === 'completed' ? '✓' : index + 1 }),
          node('div', {}, [node('h4', { text: step.title }), node('p', { text: step.capability })]),
          planStepBadge(step.status),
        ])))
      : stateBox('尚无计划', '任务仍在建立目标、范围或交付物。'),
  ]);

  const actions = Array.isArray(detail.actions) ? detail.actions : [];
  const actionSection = node('section', { class: 'task-panel-section' }, [
    node('header', {}, [node('h3', { text: '下一步' }), node('span', { text: actions.length ? `${actions.length} 项` : '无需操作' })]),
    actions.length
      ? node('div', { class: 'task-panel-actions' }, actions.map((action) =>
        actionButton(action, task, detail.review_request, () => selectConversation(chatConversation))))
      : node('p', { class: 'muted-copy', text: '当前任务会继续推进，或在需要输入和确认时提供操作。' }),
  ]);

  const artifacts = Array.isArray(detail.artifacts) ? detail.artifacts.slice(-4).reverse() : [];
  const artifactSection = node('section', { class: 'task-panel-section' }, [
    node('header', {}, [node('h3', { text: '交付与证据' }), node('span', { text: `${detail.artifacts?.length ?? 0} 项` })]),
    artifacts.length
      ? node('ul', { class: 'task-panel-list' }, artifacts.map((artifact) =>
        node('li', { class: 'task-panel-item' }, [
          node('h4', { text: artifact.title }),
          node('p', { text: `${artifact.artifact_type} · ${artifact.evidence_refs?.length ?? 0} 条 Evidence` }),
        ])))
      : node('p', { class: 'muted-copy', text: '尚未产生可展示的 Artifact。' }),
  ]);

  const activity = Array.isArray(detail.activity) ? detail.activity.slice(-4).reverse() : [];
  const activitySection = node('section', { class: 'task-panel-section' }, [
    node('header', {}, [node('h3', { text: '最近活动' }), node('span', { text: activity.length ? `${activity.length} 条` : '暂无' })]),
    activity.length
      ? node('ol', { class: 'task-panel-list' }, activity.map((item) =>
        node('li', { class: 'task-panel-item' }, [
          node('h4', { text: item.title }),
          node('p', { text: `${formatDate(item.created_at)} · ${productStateLabel(item.state)}` }),
        ])))
      : node('p', { class: 'muted-copy', text: '尚无任务活动。' }),
  ]);

  taskPanelBody.replaceChildren(summary, plan, actionSection, artifactSection, activitySection, attemptList(detail.attempts));
}

async function loadConversationTask(taskId) {
  if (!taskId) {
    resetConversationTaskPanel();
    return;
  }
  const epoch = ++taskPanelEpoch;
  const changingTask = renderedTaskId !== taskId;
  const hasRenderedTask = taskPanelBody?.querySelector('.task-panel-summary') !== null;
  if (changingTask || !hasRenderedTask) {
    taskPanelBody?.replaceChildren(stateBox('正在读取任务状态', '从 Product Projection 获取最新计划、操作和活动。'));
  }
  try {
    const body = await api(`/api/product/tasks/${encodeURIComponent(taskId)}`);
    if (epoch !== taskPanelEpoch) return;
    const fingerprint = taskPanelProjectionFingerprint(body.detail);
    taskPanel?.removeAttribute('data-refresh');
    if (renderedTaskId === taskId && renderedTaskFingerprint === fingerprint) return;
    const scrollTop = changingTask ? 0 : (taskPanelBody?.scrollTop ?? 0);
    renderConversationTaskPanel(body.detail);
    renderedTaskId = taskId;
    renderedTaskFingerprint = fingerprint;
    if (taskPanelBody instanceof HTMLElement) taskPanelBody.scrollTop = scrollTop;
  } catch (error) {
    if (epoch !== taskPanelEpoch || !(taskPanelBody instanceof HTMLElement)) return;
    if (renderedTaskId === taskId && hasRenderedTask) {
      taskPanel?.setAttribute('data-refresh', 'stale');
      announce('任务状态刷新失败，继续显示上次有效状态');
      return;
    }
    renderError(taskPanelBody, error, () => loadConversationTask(taskId));
  }
}

function renderPresentation(container, presentation) {
  const markdown = node('div', { class: 'markdown-body' });
  renderMarkdown(markdown, presentation.markdown);
  const children = [markdown];
  if (presentation.fields?.length) children.push(node('div', { class: 'presentation-fields' }, presentation.fields.map((field) =>
    node('span', { class: 'presentation-field' }, [node('strong', { text: field.label }), field.value]))));
  if (presentation.table) {
    const table = node('table', { class: 'data-table' });
    table.append(node('thead', {}, [node('tr', {}, presentation.table.columns.map((column) => node('th', { text: column })))]));
    table.append(node('tbody', {}, presentation.table.rows.map((row) => node('tr', {}, row.map((cell) => node('td', { text: cell ?? '—' }))))));
    children.push(node('div', { class: 'table-wrap' }, [table]));
    if (presentation.table.truncated) children.push(notice('结果已截断', '当前只展示有界预览，请以 QueryResult Artifact 为准。'));
  }
  container.replaceChildren(...children);
}

function renderConversation(data) {
  const feed = page.querySelector('[data-conversation-feed]');
  const title = page.querySelector('[data-conversation-title]');
  title.textContent = data.summary.title;
  const entries = data.entries.map((entry) => {
    const presentation = node('div', { class: 'conversation-presentation' });
    renderPresentation(presentation, entry.presentation);
    const actions = node('div', { class: 'message-actions' }, entry.actions.map((action) => actionButton(action, entry.task, null, resumeConversationPolling)));
    return node('article', { class: 'conversation-entry' }, [
      node('div', { class: 'user-message' }, [entry.user_message.text, node('time', { text: formatDate(entry.user_message.created_at) })]),
      node('div', { class: 'forge-message' }, [
        node('header', {}, [node('h3', { text: entry.presentation.title }), badge(entry.task.display_state), node('a', { class: 'button button-quiet', href: `/tasks/${entry.task.task_run_id}`, text: '任务详情' })]),
        presentation,
        actions,
      ]),
    ]);
  });
  feed.replaceChildren(...entries);
  feed.scrollTop = feed.scrollHeight;
}

async function selectConversation(id, push = false) {
  const changed = chatConversation !== id;
  chatConversation = id;
  const epoch = ++conversationEpoch;
  if (changed) resetConversationTaskPanel('正在读取当前对话任务。');
  if (push) history.pushState({}, '', '/chat?conversation=' + encodeURIComponent(id));
  clearTimeout(chatPoll);
  try {
    const body = await api('/api/product/conversations/' + encodeURIComponent(id));
    if (epoch !== conversationEpoch || chatConversation !== id) return null;
    const conversation = body.conversation;
    renderConversation(conversation);
    const latestTaskId = conversation.entries?.at(-1)?.task?.task_run_id ?? null;
    await Promise.all([loadConversationList(), loadConversationTask(latestTaskId)]);
    return epoch === conversationEpoch && chatConversation === id ? conversation : null;
  } catch (error) {
    if (epoch !== conversationEpoch || chatConversation !== id) return null;
    resetConversationTaskPanel(error.status === 404 ? '当前对话不存在或不可见。' : '当前任务投影不可用。');
    renderError(page.querySelector('[data-conversation-feed]'), error, () => selectConversation(id));
    return null;
  }
}

async function resumeConversationPolling() {
  clearTimeout(chatPoll);
  chatPoll = setTimeout(() => pollConversation(true), 700);
}

function conversationNeedsPolling(conversation) {
  if (!conversation) return false;
  const latest = conversation.entries?.at(-1);
  const hasEnabledAction = latest?.actions?.some((action) => action.availability === 'enabled') ?? false;
  return latest?.task?.display_state === 'running'
    || (latest?.task?.display_state === 'ready' && !hasEnabledAction);
}

async function pollConversation(immediate = false) {
  clearTimeout(chatPoll);
  if (!chatConversation) return;
  const id = chatConversation;
  const epoch = conversationEpoch;
  if (!immediate) await new Promise((resolve) => setTimeout(resolve, 1200));
  if (chatConversation !== id || conversationEpoch !== epoch) return;
  const conversation = await selectConversation(id);
  if (chatConversation === id && conversationNeedsPolling(conversation)) chatPoll = setTimeout(() => pollConversation(true), 2500);
}

function newConversation() {
  clearTimeout(chatPoll);
  conversationEpoch += 1;
  chatConversation = conversationId();
  history.pushState({}, '', '/chat');
  page.querySelector('[data-conversation-title]').textContent = '新对话';
  const empty = page.querySelector('[data-conversation-empty]');
  page.querySelector('[data-conversation-feed]').replaceChildren(empty ? empty.cloneNode(true) : stateBox('新对话', '输入一个业务问题。'));
  resetConversationTaskPanel();
  setConversationTaskPanel(false);
  bindPromptButtons();
  page.querySelector('#conversation-input')?.focus();
  loadConversationList();
}

function bindPromptButtons() {
  page.querySelectorAll('[data-prompt]').forEach((button) => button.addEventListener('click', () => {
    const input = page.querySelector('#conversation-input');
    input.value = button.dataset.prompt || '';
    input.focus();
  }, { once: true }));
}

function setupChat() {
  const selected = new URLSearchParams(location.search).get('conversation');
  chatConversation = selected && /^web_[A-Za-z0-9_-]{8,128}$/.test(selected) ? selected : conversationId();
  page.querySelector('[data-new-conversation]')?.addEventListener('click', newConversation);
  taskPanelToggle?.addEventListener('click', () => setConversationTaskPanel(true));
  taskPanelClose?.addEventListener('click', () => setConversationTaskPanel(false));
  taskPanelBackdrop?.addEventListener('click', () => setConversationTaskPanel(false));
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && taskPanel?.dataset.open === 'true') setConversationTaskPanel(false);
  });
  const form = page.querySelector('[data-conversation-form]');
  const input = page.querySelector('#conversation-input');
  form?.addEventListener('submit', async (event) => {
    event.preventDefault();
    const message = input.value.trim();
    if (!message || input.disabled) return;
    const id = chatConversation;
    const payload = { message, conversation_id: id };
    const key = 'message:' + id;
    const command = pendingCommand(key, payload);
    input.disabled = true;
    // Preserve the conversation identity across reload, including a lost response.
    history.replaceState({}, '', '/chat?conversation=' + encodeURIComponent(id));
    try {
      await api('/api/pi/chat/messages', { method: 'POST', body: JSON.stringify({ ...payload, message_id: command.id }) });
      acknowledgeCommand(key, command);
      if (chatConversation !== id) return;
      input.value = '';
      await pollConversation(true);
    } catch (error) {
      if (chatConversation !== id) return;
      const status = notice(error.deliveryUnknown ? '消息投递结果未知' : '消息被拒绝', diagnosticMessage(error) + ' · Command ' + command.id + (error.deliveryUnknown ? '。再次发送相同内容会沿用此 ID，不会另建任务。' : ''), 'partial');
      status.append(node('a', { class: 'button', href: '/chat?conversation=' + encodeURIComponent(id), text: '回查原对话' }));
      const feed = page.querySelector("[data-conversation-feed]");
      feed.querySelector("[data-conversation-empty]")?.remove();
      feed.append(status);
      feed.scrollTop = feed.scrollHeight;
    } finally { input.disabled = false; input.focus(); }
  });
  input?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); form.requestSubmit(); }
  });
  window.addEventListener('popstate', async () => {
    const id = new URLSearchParams(location.search).get('conversation');
    if (id) {
      const conversation = await selectConversation(id);
      if (conversationNeedsPolling(conversation)) resumeConversationPolling();
    } else newConversation();
  });
  bindPromptButtons();
  loadConversationList();
  if (selected) selectConversation(chatConversation).then((conversation) => {
    if (conversationNeedsPolling(conversation)) resumeConversationPolling();
  });
}

async function loadTasks() {
  const list = page.querySelector('[data-task-list]');
  const filter = page.querySelector('[data-task-status-filter]');
  const count = page.querySelector('[data-task-count]');
  const status = filter?.value || '';
  try {
    const data = await api(`/api/product/tasks?limit=100${status ? `&status=${encodeURIComponent(status)}` : ''}`);
    count.textContent = `${data.tasks.length} 个任务${data.truncated_possible ? ' · 已达读取上限' : ''}`;
    if (!data.tasks.length) { list.replaceChildren(stateBox('没有符合条件的任务', '可以回到对话发起新的数据任务。')); return; }
    list.replaceChildren(itemList(data.tasks));
  } catch (error) { renderError(list, error, loadTasks); }
}

function planStepBadge(status) {
  const mapping = {
    pending: ['offline', '未开始'], ready: ['ready', '待执行'], running: ['running', '进行中'],
    waiting_approval: ['waiting_decision', '等待确认'], completed: ['completed', '已完成'],
    skipped: ['cancelled', '已跳过'], failed: ['failed', '未完成'],
  };
  const [state, label] = mapping[status] || ['offline', '状态未知'];
  return node('span', { class: 'status', 'data-state': state, text: label });
}

function renderTaskDetail(detail) {
  const hero = page.querySelector('[data-task-hero]');
  hero.querySelector('h2').textContent = detail.task.title;
  hero.querySelector('.status').replaceWith(badge(detail.task.display_state));
  const notices = page.querySelector('[data-page-notices]');
  notices.replaceChildren();
  if (detail.projection_meta.availability !== 'ready') notices.append(notice('任务信息部分可用', detail.projection_meta.unavailable_reasons.join('、')));

  const planBody = page.querySelector('[data-task-plan] .panel-body');
  if (!detail.plan) planBody.replaceChildren(stateBox('尚无执行计划', '该任务当前没有可投影的计划。'));
  else {
    const list = node('ol', { class: 'plan-list' }, detail.plan.steps.map((step, index) => node('li', { class: 'plan-step' }, [
      node('span', { class: 'step-number', text: index + 1 }),
      node('div', {}, [node('h3', { text: step.title }), node('p', { text: `${step.capability}${step.required ? ' · 必需' : ' · 可选'}` })]),
      planStepBadge(step.status),
    ])));
    planBody.replaceChildren(list);
  }

  const reviewPanel = page.querySelector('[data-task-review]');
  if (detail.review_request) {
    reviewPanel.hidden = false;
    const review = detail.review_request;
    const facts = node('dl', { class: 'review-facts' }, [
      ['Query', review.query_run_id], ['方言', review.dialect], ['有效期', formatDate(review.expires_at)], ['只读', review.read_only ? '是' : '否'],
    ].map(([label, value]) => node('div', { class: 'review-fact' }, [node('dt', { text: label }), node('dd', { text: value })])));
    const approve = detail.actions.find((action) => action.action_type === 'approve_query');
    const primary = approve
      ? node('div', { class: 'review-primary-action' }, [
        node('p', { text: '确认后才会发起只读执行；取消任务或离开不会执行 SQL。' }),
        actionButton(approve, detail.task, review, loadTaskDetail),
      ])
      : notice('审核已结束', '当前没有可执行的 SQL 审核操作；以下内容仅供追溯。', 'ready');
    page.querySelector('[data-task-review-body]').replaceChildren(
      facts,
      node('pre', { class: 'code-block', text: review.sql }),
      notice('审批边界', '执行前会重新核对 SQL、安全校验结果和 Registry 版本。', 'waiting_decision'),
      primary,
    );
  } else reviewPanel.hidden = true;

  renderPresentation(page.querySelector('[data-task-presentation] .panel-body'), detail.presentation);
  const artifactContainer = page.querySelector('[data-task-artifacts] .panel-body');
  if (!detail.artifacts.length) artifactContainer.replaceChildren(stateBox('暂无交付', '任务推进后会生成查询、分析或报告 Artifact。'));
  else artifactContainer.replaceChildren(node('ul', { class: 'artifact-list' }, detail.artifacts.map((artifact) => node('li', { class: 'artifact-row' }, [
    node('div', {}, [node('h3', { text: artifact.title }), node('p', { text: `${artifact.artifact_type} · ${artifact.evidence_refs.length} 条 Evidence · ${formatDate(artifact.created_at)}` })]),
    artifact.href ? node('a', { class: 'button', href: artifact.href, text: '打开' }) : badge(artifact.state),
  ]))));

  const actionContainer = page.querySelector('[data-task-actions] .panel-body');
  if (!detail.actions.length) actionContainer.replaceChildren(stateBox('当前无需操作', '任务会在需要补充或确认时提供操作。'));
  else actionContainer.replaceChildren(node('div', { class: 'action-stack' }, detail.actions.map((action) => actionButton(action, detail.task, detail.review_request, loadTaskDetail))));

  const relations = page.querySelector('[data-task-relations] .panel-body');
  const links = [];
  if (detail.relations.parent_task_run_id) links.push(node('a', { class: 'relation-link', href: `/tasks/${detail.relations.parent_task_run_id}`, text: `父任务 ${detail.relations.parent_task_run_id}` }));
  for (const child of detail.relations.child_task_run_ids) links.push(node('a', { class: 'relation-link', href: `/tasks/${child}`, text: `补查任务 ${child}` }));
  relations.replaceChildren(links.length ? node('div', { class: 'relation-list' }, links) : node('p', { class: 'muted-copy', text: '没有关联的父任务或补查任务。' }));

  const activity = page.querySelector('[data-task-activity] .panel-body');
  activity.replaceChildren(node('ol', { class: 'activity-list' }, detail.activity.slice().reverse().map((item) => node('li', { class: 'activity-row' }, [
    node('h3', { text: item.title }), node('p', { text: `${formatDate(item.created_at)} · ${item.state === 'ready' ? '已记录' : productStateLabel(item.state)}` }),
  ]))));
  activity.append(attemptList(detail.attempts));
}

async function loadTaskDeliveries(task) {
  const panel = page.querySelector('[data-task-deliveries]');
  if (!panel) return;
  panel.hidden = task.channel !== 'feishu';
  if (panel.hidden) return;
  const container = panel.querySelector('.panel-body');
  try {
    const data = await api('/api/pi/tasks/' + encodeURIComponent(task.task_run_id) + '/deliveries');
    const labels = { delivered: '已送达', failed: '投递被拒绝', unknown: '送达结果未知', pending: '等待投递', sending: '正在投递' };
    container.replaceChildren(...data.deliveries.map((receipt) => {
      const card = notice(labels[receipt.status] || '状态未知', receipt.delivery_id + ' · Command ' + receipt.command_id + ' · Request ' + receipt.request_id + (receipt.error_code ? ' · ' + receipt.error_code : ''), receipt.status === 'delivered' ? 'ready' : 'partial');
      if (receipt.retry_available) {
        const retry = node('button', { class: 'button', type: 'button', text: '仅重送展示' });
        retry.addEventListener('click', async () => {
          retry.disabled = true;
          try {
            await api('/api/pi/deliveries/' + encodeURIComponent(receipt.delivery_id) + '/retry', { method: 'POST' });
            await loadTaskDeliveries(task);
          } catch (error) { announce(diagnosticMessage(error)); retry.disabled = false; }
        });
        card.append(retry);
      }
      return card;
    }));
    if (!data.deliveries.length) container.append(notice('未记录投递回执', '此任务可能早于回执功能；没有记录不等于尚未发送。'));
  } catch (error) { renderError(container, error, () => loadTaskDeliveries(task)); }
}


async function loadTaskDetail() {
  const taskId = page.dataset.taskId;
  try {
    const detail = (await api(`/api/product/tasks/${encodeURIComponent(taskId)}`)).detail;
    renderTaskDetail(detail);
    await loadTaskDeliveries(detail.task);
    announce("任务详情已更新");
  }
  catch (error) { renderError(page.querySelector('[data-task-plan] .panel-body'), error, loadTaskDetail); }
}

function openActionDialog(action, task, review, refresh) {
  const dialog = document.querySelector('[data-action-dialog]');
  const form = dialog?.querySelector('[data-action-form]');
  if (!(dialog instanceof HTMLDialogElement) || !(form instanceof HTMLFormElement)) return;
  const inputWrap = dialog.querySelector('[data-dialog-input-wrap]');
  const input = dialog.querySelector('#dialog-input');
  const title = dialog.querySelector('[data-dialog-title]');
  const copy = dialog.querySelector('[data-dialog-copy]');
  title.textContent = action.label;
  const requiresInput = action.action_type === 'provide_input';
  inputWrap.hidden = !requiresInput;
  input.value = '';
  copy.textContent = action.action_type === 'approve_query'
    ? '将批准当前页面展示的精确 SQL 和安全校验对象，并以只读方式执行。'
    : action.action_type === 'cancel_task'
      ? '取消后不会自动重放已停止的阶段。'
      : '确认后将通过当前 Task 的 typed action 推进。';
  dialog.returnValue = '';
  const onClose = async () => {
    dialog.removeEventListener('close', onClose);
    if (dialog.returnValue !== 'confirm') return;
    const payload = { ...(action.payload || {}) };
    if (requiresInput) {
      if (!input.value.trim()) { announce('补充信息不能为空'); return; }
      payload.text = input.value.trim();
    }
    if (action.action_type === 'approve_query' && review) {
      payload.query_run_id = review.query_run_id;
      payload.sql_hash = review.sql_hash;
      payload.assurance_report_hash = review.assurance_report_hash;
    }
    const key = "action:" + task.task_run_id + ":" + action.action_type;
    const body = { action: action.action_type, conversation_id: task.conversation_id, payload };
    const command = pendingCommand(key, body, "web_action");
    try {
      await api(`/api/pi/chat/tasks/${task.task_run_id}/actions`, {
        method: "POST", body: JSON.stringify({ ...body, message_id: command.id }),
      });
      acknowledgeCommand(key, command);
      announce('操作已提交');
      await refresh();
      window.setTimeout(() => refresh(), 1600);
    } catch (error) { announce((error.deliveryUnknown ? "操作投递结果未知，重试会沿用原命令：" : "操作被拒绝：") + diagnosticMessage(error) + " · Command " + command.id); }
  };
  dialog.addEventListener('close', onClose);
  dialog.showModal();
  if (requiresInput) input.focus();
}

let reportCursor = null;
let reportRows = [];

async function loadReports(reset = true) {
  const list = page.querySelector('[data-report-list]');
  const filter = page.querySelector('[data-report-status-filter]');
  const status = filter?.value || '';
  try {
    const query = new URLSearchParams({ limit: '50' });
    if (status) query.set('status', status);
    if (!reset && reportCursor) query.set('cursor', reportCursor);
    const data = await api(`/api/product/reports?${query}`);
    reportRows = reset ? data.reports : [...reportRows, ...data.reports];
    reportCursor = data.next_cursor;
    page.querySelector('[data-report-count]').textContent = `${reportRows.length} 份报告`;
    const pagination = page.querySelector('[data-report-pagination]');
    pagination.hidden = !reportCursor;
    if (!reportRows.length) { list.replaceChildren(stateBox('暂无报告', '完成分析并生成报告后会出现在这里。')); return; }
    list.replaceChildren(node('ul', { class: 'item-list' }, reportRows.map((report) => node('li', { class: 'item-row' }, [
      node('a', { class: 'item-link', href: report.internal_url || '#'}, [node('p', { class: 'item-title', text: report.title }), node('div', { class: 'item-meta' }, [`revision ${report.revision}`, formatDate(report.updated_at), `PDF ${report.pdf_status}`, `PPTX ${report.pptx_status}`])]),
      badge(report.display_state),
    ]))));
  } catch (error) { renderError(list, error, () => loadReports(true)); }
}

async function loadDataSummary() {
  const stats = page.querySelector('[data-data-stats]');
  const links = page.querySelector('[data-data-links]');
  const notices = page.querySelector('[data-page-notices]');
  try {
    const data = await api('/api/product/data-summary');
    stats.replaceChildren(
      ...[
        ['数据表', data.counts.tables, '结构层'], ['业务指标', data.counts.metrics, '语义层'],
        ['Registry revision', data.source_revision.slice(0, 18), '内容版本'], ['当前状态', productStateLabel(data.status), '结构与指标'],
      ].map(([label, value, noteText]) => node('div', { class: 'stat' }, [node('p', { class: 'stat-label', text: label }), node('p', { class: `stat-value${label.includes('revision') || label === '当前状态' ? ' stat-value-small' : ''}`, text: value }), node('p', { class: 'stat-note', text: noteText })])),
    );
    links.replaceChildren(...data.links.map((item) => node('a', { class: 'data-entry', href: item.href }, [node('h3', { text: item.label }), node('p', { text: '进入现有可信数据资产真相源进行查看或维护。' }), node('span', { text: '打开 →' })])));
    notices.replaceChildren();
    if (data.status !== 'ready') notices.append(notice('数据资产部分可用', data.unavailable_reasons.join('、')));
  } catch (error) { renderError(links, error, loadDataSummary); }
}

function setupRefresh(handler) {
  page.querySelectorAll('[data-refresh-page]').forEach((button) => button.addEventListener('click', handler));
}

if (page) {
  const type = page.dataset.productPage;
  if (type === 'workspace') { setupRefresh(loadWorkspace); loadWorkspace(); }
  if (type === 'chat') setupChat();
  if (type === 'tasks') {
    setupRefresh(loadTasks);
    page.querySelector('[data-task-status-filter]')?.addEventListener('change', loadTasks);
    loadTasks();
  }
  if (type === 'task-detail') loadTaskDetail();
  if (type === 'reports') {
    setupRefresh(() => loadReports(true));
    page.querySelector('[data-report-status-filter]')?.addEventListener('change', () => loadReports(true));
    page.querySelector('[data-load-more-reports]')?.addEventListener('click', () => loadReports(false));
    loadReports(true);
  }
  if (type === 'data') { setupRefresh(loadDataSummary); loadDataSummary(); }
}
