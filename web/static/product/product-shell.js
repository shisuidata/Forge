const nav = document.querySelector('[data-product-nav]');
const toggle = document.querySelector('[data-nav-toggle]');
const backdrop = document.querySelector('[data-nav-backdrop]');
const announcer = document.querySelector('[data-product-announcer]');

function setNavigation(open) {
  if (!(nav instanceof HTMLElement) || !(toggle instanceof HTMLButtonElement)) return;
  nav.classList.toggle('is-open', open);
  toggle.setAttribute('aria-expanded', String(open));
  if (backdrop instanceof HTMLButtonElement) backdrop.hidden = !open;
  document.documentElement.style.overflow = open ? 'hidden' : '';
  if (open) {
    const first = nav.querySelector('a');
    if (first instanceof HTMLElement) first.focus();
  } else {
    toggle.focus();
  }
}

if (toggle instanceof HTMLButtonElement) {
  toggle.addEventListener('click', () => setNavigation(!nav?.classList.contains('is-open')));
}
if (backdrop instanceof HTMLButtonElement) {
  backdrop.addEventListener('click', () => setNavigation(false));
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && nav?.classList.contains('is-open')) setNavigation(false);
});

const desktop = window.matchMedia('(min-width: 52.01rem)');
desktop.addEventListener('change', (event) => {
  if (event.matches && nav?.classList.contains('is-open')) setNavigation(false);
});

export function announce(message) {
  if (!(announcer instanceof HTMLElement)) return;
  announcer.textContent = '';
  window.requestAnimationFrame(() => { announcer.textContent = String(message).slice(0, 500); });
}

export function productStateLabel(state) {
  const labels = {
    loading: '正在读取',
    empty: '当前为空',
    needs_input: '需要补充',
    waiting_decision: '等待确认',
    running: '进行中',
    partial: '部分可用',
    ready: '可用',
    available: '可用',
    planned: '规划中',
    blocked: '未接入 Runtime',
    failed: '未完成',
    forbidden: '没有权限',
    offline: '依赖不可用',
    completed: '已完成',
    cancelled: '已取消',
    superseded: '已被替代',
  };
  return labels[state] || '状态未知';
}

window.ForgeProductShell = Object.freeze({ announce, productStateLabel });
