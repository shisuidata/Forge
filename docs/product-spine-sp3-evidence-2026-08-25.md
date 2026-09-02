# Product Spine SP3 Evidence（2026-08-25）

> Requirement: `REQ-2026-08-25-017` · Scope: Product Shell Foundation · Verdict: PASS

## 实施

- 新增 `web/templates/product_base.html`；
- 新增本地 `web/static/product/product.css`；
- 新增本地 `web/static/product/product-shell.js`；
- `main.py` 挂载 `/static`；现有 `/charts` 保持兼容；
- 一级导航仅为：工作台、对话、任务、报告、数据资产、管理与诊断；
- 未显示 Agents & Apps、Economics、Pipeline、Memory 或 Architecture；
- 未恢复独立“新建任务”表单。

## 设计与交互边界

- 方向：克制的 editorial/operational work surface；暖灰纸面、白色工作面、深色侧栏、moss 主操作、amber Decision、red failure；
- 无 Marketing Hero、渐变氛围背景、卡片墙或口号；
- 本地字体栈，不下载外部字体；
- 共享 panel、status、button、field、table、notice、empty/partial/offline、skeleton、code/evidence 组件；
- 支持 skip link、`aria-current`、focus-visible、Escape 关闭导航、reduced motion 和 print；
- JS 只处理 Shell navigation、announcer 和状态标签，不发起 fetch、不写 localStorage、不创建第二前端状态源。

## 验证

- Python 全量：`581 passed / 24 skipped`；
- SP3/Docs/Web content 定向：`10 passed`；
- Pi：`114 passed`；
- TypeScript typecheck：PASS；
- npm audit：0 vulnerabilities；
- CSS/JS 通过 `/static/product/*` 本地返回；
- Template render/active navigation：PASS；
- 0 CDN、0 inline style/script、无 deferred 产品入口、无 fixture 数据；
- `git diff --check`：PASS。

## 未做

SP3 没有将任何现有 `/chat`、`/tasks` 或 Admin 页面迁入新 Shell，也没有增加 `/workspace`、`/reports`、`/data` 页面。因此当前用户仍看不到新 Product Shell；SP4 才连接真实 Product BFF 并执行浏览器门禁。

## Verdict

SP3 PASS，允许进入 SP4。SP4 必须使用真实 `/api/product/*` 和已有 typed actions；不能为页面完整度加入 fixture 或无效按钮。
