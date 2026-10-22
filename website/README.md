# Forge Website

Forge 官网沿用 Astro + Starlight。首页是独立的 Astro 产品页，文档继续使用 Starlight；不运行 Forge API、Pi、模型或数据库，也不需要登录。

## 本地开发与预览

在仓库根目录运行：

```bash
npm --prefix website ci
npm --prefix website run dev -- --host 127.0.0.1 --port 4331
```

检查实际交付产物和站内搜索时：

```bash
npm --prefix website run build
npm --prefix website run preview -- --host 127.0.0.1 --port 4331
```

不要在同一端口同时运行 dev 和 preview。内容大量更新或执行 build 后，如 dev 仍呈现旧 Markdown，请重新启动预览并检查构建产物，不以 HTTP 200 代替内容验证。

## 文件职责

- `src/pages/index.astro`：官网结构、产品示意切换、移动导航和命令复制。
- `src/styles/landing.css`：独立首页的响应式视觉，不覆盖文档布局。
- `src/styles/brand.css`：品牌变量与随包字体；Manrope Variable / IBM Plex Mono，不依赖 Google Fonts CDN。
- `src/styles/custom.css`：Starlight 明暗主题、导航、代码和表格。
- `src/components/Mark.astro`、`public/brand/forge-mark-{light,dark}.webp`：用户确认的生成式蚊子标记及深浅背景派生；PNG/ICO 浏览器图标同步采用同一图形。
- `src/content/docs/`：介绍、快速开始、概念、参考、架构课程与历史日志；既有文档路由保留。
- `astro.config.mjs`：文档导航、品牌、Markdown 表格键盘焦点；正式域名已由用户确定为 `https://forge.ss-data.cc`，用于 site/canonical/sitemap。

旧 `src/content/docs/index.mdx` 已由独立首页替代；不要恢复两份首页真相源。

## 设计参考（2026-10-16）

本轮在真实浏览器研究以下公开网站，借鉴信息结构与视觉方法，不复用品牌、截图、客户标识或代码：

| 参考 | 借鉴 |
|---|---|
| [Langfuse](https://langfuse.com/) | 紧凑开发者导航、克制浅色布局、产品与证据入口 |
| [Supabase](https://supabase.com/) | 开发者导向的产品分区、清楚的开始使用路径 |
| [dbt](https://www.getdbt.com/) | 品牌色节奏、先价值再产品演示的叙事 |

Forge 使用原创暖白、墨黑、锻造橙和细边框；首页从产品价值 → Evaluate/Enforce/Explain → 验证边界 → 本地试跑 → FAQ 组织内容。

## 内容与事实边界

- 当前产品事实以 [当前状态](../docs/current-project-state.md)、[北极星](../docs/product-north-star.md)为准。
- 首页 Inspector 为明确标注的合成、非实时示意，不连接数据库或发起授权；阶段按钮只切换说明，不伪装执行。
- 不展示虚构客户、采用量、未经验证的收益或“100% 正确”“错误已解决”“生产就绪”。
- 首页 Python 1011/26、Pi 212 来自 [2026-10-04 本地评估的公开摘要](../docs/archive/engineering/latest-test-report-2026-10-14.md)，不是本轮官网改版重新运行的结果，也不是公开提交/远端 CI/部署认证。
- 未关闭的 P0、安全审核与外部采用边界保留；官网改版不关闭这些门禁。
- 发布前已逐路由审阅全部 26 篇文档及侧栏入口：当前指南区分公开 v1 合成 Quickstart 与本地未发布 v2 候选；不把 `--agent` 当成公开命令。历史课程与日志保留原文和成绩，并加顶部醒目历史提示，不能作为当前安全或生产承诺。

## 本轮实际验收（2026-10-16）

- `npm --prefix website run build` 通过，28 个静态页面及 Pagefind 搜索索引生成。
- 实际构建产物经 `astro preview` 验证，不只检查源码或 dev 状态码；Quickstart 正文为当前公开 Trust Runtime 路径。
- 浏览器目视检查 1440px 桌面、390px 和 320px 窄屏；首页及已检查文档无页面级横向溢出。
- 三阶段点击、方向键切换、手机导航开关/Escape/选中后收起、FAQ 展开有效；命令复制成功路径已在 dev 读取剪贴板核对，产物预览也实际观察到浏览器拒绝复制时的手动复制提示。
- 文档明暗主题、站内搜索（“五分钟上手”返回实际结果）、宽表键盘滚动经浏览器验证。
- 首页与 Quickstart 链接合并检查 38 个内部 URL/锚点，无 HTTP/锚点失败；不代表全部历史文档外链已验证。
- 仅允许本地主机请求的浏览器能加载首页与本地字体，无页面错误；不需外部字体服务。
- axe-core 4.13 的 WCAG A/AA 自动检查：首页各阶段、320px 首页、Quickstart 明暗主题及390px Benchmark 检查均为 0 条确认违规；仍有人工复核项，未宣称通过完整 WCAG 认证。发现的小字对比度与宽表键盘访问问题已修复。
- 初版构建曾提示未设置 site；用户确定域名后，当前配置已生成正式 sitemap。Astro/Starlight Markdown 插件 API 弃用及默认 404 无内容条目的提示仍保留，不隐藏警告。

初版官网重设计验收未重跑 Python/Pi 回归、未调用模型、未读取真实数据、未重启 NAS、未提交/推送或发布部署，对应 [REQ-2026-10-16-072](../docs/requirements-pool.md#req-2026-10-16-072参考同类优质产品重做-forge-官网)。后续品牌与 Cloudflare 静态发布授权单列为 REQ-2026-10-16-073。

## 已确认品牌与 Cloudflare 发布

- 用户选择 `public/brand/forge-mosquito-generated.webp`，由 GPT Image 2 生成，非 Imagen；保留原图，不宣称已经矢量化或取得商标认证。
- 深浅背景版本保持同一轮廓：透明原图按 alpha 边界加留白后缩放至 512px；深色背景版本只将非橙色主体调整为浅色，保留橙色强调与 alpha。
- 图标包含 16/32/180/192/512px PNG 和 PNG-backed ICO，来自同一确认图形；未采用的手画 SVG 和独立方向稿已移出公开目录，在本地 `.forge/brand-explorations/2026-10-16/` 保留。
- `wrangler.jsonc` 只绑定静态资产目录 `dist` 和 `forge.ss-data.cc`，关闭 workers.dev 与版本预览 URL；没有 API、数据库、模型、Secret 或 Runtime bindings。
- 发布前须检查文案、当前公开源码与未发布候选的区分、资源清单和最终构建；不能用网站上线替代 Runtime P0/R0.6/H 验收。

在仓库根目录，使用当前获授权的 Cloudflare 登录：

```bash
npm --prefix website run build
CI=true WRANGLER_SEND_METRICS=false wrangler --cwd /tmp \
  --config "$PWD/website/wrangler.jsonc" deploy --no-autoconfig
```

只上传静态构建；从 `/tmp` 运行避免仓库根目录自动配置/环境发现。不得使用其它项目凭证或为了部署自动扩大 OAuth 权限。实际是否上线与部署版本以发布回执及域名 HTTP/浏览器验证为准。

## Cloudflare 上线回执（2026-10-16）

- 正式地址：`https://forge.ss-data.cc/`；Cloudflare Worker Static Assets 项目 `forge-website`，不是 Pages，也不是 Forge Runtime。
- 首次部署：`2026-10-15T21:05:36.158Z`（北京时间 2026-10-16）；100% 流量版本 `82162d84-1a2e-4c19-b158-ceb6f88a885f`，标签 `website-2026-10-16`。
- 最终产物：134 个文件、3,098,075 bytes；清单 SHA-256 `986ac53787207bba6c685ddbb7fae8b5d0cf36d3cf7469a28afc15b56c023ef4`。
- 清单算法：按相对 `dist` 路径排序，每行 `SHA256(file) + 两个空格 + 相对路径 + LF`，再对全部行连接后的 UTF-8 内容取 SHA-256。
- 首页 `index.html` SHA-256：`f4b47b1f6a1197effdd1bb49f7fbc28925bc794bcd1e52b95436516307bc4121`。
- 发布后从真实域名逐文件下载全部 134 项，字节数、清单 hash 与本地最终构建完全一致，0 项内容不一致。
- 公共 DoH 返回 Cloudflare 地址；真实 HTTPS 浏览器打开首页与文档，直连 Cloudflare 保留证书校验，首页 HTTP 200、TLS verify 0。未知路径、`/api/health`、`/openapi.json` 均为 404，无 Runtime/API 暴露。
- 最终本地产物全站检查：sitemap 的 27 个公开页面、48 个唯一站内目标及对应锚点，0 条失败。
- 线上桌面/390px 手机目视、Evaluate → Enforce 切换、手机菜单展开及选择后关闭、文档深色 Logo、390px 无页面横向溢出均已观察。
- 线上搜索“五分钟上手”返回 4 项实际结果，点击“安装部署”进入对应文档；全新验收 tab 的页面错误记录为空。批量资源比对采用保留 TLS 校验的直连下载，避免本机代理 fake-IP/旧解析干扰。
- 只使用已有 Cloudflare 授权，未扩大 OAuth 权限、未提交/推送 Git、未启动 NAS、未部署 API/Pi/模型/数据库、未修改其它站点；Runtime P0、R0.6 和 H 门禁不变。本轮未重跑 Python/Pi。

### 回退边界

`wrangler deployments list` 当前仅有这次首次部署，没有更早的正式官网版本；本轮未执行回退。以后如需恢复本次已验收版本，先核对部署列表及目标，经用户确认后执行：

```bash
CI=true WRANGLER_SEND_METRICS=false wrangler --cwd /tmp \
  --config "$PWD/website/wrangler.jsonc" \
  rollback 82162d84-1a2e-4c19-b158-ceb6f88a885f
```

保留 Cloudflare 版本与本地源文件；不要通过删除域名、猜测旧版本或发布整个 Forge 工作区来回退静态官网。
