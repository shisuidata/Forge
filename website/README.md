# Forge Website

Forge 官网使用 Astro + Starlight。首页是独立的 Astro 页面，文档使用 Starlight；不运行 Forge API、Pi、模型或数据库，也不需要登录。

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

不要在同一端口同时运行 dev 和 preview。验收以构建产物为准，不以 dev 状态码代替内容检查。

## 设计：关口（2026-10-25，REQ-2026-10-25-074）

用户要求完全重新设计首页、文档外观与品牌，不基于 2026-10-16 的旧版本。设计概念取自 Forge 的本质：Agent 生成的每条 SQL 都要经过关口，结果被盖章，执行后留下凭证。

- **品牌**：`public/brand/forge-mark.svg` 是一个道闸形状的 “F”，横杆带警示斜纹；同一 SVG 生成 16/32/48（ICO）、180、192、512px 图标。字标为加宽的 Archivo 大写 “FORGE”。
- **配色**：石墨底（`--forge-graphite`），警示黄（`--forge-signal`）是唯一品牌色；绿、蓝、红只用于“通过 / 需审核 / 拒绝”的结果印章。文档浅色主题用冷纸色底和深琥珀色链接。
- **字体**：Archivo Variable（宽度轴 62–125%）用于标题与正文，Martian Mono Variable 用于代码、标签与印章；中文使用系统黑体。字体随包加载，许可证在 `public/licenses/`。
- **首页结构**：首屏 → 关口看板 → 三道关口（各自写明“不证明什么”）→ 有界授权通行证 → Explain 证据小票 → 公开验证记录与未验证范围 → 本地试跑 → 常见问题。
- **看板与卡片**：关口看板、Mandate 通行证、Explain 小票都是标注为“合成示意”的静态内容，场景与 `forge quickstart --agent` 一致，不连接任何服务。

## 文件职责

- `src/pages/index.astro`：首页结构、文案数据、复制、手机菜单与看板重放脚本。
- `src/styles/brand.css`：品牌变量与字体导入，首页与文档共用。
- `src/styles/landing.css`：首页样式，不影响文档布局。
- `src/styles/custom.css`：Starlight 深浅主题、侧栏、目录、正文、表格与提示框。
- `src/components/Mark.astro`：首页内联 SVG 标记；`src/assets/forge-mark.svg` 供 Starlight 页头使用，与 `public/brand/forge-mark.svg` 内容相同。
- `src/content/docs/`：介绍、快速开始、概念、参考、历史课程与日志；路由保持不变。
- `astro.config.mjs`：文档导航、Logo、图标、Markdown 表格键盘焦点；正式域名 `https://forge.ss-data.cc`。

## 内容与事实边界

- 当前产品事实以[当前状态](../docs/current-project-state.md)和[北极星](../docs/product-north-star.md)为准。
- 首页验证数字来自 [2026-10-22 公开测试摘要](../docs/archive/engineering/latest-test-report-2026-10-22.md)（提交 `e50b0a0`），不是官网改版时重新运行的结果。
- 准确率只写 BIRD 500 题同候选重评的 313 vs 314 统计持平，不宣称准确率优势。
- 首页列出尚未验证的范围：真实数据库只读角色、真实身份与客户数据、独立安全审核、外部采用。
- 不展示虚构客户、采用量或收益，不写“生产就绪”“安全认证”“100% 正确”。

## 部署

```bash
npm --prefix website run build
CI=true WRANGLER_SEND_METRICS=false wrangler --cwd /tmp \
  --config "$PWD/website/wrangler.jsonc" deploy --no-autoconfig
```

`wrangler.jsonc` 只绑定静态资产目录 `dist` 与 `forge.ss-data.cc`，关闭 workers.dev 与版本预览 URL；没有 API、数据库、模型、Secret 或 Runtime 绑定。从 `/tmp` 运行，避免仓库根目录的自动配置发现。只使用已有 Cloudflare 授权，不扩大 OAuth 权限。是否上线以部署回执和真实域名的逐文件比对为准。

## Cloudflare 上线回执（2026-10-28，MCP 接入指南）

- 源码提交：`0825b49`（新增 `/guides/mcp/`“接入你的 Agent（MCP）”，首页 FAQ 与“本地试跑”加入口；其余页面与设计不变）。100% 流量版本 `b06dbed5-a468-4016-9fe4-ce0d31f324b2`，取代 `46dd88ff`。本次未打标签。
- 产物：115 个文件、2,749,079 bytes；清单 SHA-256 `e822c24fe32f5d26b56cc2e8e49a4e13657e345c014f96b85cbc76510a4c8f46`（算法同下方旧回执）；首页 `index.html` SHA-256 `ca16fbda3449062f0463b69aa72f7bbf5932cda0743595fad15d84ab9c5bb0a9`。
- 发布后从真实域名下载全部 115 项，与本地构建逐字节一致；首页与新指南 HTTP 200。真实浏览器访问线上 `/guides/mcp/`：1440px 与 390px 无横向溢出、无页面错误、无站外请求；站内搜索 “forge review” 命中新指南。
- 部署前对同一构建：29 页、1902 个站内链接与锚点全部有效；axe-core 4.13 WCAG A/AA 在上次所检页面及新指南（浅色 1440、深色 1440、深色 390）均为 0 条违规（自动检查，不等于完整 WCAG 认证）。
- 回退到上一版（经用户确认后）：`wrangler ... rollback 46dd88ff-9979-43b2-814d-35422beb99da`。未部署 Forge API、Pi、模型或数据库；R0.6 与 H 门禁不变。

## Cloudflare 上线回执（2026-10-25，关口设计）

- 正式地址：`https://forge.ss-data.cc/`；Worker Static Assets 项目 `forge-website`，只有静态资产。
- 源码提交：`37303a6`（设计主体 `a852297`，加桌面首屏两行修正）。100% 流量版本 `46dd88ff-9979-43b2-814d-35422beb99da`，标签 `website-2026-10-25`；同日先部署过 `a852297` 对应的 `0bc1ce7b-0674-49b1-b0a9-551228972b47`，已被取代。
- 产物：113 个文件、2,696,524 bytes；清单 SHA-256 `71895981af9acf74fbbeefc2458245dc15c333c480f0e37fc8b9be7c0f714d73`（算法同下方旧回执）；首页 `index.html` SHA-256 `14ff8b993e4ba31e9cb9f9901934898139b4a7c305a4a35c4b6444c20628a6e0`。
- 发布后经公共 DoH 解析、保留证书校验直连下载全部 113 项，与本地构建逐字节一致；首页 HTTP 200、TLS verify 0；`/api/health`、`/openapi.json` 与旧标记 `/brand/forge-mark-light.webp` 均为 404。
- 真实浏览器访问线上域名：1440px 与 390px 无横向溢出、无页面错误、无站外请求；文档搜索“五分钟上手”返回 4 项结果。
- 部署前对同一构建：28 页、1819 个站内链接与锚点全部有效；axe-core 4.13 WCAG A/AA 在首页 1440/390、Quickstart 深浅主题、390px 介绍页、深色课程页、浅色 Benchmark 页均为 0 条违规（自动检查，不等于完整 WCAG 认证）；320px 无溢出，菜单、Escape、复制、重放、主题切换与搜索可用。
- 回退到旧版官网（经用户确认后）：`wrangler ... rollback 82162d84-1a2e-4c19-b158-ceb6f88a885f`。未部署 Forge API、Pi、模型或数据库；R0.6 与 H 门禁不变。

## Cloudflare 上线回执（2026-10-16，旧版官网）

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
