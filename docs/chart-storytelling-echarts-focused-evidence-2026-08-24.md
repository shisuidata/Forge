# H5 ECharts Focused Candidate Evidence（2026-08-24）

## Verdict

ECharts focused candidate 已在隔离工具包 `tools/chart-storytelling-echarts-candidate/` 完成。候选不再展示 engine 实验室、宣传 Hero、slogan、候选说明或 Renderer 自我介绍；首屏直接进入报告标题、范围、新鲜度、数据质量和执行摘要，第一张图在 1600×1000 viewport 的 `y=585.8px` 开始可见。

当前状态：**实现与自动门禁通过，等待用户视觉与信息价值确认**。这不是 H5 R1 生产批准；生产 Pi package、Skills/Prompt、Structured Tool、Chart v1 Renderer、NAS 和已发布 Report revision 均未修改。

## Semantic Correction

原 bake-off 第四图是渠道存量堆叠面积图，只能说明结构，不能证明“4→6 月新增量中直营贡献 50%”。Focused candidate 改为渠道增量贡献图：

```text
直营：402K - 315K = 87K  / 174K = 50.0%
平台：271K - 218K = 53K  / 174K = 30.5%
门店：222K - 188K = 34K  / 174K = 19.5%
合计：87K + 53K + 34K = 174K
```

计算仅使用 `qr_monthly_story#row:4` 与 `#row:6`，图形、tooltip、数据表、Annotation 和 Evidence drawer 使用相同 normalized records。

同时修正：

- 排名图直接标出前两名仅差 `31.8K / 4.4%`，第一、第二名均被编码，避免只突出“冠军”制造赢家错觉；
- 趋势图 Y 轴改为零基线，四月/六月偏差通过 markPoint 和 Evidence 表达，不靠截断坐标夸大波动；
- Pareto 图继续保留 80% 经营覆盖线和第六类 threshold；
- 终端用户页面只显示报告内容；技术选型与边界留在本文和 README。

## Adapter Boundary

`src/adapter.js` 仅接受四个固定 `viewId`：

- `ranking`
- `pareto`
- `trend`
- `contribution`

每个 ID 映射到一个 allowlisted ECharts Option builder。输入不存在“自由 Option”通道，未知 ID 直接抛错；formatter、颜色、markLine/markPoint、tooltip 和 renderer 全部由代码固定。依赖只有 pinned `echarts@6.1.0`，无 CDN、Vega、G2 或自研 SVG/Canvas geometry。

严格 CSP：`script-src 'self'`，不包含 `unsafe-eval`。

## Contract Finding

当前候选 `ChartArtifact v2` 能表达存量多系列结构，但不能完整声明“选择两个周期，对多个 series 做 difference，并产生新的可见 grain”。Focused candidate 的固定 `period_delta` 证明了业务呈现方向，但仍只是隔离语义原型。

因此 R1 增加阻断门禁：正式 Contract 必须新增可确定性复算的 period-delta transform/output-grain，并同步修改 fixture、Python/TypeScript validator、Analysis/Report Skills、Structured Tool、Compatibility Gate 和 Renderer。Renderer 不得在 Contract 外自行猜 baseline/comparison。

## Browser Gate

Chromium 1600×1000：

- 4 SVG / 0 Canvas；
- 首次候选渲染约 50.5ms（同机单次证据，不作跨机器 benchmark）；
- tooltip PASS；
- 趋势 series toggle PASS；
- datum click → Evidence PASS；
- 增量数据表 3 行、两期 Evidence 与 50.0% 复算 PASS；
- JavaScript disabled 时报告标题、核心结论、Evidence 文本和限制仍可读；
- strict CSP 下 0 console error / 0 page error。

Production build：

- JS `596.91 kB / 200.72 kB gzip`;
- CSS `11.53 kB / 3.38 kB gzip`；
- npm audit：0 vulnerabilities。

## Static Projection

同一浏览器报告确定性生成：

- PDF：5 页，`547,963 bytes`；
- PPTX：5 张 16:9 full-slide static images，`401,400 bytes`；
- PDF 文本包含报告标题、四项结论、增量拆解和 `qr_monthly_story#row:4,6`；
- 不含 `file://`、`/Users/`、`localhost` 或 `127.0.0.1`。

PPTX 继续选择同源 static image，不承诺可编辑矢量图；HTML 是交互媒介，PDF/PPTX 不依赖 hover。

## Web Content Rule

用户进一步确认任何 Forge Web 页面都不得出现 slogan、宣传 Hero 或营销文案。H5 candidate 已遵守该规则；生产模板审计与修改见：

- `docs/web-product-content-audit-2026-08-24.md`
- `tests/test_web_product_content.py`

## Evidence Paths

- 实现：`tools/chart-storytelling-echarts-candidate/`
- 截图：`/tmp/forge-echarts-focused/echarts-focused-full.png`
- 增量图：`/tmp/forge-echarts-focused/echarts-contribution.png`
- Browser gate：`/tmp/forge-echarts-focused/browser-gate.json`
- PDF/PPTX gate：`/tmp/forge-echarts-focused/static-gate.json`
- PDF：`/tmp/forge-echarts-focused/echarts-focused-report.pdf`
- PPTX：`/tmp/forge-echarts-focused/echarts-focused-report.pptx`
