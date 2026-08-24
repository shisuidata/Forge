# Forge ECharts Focused Candidate

这是 H5 在开源 engine bake-off 后的第二道隔离视觉门禁，只验证 ECharts 是否适合成为 Forge 唯一默认生产图表引擎。

## 本轮变化

- 删除 Chart Engine Lab、宣传 Hero 和多引擎切换；首屏直接展示报告主题、范围、质量和执行摘要。
- 排名图同时编码前两名差距，避免单纯高亮第一名制造“赢家”错觉。
- 趋势图改为零基线，异常通过 Evidence-bound Annotation 表达。
- 原“渠道存量堆叠面积”改为 4→6 月渠道增量贡献图：`87K + 53K + 34K = 174K`，直营占 50%。
- HTML 保留 ECharts tooltip、legend、datum click → Evidence 和同源数据表；PDF/PPTX 为静态完整投影。

## 安全与架构边界

- 仅依赖 pinned `echarts@6.1.0`，无 CDN、Vega、G2 或自研 SVG geometry。
- CSP 不允许 `unsafe-eval`。
- `src/adapter.js` 只接受四个固定 `viewId`，由 allowlisted builder 生成 ECharts Option；不接受 Artifact/模型提供的 Option、formatter、颜色、HTML 或脚本。
- Evidence 仅来自版本化 QueryResult fixture。
- 本目录不修改生产 Pi package、Skills/Prompt、Chart v1、报告 Renderer、NAS 或已发布 Report revision。

## Contract finding

当前候选 `ChartArtifact v2` 能表达存量多系列结构，但不能完整声明“两个周期之间、按多个 series 计算差值并形成新的可见 grain”。本候选使用固定的 `period_delta` 语义定义验证产品价值，并把该缺口视为 R1 阻断项：若用户通过视觉门禁，正式 Contract 必须增加可确定性复算的 period-delta transform/output-grain，再与 Skill、Structured Tool、Compatibility Gate 和 Renderer 同版本发布。Renderer 不得在 Contract 外自行猜测 baseline/comparison。

## 运行

```bash
cd tools/chart-storytelling-echarts-candidate
npm ci
npm test
npm run dev
```

浏览器打开 `http://127.0.0.1:5173/`。

自动验证与跨媒介产物：

```bash
# 从仓库根目录运行，先启动 preview 到 4175
.venv/bin/python tools/chart-storytelling-echarts-candidate/scripts/verify.py \
  --url http://127.0.0.1:4175/ \
  --output-dir /tmp/forge-echarts-focused

.venv/bin/python tools/chart-storytelling-echarts-candidate/scripts/capture.py \
  --url http://127.0.0.1:4175/ \
  --output-dir /tmp/forge-echarts-focused
```
