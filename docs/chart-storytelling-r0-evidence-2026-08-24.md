# H5 Chart Storytelling R0 Evidence Pack（2026-08-24）

## 1. Verdict

**R0 Contract / 双场景 / 跨媒介自动化：PASS；用户视觉与交互可发现性门禁：FAIL。**

这不是 H5 Runtime 批准：生产 `ChartArtifact v1`、Analysis/Report Skills、Structured Tool、Skills package revision、Renderer 和 NAS 主链均未替换。R0 只证明一套有界 ChartArtifact v2 可以从同源 Evidence 生成四个非重复决策视图，并在 HTML、PDF 和 PPTX 中保持关键结论；它没有证明当前页面结构具备可接受的产品体验。

用户审阅截图后明确指出：首屏深绿色“从图表堆砌，到决策叙事”区域没有任何报告意义，且看不到任何交互。该判断成立：候选把设计说明误当成产品报告 Hero，占据接近整屏；真实图表和 Evidence 操作在首屏以下，原生 SVG tooltip 又缺少即时反馈；底部元信息使用按钮外观却不可点击。自动化验证“控件能工作”不能推翻用户“控件不可发现”的失败结论。

## 2. 实现范围

### Contract 与验证

- Python JSON Schema：`agent/contracts/chart-artifact-v2.schema.json`
- Python Contract 注册：`agent/contracts/__init__.py` 的 `chart_artifact_v2`
- TypeScript Contract/语义 Gate：`services/pi-orchestrator/src/chart-artifacts-v2.ts`
- 正向/负向测试：
  - `tests/test_chart_storytelling_candidate.py`
  - `services/pi-orchestrator/tests/chart-artifacts-v2.test.ts`
  - `tests/fixtures/chart-storytelling/negative-cases.json`

ChartArtifact v2 固定：

- `purpose / decision_question`
- `grain / unit / encoding / series`
- 有界 deterministic transforms
- Evidence-bound annotations
- HTML 渐进交互声明
- `quality_status`

额外的 QueryResult 语义 Gate 会失败关闭：截断结果、重复可见 grain、未知单位、缺失字段 lineage、非连续月粒度、越界 Evidence，以及 stacked series 无法与 `encoding.total` 对账。

### 两个真实 fixture

1. `tests/fixtures/chart-storytelling/category-comparison.json`
   - 横向排名：回答“谁领先”；Top 7 + 尾部三类合并。
   - Pareto：回答“资源集中在哪里”；六类覆盖 82.2%。
2. `tests/fixtures/chart-storytelling/time-series.json`
   - 实际/目标趋势：回答“何时失速、何时反转”。
   - 渠道结构：回答“增长从哪里来”；四月至六月增量 174K，直营贡献 87K。

四张图没有用不同图型重复同一个 decision question。

### Deterministic R0 Renderer

- 生成工具：`tools/chart_storytelling_candidate.py`
- HTML：安全自包含，无外部图表库、模型 HTML/CSS/script 或自由颜色输入。
- HTML 交互：tooltip/focus、series focus/toggle、table fallback、Annotation → Evidence trace；默认静态首屏已经包含核心结论。
- PDF：Chromium print 投影，5 页；Annotation、单位、质量状态和 Evidence ref 静态存在，不依赖 hover。
- PPTX：5 张 16:9 静态完整投影；R0 使用同源 HTML 渲染的 full-slide image，避免 PowerPoint shape/字体差异造成结论或标注丢失。该候选不承诺可编辑矢量图。

## 3. 视觉与交互验证

桌面 Playwright（1600×1000）：

| 项目 | 结果 |
|---|---|
| Chart card | 4 |
| Evidence-bound annotation | 4 个主卡片，5 个语义标注候选 |
| Console / page error | 0 |
| 横向 overflow | 0 |
| Evidence panel | 可打开、关闭并显示 QueryRun row refs |
| Table fallback | 可展开/收起 |
| Series toggle/focus | 可操作，状态有 `aria-pressed` |
| Print media | 主导航/交互控件降级，核心信息保留 |

首轮视觉严审提出的 P1 已修正：

- Pareto 增加累计占比右侧刻度和 80% 线。
- 趋势图明确标注 `Y 轴从 650K 起`，避免夸大波动。
- KPI 从“六月目标差”修正为“六月超目标”。
- 渠道结构增加直接标签和直营 `+87K` 视觉指引。
- Evidence 入口增强；PDF/PPTX 静态输出直接打印 Evidence refs。

## 4. 跨媒介证据

本地可重建输出：`/tmp/forge-h5-chart-candidate/`

| Artifact | SHA-256 | 验证 |
|---|---|---|
| `h5-chart-storytelling.html` | `4bb2e1ecd72634a8c88832bdd1e4117a838e671f15754d0c825ea2e7442b209c` | 自包含；4 图；DOM/ARIA/交互/console/print PASS |
| `h5-chart-storytelling.pdf` | `eb4b7887de31bbe3a332528a669b65f03a0f022c8636b6b7d11aa74656e213a5` | PDFKit 确认 5 页；文本含单位/质量/Evidence；无 `file://`、本地路径、HTML 文件名或 localhost 泄漏 |
| `h5-chart-storytelling.pptx` | `55db5c0d142b7bfc75481419ef4507f412829e1e747d2ac53b54caa80564a128` | 5 页；每页一个 full-slide static picture，边界精确 16:9，无 hover 依赖 |
| `h5-chart-storytelling.png` | `42b7c9127f1067653d789db9d9881e06b154b591310bfd32477a6da8032bd9be` | 1600×2638 desktop full-page candidate |

上述 hash 对应本次本地 evidence；重新运行生成器时，浏览器/PPTX 包装元数据可能使二进制 hash 变化，数据与 Contract 一致性由 fixture 复算测试和结构检查门禁。

## 5. R1 Skills / Prompt / Renderer 兼容矩阵

R1 不能只替换 Renderer。以下组件必须作为一个发布单元固定 revision：

| Component | 当前生产 | R1 必须提供 | Gate |
|---|---|---|---|
| `business-root-cause-analysis` Skill | finding/hypothesis/建议文本规则，无 Chart Story 语义 | 只提出 chart-worthy finding、decision question、grain/unit、annotation candidate 和 Evidence；不输出颜色/CSS | Skill package revision 与 Analysis Tool schema 一致 |
| `data-analysis-report-writer` Skill | 报告文本结构，无多图去重职责 | 输出有界 Chart Story Plan：1–4 个非重复问题、叙事顺序、finding/evidence refs；允许 0 图 | Report Skill revision 与 Rendered Output Tool schema 一致 |
| `submit_analysis_artifact` | AnalysisArtifact v1 | 新版本携带有界 chart candidate semantics；每个业务解释标注绑定 finding + QueryResult Evidence | Tool schema、validator、Skill prompt 同 revision |
| `submit_rendered_output_artifact` | RenderedOutputArtifact v1 | 新版本声明 chart story selection/order，不控制视觉 | 不允许自由 HTML/Vega/颜色/script |
| `ChartArtifact` | v1 deterministic builder | v2 Planner + QueryResult semantic Gate + immutable Artifact | truncated/duplicate grain/unknown unit/bad evidence fail-closed |
| `skill-executor.ts` | 按 Stage 固定一个 Artifact Tool | 按兼容矩阵选择 v2 Tool，仍只允许一次 terminating submission | 旧 Skill + 新 Tool 或新 Skill + 旧 Tool 均拒绝启动 |
| Skills package | 当前固定 revision | 同时发布上述两个 Skill revision | package hash 固定并进入 model binding gate |
| HTML Renderer | Chart v1 + editorial report | `chart-renderer-v2` 固定视觉 token、布局、ARIA、Evidence 定位和 table fallback | 只接受 schema/semantic gate ready 的 v2 |
| PDF/PPTX Exporter | v1 static projection | 与 HTML 同源 ChartArtifact v2；Annotation、unit、quality、Evidence 静态完整 | 跨媒介数据/排序/标注 snapshot test |
| Compatibility Gate | 无 H5 tuple | 固定 `(skills_package, analysis_tool, report_tool, chart_contract, renderer, exporter)` | 任一不匹配整体 unavailable，不回退自由文本或伪装成 v2 |

建议 R1 使用新的 Artifact schema/version，而不是向现有 v1 静默增加可选字段。Chart v1 与 v2 可在迁移窗口按 Report revision 并存，但同一个 ReportRun 只能选择一套完整兼容 tuple。

## 6. Remaining Risks / Non-goals

- R0 没有证明真实模型能稳定提交新 Analysis/Report Artifact Tool；仍需独立 Compatibility Gate 和真实 model smoke。
- PPTX R0 是静态图片页，保证视觉一致但不可编辑；生产是否需要原生 vector/editable shapes 应由真实用户需求决定，不能以“更可编辑”牺牲跨媒介一致性。
- PDF 的独立第二图页面留白较多，但无截断、泄漏或信息丢失；属于后续排版 P2，不阻塞视觉门禁。
- R0 没有接入生产 Artifact Store、ReportRun、NAS 或旧报告 migration。
- `REQ-2026-08-24-010 / H6` 的可复用报告定义是后续独立能力；H5 只提供可被 Definition 引用的 Chart Story Contract，不包含 Scheduler/rerun。

## 7. 用户门禁

首轮用户门禁已判定 FAIL。修订版必须先完成：删除候选宣传 Hero/伪按钮元信息；首屏直达报告主题、数据范围与新鲜度、质量、执行摘要和第一项决策内容；使用可见 custom tooltip、明确 series 控制、图表/数据表切换与 Evidence drawer feedback。完成后再请用户分别判断：

1. 四张图是否各自提供了不同且值得保留的决策信息；
2. 视觉是否达到 Forge 专业报告的默认基线；
3. Evidence 标注是否清楚但不过度干扰阅读；
4. 是否接受 HTML 渐进交互、PDF 静态报告、PPTX 静态一致投影的媒介分层。

只有上述门禁明确通过后，才可提出 H5 R1 生产实施计划；R0 通过不自动批准 R1。
