# H5 Open-source Chart Engine Bake-off（2026-08-24）

## 1. Scope and Verdict

用户明确要求 Forge 不重复实现成熟图表能力。本轮在隔离工具包 `tools/chart-engine-bakeoff/` 中，用同一份品类横截面与月度多系列 fixture 比较：

- Apache ECharts `6.1.0`（Apache-2.0）
- Vega/Vega-Lite `6.4.x`（BSD-3-Clause）
- AntV G2 `5.4.8`（MIT）

三个 engine 均完成 4 个同题决策视图、SVG browser rendering、tooltip、legend interaction、datum click → QueryResult Evidence、table fallback、严格 CSP 和静态 PDF projection。

**初步推荐：ECharts 作为 Forge 唯一默认生产 engine；Vega-Lite 保留为声明式治理对照，不进入浏览器 Runtime；G2 不进入下一轮。**

这仍是选型建议，不是 H5 R1 Runtime 批准。生产 Pi package、Skills/Prompt、Artifact consumer、Renderer 和 NAS 均未修改。

## 2. Fair Comparison Contract

三者消费完全相同的 normalized data：

- `categoryRanking`：Top 7 + “其他 3 类”；
- `categoryPareto`：10 个品类销售额和累计贡献；
- `trendLong`：实际/目标月度销售额；
- `monthlyLong`：直营/平台/门店月度结构；
- 每个 datum 固定 `qr_*#row:n` Evidence。

页面壳、报告标题、业务结论、颜色 token、数据表和 Evidence drawer 相同。引擎只负责 chart geometry、tooltip、legend、selection、annotation 和 SVG rendering。

固定安全边界：

- 本地 pinned dependencies，无 CDN；
- CSP `script-src 'self'`，不允许 `unsafe-eval`；
- 不接受模型 ECharts Option、Vega spec、G2 spec 或 formatter；
- 不手写 SVG/Canvas geometry；
- Evidence drawer 只接受 normalized fixture 中已有 refs。

## 3. Automated Results

桌面 Chromium 1600×1000，同一机器、每个 engine 6 次独立页面运行。数字只用于本轮相对比较，不宣称跨机器 benchmark。

| Engine | Cold render | Warm median | Engine-specific JS gzip | Browser output | Datum → Evidence | Strict CSP |
|---|---:|---:|---:|---|---|---|
| ECharts | 85.1 ms | 58.5 ms | 193.0 kB | 4 SVG / 0 Canvas | PASS | PASS |
| Vega-Lite + interpreter | 91.0 ms | 85.3 ms | 276.4 kB | 4 SVG / 0 Canvas | PASS | PASS |
| AntV G2 + SVG renderer | 372.5 ms | 368.5 ms | 398.1 kB | 4 SVG / 0 Canvas | PASS | PASS |

Bundle 口径：

- ECharts 按需注册 Bar/Line/Grid/Tooltip/Legend/MarkLine/MarkPoint/ARIA/SVGRenderer，`570.22 kB / 192.99 kB gzip`；
- Vega 主 chunk + 运行时需要的 DSV shared chunk，约 `803.02 kB / 276.37 kB gzip`；
- G2 主 chunk + DSV shared chunk，约 `1,348.02 kB / 398.06 kB gzip`。

静态导出：

| Engine | 4 SVG aggregate | Print PDF | PDF pages |
|---|---:|---:|---:|
| ECharts | 32,371 bytes | 370,115 bytes | 4 |
| Vega-Lite | 85,855 bytes | 368,970 bytes | 4 |
| G2 | 74,279 bytes | 419,780 bytes | 4 |

三份 PDF 都保留报告标题、80% 阈值、四月异常和渠道贡献；内容扫描未发现 `file://`、`/Users/` 或 `localhost` 泄漏。

Browser smoke：三个 engine 均为 `0 console error / 0 page error`，table fallback 10/6 行可打开，图例可点击，排名第一 datum 均返回 `qr_category_story#row:1`。

## 4. CSP Finding

Vega 默认 expression code generation 使用动态 `Function`，在严格 CSP 下首先失败。按官方路径改为：

```text
Vega-Lite compile → Vega parse(..., { ast: true })
                  → View({ expr: expressionInterpreter })
```

之后无需 `unsafe-eval` 即可运行。这个路径成立，但增加 interpreter、运行时成本和集成复杂度；CSP-safe 不代表可以接收不可信任 Vega spec。

ECharts 与 G2 在本轮固定 adapter 下无需 `unsafe-eval`。

## 5. Accessibility Finding

本轮实际渲染中，三个 engine 的 chart SVG 内部均没有键盘可聚焦 datum；测试到的外层 SVG 也没有可直接依赖的统一 role/aria-label。不能因库文档声明 ARIA/a11y 能力，就把真实产物误报为可访问。

Forge 必须继续提供：

- chart host 的业务 `aria-label`；
- 键盘可操作的 legend/外部控制；
- 同源 table fallback；
- Annotation 和结论不依赖 hover；
- 必要时提供 Evidence 列表而不是要求键盘逐点导航。

这些属于报告 shell/a11y projection，不是重新实现图表引擎。

## 6. Qualitative Assessment

### ECharts

优势：

- 本轮最小、最快；
- tooltip、legend、stack、双轴、markLine、markPoint 和 SVGRenderer 完整；
- 四个视图映射直接，Annotation/Evidence event bridge 简单；
- 中文业务图表生态与维护经验充足；
- Apache-2.0 适合私有化商业分发。

风险：

- Option 面过宽，必须由 Forge allowlisted adapter 生成；
- formatter 只能是 Forge 固定函数，不能来自模型/Artifact；
- 无障碍仍依赖报告 shell 和 table fallback。

### Vega/Vega-Lite

优势：

- 声明式 grammar 与 Schema 最接近 Forge governance 思路；
- layer/transform/selection 可表达复杂图表；
- SVG 和 CSP-safe interpreter 路径可用。

风险：

- layered spec 容易出现隐蔽重复 mark。本轮曾因 annotation layer 继承主数据而在 PDF 中产生 10 个重叠“80% 覆盖线”；自动内容扫描才发现并修正；
- client-side compiler/interpreter 更重；可通过服务端预编译优化，但会增加一条部署路径；
- Evidence event 从 compiled Vega item 恢复 datum，需要更复杂的 lineage discipline。

结论：适合作为声明式设计参考或离线 spec compiler，不优于 ECharts 作为 Forge 默认浏览器 engine。

### AntV G2

优势：

- Grammar API 清晰、MIT、SVGRenderer 可用；
- tooltip、legend、stack 和 click event 均可工作；
- 视觉默认值较现代。

风险：

- 本轮 bundle 最大、渲染明显最慢；
- Pareto 双尺度表达需要更多适配，本轮为公平阅读采用归一化柱高；
- 同等功能没有显示出足以抵消成本的独特优势。

结论：本轮停止，不进入下一门。

## 7. Shared Chart-story Finding

引擎选择不能修复错误的 decision question。三者的渠道堆叠面积图都展示“存量结构”，但结论是“4–6 月新增量中直营占 50%”；仅靠底部文字不足以自证。

进入 ECharts focused candidate 时，应把第四张图改为**增量贡献图**或在 4–6 月区间直接编码 baseline、渠道增量与 87K/50% Annotation。该修正属于 Chart Story semantics，不属于自研图表 geometry。

同理，排名图不应仅用高亮第一名强化“赢家”错觉；应以差距 bracket/annotation 强调前两名接近。

## 8. Recommendation and Next Gate

推荐生产方向：

```text
ChartArtifact v2
  → semantic/quality/evidence gate
  → allowlisted ECharts adapter
  → ECharts SVGRenderer (HTML)
  → deterministic SVG/PNG projection (PDF/PPTX)
```

下一门只做 ECharts focused candidate：

1. 删除 engine 实验室外壳，回到真实报告首屏；
2. 修正渠道图的 decision semantics；
3. 定义 allowlisted Option mapper，不允许自由 Option/formatter；
4. 验证 HTML tooltip/legend/Evidence/table 与无 JS/print fallback；
5. 验证 browser SVG 与静态 PDF/PPTX 的数据、Annotation 和 Evidence 一致；
6. 用户再次通过视觉与信息价值门禁后，才提出 R1 Skills/Prompt/Tool/Renderer 同版本实施计划。

## 9. Evidence Paths

- 实现：`tools/chart-engine-bakeoff/`
- 本地截图：`/tmp/forge-chart-engine-bakeoff/`
- 本地 SVG/PDF：`/tmp/forge-chart-engine-bakeoff/exports/`
- 运行：

```bash
cd tools/chart-engine-bakeoff
npm ci
npm test
npm run build
npm run dev
```
