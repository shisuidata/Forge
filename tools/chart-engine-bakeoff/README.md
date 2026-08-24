# Forge Chart Engine Bake-off

隔离的 H5 R0 选型工具。它使用同一份 `ChartArtifact v2` fixture 比较：

- Apache ECharts 6.1（按需模块 + SVGRenderer）
- Vega/Vega-Lite 6.4（SVG View + CSP-safe `vega-interpreter`）
- AntV G2 5.4（SVG Renderer）

该工具不进入生产 Pi package，不读取数据库或 Secret，也不代表同时支持三个生产 Renderer。

## 运行

```bash
cd tools/chart-engine-bakeoff
npm ci
npm test
npm run build
npm run dev
```

打开 `http://127.0.0.1:5173/`，或通过查询参数直接选择：

```text
?engine=echarts
?engine=vega
?engine=g2
```

## 固定边界

- 本地固定依赖，无 CDN。
- CSP 不允许 `unsafe-eval`；Vega 使用官方 interpreter。
- 三个 engine 消费 `tests/fixtures/chart-storytelling/` 的同源数据。
- Chart click 只能回传 fixture 中已有的 QueryResult Evidence refs。
- 数据表 fallback 独立于图表引擎。
- 不手写 SVG、Canvas geometry、tooltip、legend、selection 或 annotation layout。

## 选型维度

1. 交互可发现性；
2. Evidence event bridge；
3. SVG/静态导出；
4. CSP 与治理边界；
5. 无障碍 + 数据表 fallback；
6. bundle、首次渲染与维护复杂度。
