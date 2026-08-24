import { parse, View } from "vega";
import { compile } from "vega-lite";
import { Handler } from "vega-tooltip";
import { expressionInterpreter } from "vega-interpreter";
import { categoryPareto, categoryRanking, evidenceFor, monthly, monthlyLong, palette, trendLong } from "../data.js";

const tooltipHandler = new Handler({ theme: "dark" });
const config = {
  background: null,
  font: "Avenir Next, PingFang SC, sans-serif",
  view: { stroke: null },
  axis: {
    domainColor: palette.grid,
    gridColor: palette.grid,
    labelColor: palette.slate,
    labelFontSize: 10,
    titleColor: palette.slate,
    titleFontSize: 10,
    tickColor: palette.grid,
  },
  legend: { labelColor: palette.slate, labelFontSize: 10, title: null, orient: "top-right" },
};

const currencyTooltip = { field: "value", type: "quantitative", title: "销售额", format: ",.0f" };

async function mount(root, spec, showEvidence) {
  const runtime = parse(compile({ ...spec, config }).spec, null, { ast: true });
  const view = new View(runtime, { renderer: "svg", container: root, hover: true, expr: expressionInterpreter });
  view.tooltip(tooltipHandler.call);
  view.addEventListener("click", (_event, item) => {
    const datum = item?.datum;
    if (datum?.evidence) showEvidence(evidenceFor(datum));
  });
  await view.runAsync();
  return view;
}

export async function renderEngine({ roots, showEvidence }) {
  const common = { width: "container", height: 290, autosize: { type: "fit", contains: "padding", resize: true } };
  const views = [];
  views.push(await mount(roots.ranking, {
    ...common,
    data: { values: categoryRanking.map((row, index) => ({ ...row, value: row.sales, tone: index === 0 ? "leader" : index === categoryRanking.length - 1 ? "other" : "base" })) },
    mark: { type: "bar", cornerRadiusEnd: 6, cursor: "pointer" },
    encoding: {
      y: { field: "category", type: "ordinal", sort: null, axis: { title: null, ticks: false } },
      x: { field: "sales", type: "quantitative", axis: { title: null, format: "~s" } },
      color: { field: "tone", type: "nominal", scale: { domain: ["leader", "base", "other"], range: [palette.lime, palette.moss, "#d7a28e"] }, legend: null },
      tooltip: [
        { field: "category", type: "nominal", title: "品类" },
        { field: "sales", type: "quantitative", title: "销售额", format: ",.0f" },
        { field: "orders", type: "quantitative", title: "订单数", format: "," },
      ],
    },
  }, showEvidence));

  views.push(await mount(roots.pareto, {
    ...common,
    data: { values: categoryPareto.map((row) => ({ ...row, shortCategory: row.category.slice(0, 2), value: row.sales })) },
    layer: [
      {
        mark: { type: "bar", color: palette.pale, cornerRadiusTopLeft: 5, cornerRadiusTopRight: 5, cursor: "pointer" },
        encoding: {
          x: { field: "shortCategory", type: "ordinal", sort: null, axis: { title: null, labelAngle: 0 } },
          y: { field: "sales", type: "quantitative", axis: { title: "销售额", format: "~s" } },
          tooltip: [{ field: "category", title: "品类" }, { field: "sales", type: "quantitative", title: "销售额", format: ",.0f" }],
        },
      },
      {
        mark: { type: "line", color: palette.moss, strokeWidth: 3, point: { filled: true, fill: palette.white, stroke: palette.moss, strokeWidth: 2, size: 55 }, cursor: "pointer" },
        encoding: {
          x: { field: "shortCategory", type: "ordinal", sort: null },
          y: { field: "cumulative", type: "quantitative", axis: { title: "累计贡献", orient: "right", format: ".0%" }, scale: { domain: [0, 1] } },
          tooltip: [{ field: "category", title: "品类" }, { field: "cumulative", type: "quantitative", title: "累计贡献", format: ".1%" }],
        },
      },
      {
        data: { values: [{ threshold: .8 }] },
        mark: { type: "rule", color: palette.coral, strokeDash: [6, 5] },
        encoding: { y: { field: "threshold", type: "quantitative", scale: { domain: [0, 1] } } },
      },
      {
        data: { values: [{ threshold: .8, label: "80% 覆盖线" }] },
        mark: { type: "text", color: palette.coral, fontSize: 9, fontWeight: 700, align: "right", dx: -5, dy: -7 },
        encoding: { x: { value: "width" }, y: { field: "threshold", type: "quantitative", scale: { domain: [0, 1] } }, text: { field: "label" } },
      },
    ],
    resolve: { scale: { y: "independent" } },
  }, showEvidence));

  const seriesSelection = {
    name: "series_pick",
    select: { type: "point", fields: ["series"] },
    bind: "legend",
  };
  views.push(await mount(roots.trend, {
    ...common,
    data: { values: trendLong },
    layer: [
      {
        params: [seriesSelection],
        mark: { type: "line", point: { filled: true, size: 50 }, strokeWidth: 3, cursor: "pointer" },
        encoding: {
          x: { field: "monthLabel", type: "ordinal", sort: monthly.map((row) => row.monthLabel), axis: { title: null, labelAngle: 0 } },
          y: { field: "value", type: "quantitative", scale: { domain: [650000, 930000] }, axis: { title: null, format: "~s" } },
          color: { field: "series", type: "nominal", scale: { domain: ["实际", "目标"], range: [palette.moss, palette.coral] } },
          strokeDash: { field: "series", type: "nominal", scale: { domain: ["实际", "目标"], range: [[1, 0], [7, 5]] }, legend: null },
          opacity: { condition: { param: "series_pick", value: 1 }, value: .12 },
          tooltip: [{ field: "month", title: "月份" }, { field: "series", title: "系列" }, currencyTooltip],
        },
      },
      {
        data: { values: [{ monthLabel: "04月", value: 724000, label: "低于目标 6.4%" }] },
        mark: { type: "text", color: palette.coral, fontWeight: 700, fontSize: 10, dy: -14 },
        encoding: { x: { field: "monthLabel", type: "ordinal", sort: monthly.map((row) => row.monthLabel) }, y: { field: "value", type: "quantitative", scale: { domain: [650000, 930000] } }, text: { field: "label" } },
      },
    ],
  }, showEvidence));

  const channelSelection = {
    name: "channel_pick",
    select: { type: "point", fields: ["channel"] },
    bind: "legend",
  };
  views.push(await mount(roots.mix, {
    ...common,
    data: { values: monthlyLong },
    params: [channelSelection],
    mark: { type: "area", line: true, cursor: "pointer" },
    encoding: {
      x: { field: "monthLabel", type: "ordinal", sort: monthly.map((row) => row.monthLabel), axis: { title: null, labelAngle: 0 } },
      y: { field: "value", type: "quantitative", stack: "zero", axis: { title: null, format: "~s" } },
      color: { field: "channel", type: "nominal", scale: { domain: ["门店", "平台", "直营"], range: ["#c9dfd1", palette.mint, palette.moss] } },
      opacity: { condition: { param: "channel_pick", value: .95 }, value: .12 },
      order: { field: "channel", sort: "ascending" },
      tooltip: [{ field: "month", title: "月份" }, { field: "channel", title: "渠道" }, currencyTooltip],
    },
  }, showEvidence));

  return { dispose: () => views.forEach((view) => view.finalize()) };
}
