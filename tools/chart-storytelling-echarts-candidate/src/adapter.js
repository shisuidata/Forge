import * as echarts from "echarts/core";
import { BarChart, LineChart } from "echarts/charts";
import {
  AriaComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TooltipComponent,
} from "echarts/components";
import { SVGRenderer } from "echarts/renderers";

import {
  categoryPareto,
  categoryRanking,
  channelContributions,
  currency,
  evidenceFor,
  monthlyRows,
  palette,
  storyMetrics,
} from "./data.js";
import { viewById } from "./story.js";

echarts.use([
  BarChart,
  LineChart,
  AriaComponent,
  GridComponent,
  LegendComponent,
  MarkAreaComponent,
  MarkLineComponent,
  MarkPointComponent,
  TooltipComponent,
  SVGRenderer,
]);

const escapeHtml = (value) => String(value)
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");
const percent = (value) => `${(value * 100).toFixed(0)}%`;
const axisLine = { lineStyle: { color: palette.grid } };
const axisLabel = { color: palette.slate, fontSize: 11 };
const splitLine = { lineStyle: { color: palette.grid } };
const baseTooltip = {
  trigger: "axis",
  confine: true,
  backgroundColor: "rgba(20,36,31,.97)",
  borderColor: "rgba(255,255,255,.2)",
  padding: [10, 12],
  textStyle: { color: "#fff", fontSize: 12 },
  axisPointer: { type: "line", lineStyle: { color: palette.coral, type: "dashed" } },
};

function datum(row, value, extra = {}) {
  return { value, datum: { ...row, value, ...extra } };
}

function rankingOption() {
  const first = categoryRanking[0];
  const second = categoryRanking[1];
  return {
    grid: { left: 88, right: 110, top: 24, bottom: 28, containLabel: true },
    tooltip: {
      ...baseTooltip,
      trigger: "item",
      formatter: ({ data, name }) => `${escapeHtml(name)}<br><b>${currency.format(data.value)}</b><br><small>点击柱形查看数据来源</small>`,
    },
    xAxis: { type: "value", min: 0, axisLabel: { ...axisLabel, formatter: currency.format }, splitLine, axisLine },
    yAxis: {
      type: "category",
      inverse: true,
      data: categoryRanking.map((row) => row.category),
      axisLabel,
      axisTick: { show: false },
      axisLine,
    },
    series: [{
      id: "sales",
      name: "销售额",
      type: "bar",
      barWidth: 25,
      data: categoryRanking.map((row, index) => ({
        ...datum(row, row.sales),
        itemStyle: {
          color: index === 0 ? palette.lime : index === 1 ? palette.mint : index === categoryRanking.length - 1 ? "#d9a08d" : palette.moss,
          borderRadius: [0, 6, 6, 0],
        },
      })),
      label: { show: true, position: "right", formatter: ({ value }) => currency.format(value), color: palette.ink, fontWeight: 700, fontSize: 10 },
      markLine: {
        silent: true,
        symbol: ["none", "none"],
        lineStyle: { color: palette.coral, width: 2 },
        label: {
          show: true,
          formatter: `仅差 ${currency.format(storyMetrics.topGap)} · ${(storyMetrics.topGapRatio * 100).toFixed(1)}%`,
          position: "middle",
          color: palette.coral,
          backgroundColor: palette.card,
          padding: [4, 7],
          borderRadius: 5,
          fontSize: 10,
          fontWeight: 700,
        },
        data: [[{ coord: [second.sales, first.category] }, { coord: [first.sales, first.category] }]],
      },
    }],
  };
}

function paretoOption() {
  return {
    grid: { left: 58, right: 58, top: 42, bottom: 54, containLabel: true },
    tooltip: {
      ...baseTooltip,
      formatter: (items) => {
        const row = items[0]?.data?.datum;
        return row
          ? `${escapeHtml(row.category)}<br>销售额 <b>${currency.format(row.sales)}</b><br>累计贡献 <b>${(row.cumulative * 100).toFixed(1)}%</b><br><small>点击图形查看数据来源</small>`
          : "";
      },
    },
    legend: { top: 0, right: 0, textStyle: { color: palette.slate, fontSize: 10 } },
    xAxis: {
      type: "category",
      data: categoryPareto.map((row) => row.category.slice(0, 2)),
      axisLabel,
      axisTick: { show: false },
      axisLine,
    },
    yAxis: [
      { type: "value", min: 0, name: "销售额", axisLabel: { ...axisLabel, formatter: currency.format }, splitLine, axisLine },
      { type: "value", min: 0, max: 1, interval: 0.2, name: "累计贡献", axisLabel: { ...axisLabel, formatter: percent }, splitLine: { show: false }, axisLine },
    ],
    series: [
      {
        id: "pareto-sales",
        name: "销售额",
        type: "bar",
        barMaxWidth: 30,
        data: categoryPareto.map((row) => ({
          ...datum(row, row.sales),
          itemStyle: { color: palette.pale, borderRadius: [5, 5, 0, 0] },
        })),
      },
      {
        id: "pareto-cumulative",
        name: "累计贡献",
        type: "line",
        yAxisIndex: 1,
        symbolSize: 7,
        lineStyle: { width: 3, color: palette.moss },
        itemStyle: { color: palette.card, borderColor: palette.moss, borderWidth: 2 },
        data: categoryPareto.map((row) => datum(row, row.cumulative)),
        markLine: {
          silent: true,
          symbol: "none",
          lineStyle: { color: palette.coral, type: "dashed" },
          label: { formatter: "80% 经营覆盖线", color: palette.coral, fontWeight: 700, fontSize: 10 },
          data: [{ yAxis: 0.8 }],
        },
        markPoint: {
          symbol: "circle",
          symbolSize: 18,
          label: { show: false },
          itemStyle: { color: palette.lime, borderColor: palette.deep, borderWidth: 2 },
          data: [{ coord: [5, categoryPareto[5].cumulative], value: categoryPareto[5].cumulative }],
        },
      },
    ],
  };
}

function trendOption() {
  return {
    grid: { left: 62, right: 34, top: 48, bottom: 46, containLabel: true },
    tooltip: {
      ...baseTooltip,
      formatter: (items) => `${escapeHtml(items[0]?.axisValueLabel ?? "")}<br>${items.map((item) => `${item.marker}${escapeHtml(item.seriesName)} <b>${currency.format(item.value)}</b>`).join("<br>")}<br><small>点击数据点查看数据来源</small>`,
    },
    legend: { top: 0, right: 0, selectedMode: true, textStyle: { color: palette.slate, fontSize: 10 } },
    xAxis: { type: "category", data: monthlyRows.map((row) => row.monthLabel), axisLabel, axisTick: { show: false }, axisLine },
    yAxis: { type: "value", min: 0, max: 1_000_000, interval: 200_000, axisLabel: { ...axisLabel, formatter: currency.format }, splitLine, axisLine },
    series: [
      {
        id: "actual",
        name: "实际",
        type: "line",
        symbolSize: 8,
        lineStyle: { width: 4, color: palette.moss },
        itemStyle: { color: palette.card, borderColor: palette.moss, borderWidth: 2 },
        data: monthlyRows.map((row) => datum(row, row.total, { series: "实际" })),
        markArea: {
          silent: true,
          itemStyle: { color: "rgba(200,239,114,.12)" },
          label: { color: palette.moss, fontSize: 10, formatter: "反转窗口" },
          data: [[{ xAxis: "4月" }, { xAxis: "6月" }]],
        },
        markPoint: {
          symbolSize: 46,
          label: { color: palette.ink, fontSize: 9, fontWeight: 700 },
          data: [
            { coord: ["4月", monthlyRows[3].total], value: "-6.4%", itemStyle: { color: palette.coral } },
            { coord: ["6月", monthlyRows[5].total], value: "+9.1%", itemStyle: { color: palette.lime } },
          ],
        },
      },
      {
        id: "target",
        name: "目标",
        type: "line",
        symbolSize: 6,
        lineStyle: { width: 2, color: palette.coral, type: "dashed" },
        itemStyle: { color: palette.coral },
        data: monthlyRows.map((row) => datum(row, row.target, { series: "目标" })),
      },
    ],
  };
}

function contributionOption() {
  const sorted = [...channelContributions].sort((left, right) => right.delta - left.delta);
  return {
    grid: { left: 72, right: 130, top: 28, bottom: 42, containLabel: true },
    tooltip: {
      ...baseTooltip,
      trigger: "item",
      formatter: ({ data, name }) => `${escapeHtml(name)}<br>4月 ${currency.format(data.datum.baseline)} → 6月 ${currency.format(data.datum.comparison)}<br>新增 <b>+${currency.format(data.datum.delta)}</b> · ${percent(data.datum.share)}<br><small>点击柱形查看两期数据来源</small>`,
    },
    xAxis: { type: "value", min: 0, max: 100_000, axisLabel: { ...axisLabel, formatter: currency.format }, splitLine, axisLine },
    yAxis: { type: "category", inverse: true, data: sorted.map((row) => row.channel), axisLabel: { ...axisLabel, fontSize: 12 }, axisTick: { show: false }, axisLine },
    series: [{
      id: "channel-delta",
      name: "4→6月新增量",
      type: "bar",
      barWidth: 34,
      data: sorted.map((row, index) => ({
        ...datum(row, row.delta),
        itemStyle: { color: index === 0 ? palette.lime : index === 1 ? palette.moss : palette.mint, borderRadius: [0, 7, 7, 0] },
      })),
      label: {
        show: true,
        position: "right",
        formatter: ({ data }) => `+${currency.format(data.datum.delta)} · ${percent(data.datum.share)}`,
        color: palette.ink,
        fontSize: 11,
        fontWeight: 700,
      },
    }],
  };
}

const optionBuilders = Object.freeze({
  ranking: rankingOption,
  pareto: paretoOption,
  trend: trendOption,
  period_delta: contributionOption,
});

export function buildAllowlistedOption(viewId) {
  const view = viewById(viewId);
  if (!view) throw new Error(`unsupported chart view: ${String(viewId)}`);
  const builder = optionBuilders[view.kind];
  if (!builder) throw new Error(`unsupported chart kind: ${view.kind}`);
  return {
    animation: false,
    aria: { enabled: true, decal: { show: false } },
    textStyle: { fontFamily: '"Avenir Next","PingFang SC",sans-serif', color: palette.ink },
    ...builder(),
  };
}

export function renderAllowlistedStory({ roots, showEvidence }) {
  const charts = new Map();
  for (const [viewId, root] of Object.entries(roots)) {
    const chart = echarts.init(root, null, { renderer: "svg" });
    chart.setOption(buildAllowlistedOption(viewId));
    chart.on("click", (event) => {
      const source = event.data?.datum;
      if (source?.evidence) showEvidence(evidenceFor(source));
    });
    charts.set(viewId, chart);
  }

  const resize = () => charts.forEach((chart) => chart.resize());
  window.addEventListener("resize", resize);
  return {
    toggleSeries(viewId, seriesName) {
      const chart = charts.get(viewId);
      if (!chart) return;
      chart.dispatchAction({ type: "legendToggleSelect", name: seriesName });
    },
    dispose() {
      window.removeEventListener("resize", resize);
      charts.forEach((chart) => chart.dispose());
    },
  };
}
