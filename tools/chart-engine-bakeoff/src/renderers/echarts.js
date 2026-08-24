import * as echarts from "echarts/core";
import { BarChart, LineChart } from "echarts/charts";
import { AriaComponent, GridComponent, LegendComponent, MarkLineComponent, MarkPointComponent, TooltipComponent } from "echarts/components";
import { SVGRenderer } from "echarts/renderers";
import { categoryPareto, categoryRanking, evidenceFor, monthly, palette } from "../data.js";

echarts.use([BarChart, LineChart, AriaComponent, GridComponent, LegendComponent, MarkLineComponent, MarkPointComponent, TooltipComponent, SVGRenderer]);

const currency = new Intl.NumberFormat("zh-CN", { style: "currency", currency: "CNY", notation: "compact", maximumFractionDigits: 0 });
const escapeHtml = (value) => String(value)
  .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;").replaceAll("'", "&#039;");
const base = {
  animation: false,
  aria: { enabled: true },
  textStyle: { fontFamily: '"Avenir Next","PingFang SC",sans-serif', color: palette.ink },
  grid: { left: 62, right: 28, top: 40, bottom: 45, containLabel: true },
};
const axisLine = { lineStyle: { color: palette.grid } };
const axisLabel = { color: palette.slate, fontSize: 10 };
const splitLine = { lineStyle: { color: palette.grid } };
const tooltip = {
  trigger: "axis",
  backgroundColor: "rgba(19,35,30,.96)",
  borderColor: "rgba(255,255,255,.2)",
  textStyle: { color: "#fff", fontSize: 11 },
  axisPointer: { type: "line", lineStyle: { color: palette.coral, type: "dashed" } },
};

function valueDatum(row, value) {
  return { value, datum: { ...row, value } };
}

function mount(root, option, showEvidence) {
  const chart = echarts.init(root, null, { renderer: "svg" });
  chart.setOption({ ...base, ...option });
  chart.on("click", (event) => {
    const datum = event.data?.datum;
    if (datum?.evidence) showEvidence(evidenceFor(datum));
  });
  return chart;
}

export async function renderEngine({ roots, showEvidence }) {
  const charts = [];
  charts.push(mount(roots.ranking, {
    tooltip: {
      ...tooltip,
      trigger: "item",
      formatter: ({ data, name }) => `${escapeHtml(name)}<br/><b>${currency.format(data.value)}</b><br/><small>点击查看 Evidence</small>`,
    },
    grid: { left: 76, right: 40, top: 20, bottom: 22, containLabel: true },
    xAxis: { type: "value", axisLabel: { ...axisLabel, formatter: (value) => currency.format(value) }, splitLine, axisLine },
    yAxis: { type: "category", inverse: true, data: categoryRanking.map((row) => row.category), axisLabel, axisTick: { show: false }, axisLine },
    series: [{
      name: "销售额",
      type: "bar",
      barWidth: 22,
      data: categoryRanking.map((row, index) => ({
        ...valueDatum(row, row.sales),
        itemStyle: { color: index === 0 ? palette.lime : index === categoryRanking.length - 1 ? "#d7a28e" : palette.moss, borderRadius: [0, 6, 6, 0] },
      })),
      label: { show: true, position: "right", formatter: ({ value }) => currency.format(value), color: palette.ink, fontWeight: 700, fontSize: 9 },
    }],
  }, showEvidence));

  charts.push(mount(roots.pareto, {
    tooltip: {
      ...tooltip,
      formatter: (items) => {
        const row = items[0]?.data?.datum;
        return row ? `${escapeHtml(row.category)}<br/>销售额 <b>${currency.format(row.sales)}</b><br/>累计贡献 <b>${(row.cumulative * 100).toFixed(1)}%</b><br/><small>点击图形查看 Evidence</small>` : "";
      },
    },
    legend: { top: 2, right: 5, textStyle: { color: palette.slate, fontSize: 9 } },
    grid: { left: 48, right: 48, top: 48, bottom: 44, containLabel: true },
    xAxis: { type: "category", data: categoryPareto.map((row) => row.category.slice(0, 2)), axisLabel, axisTick: { show: false }, axisLine },
    yAxis: [
      { type: "value", name: "销售额", axisLabel: { ...axisLabel, formatter: (value) => currency.format(value) }, splitLine, axisLine },
      { type: "value", min: 0, max: 1, interval: .2, name: "累计", axisLabel: { ...axisLabel, formatter: (value) => `${Math.round(value * 100)}%` }, splitLine: { show: false }, axisLine },
    ],
    series: [
      { name: "销售额", type: "bar", barMaxWidth: 30, data: categoryPareto.map((row) => ({ ...valueDatum(row, row.sales), itemStyle: { color: palette.pale, borderRadius: [5, 5, 0, 0] } })) },
      {
        name: "累计贡献", type: "line", yAxisIndex: 1, smooth: false, symbolSize: 6,
        lineStyle: { width: 3, color: palette.moss }, itemStyle: { color: palette.white, borderColor: palette.moss, borderWidth: 2 },
        data: categoryPareto.map((row) => valueDatum(row, row.cumulative)),
        markLine: { silent: true, symbol: "none", lineStyle: { color: palette.coral, type: "dashed" }, label: { formatter: "80% 覆盖线", color: palette.coral, fontSize: 9 }, data: [{ yAxis: .8 }] },
      },
    ],
  }, showEvidence));

  charts.push(mount(roots.trend, {
    tooltip: {
      ...tooltip,
      formatter: (items) => `${escapeHtml(items[0]?.axisValueLabel ?? "")}<br/>${items.map((item) => `${item.marker}${escapeHtml(item.seriesName)} <b>${currency.format(item.value)}</b>`).join("<br/>")}<br/><small>点击数据点查看 Evidence</small>`,
    },
    legend: { top: 2, right: 5, selectedMode: true, textStyle: { color: palette.slate, fontSize: 9 } },
    grid: { left: 52, right: 26, top: 48, bottom: 40, containLabel: true },
    xAxis: { type: "category", data: monthly.map((row) => row.monthLabel), axisLabel, axisTick: { show: false }, axisLine },
    yAxis: { type: "value", min: 650000, max: 930000, axisLabel: { ...axisLabel, formatter: (value) => currency.format(value) }, splitLine, axisLine },
    series: [
      {
        name: "实际", type: "line", symbolSize: 7, lineStyle: { width: 4, color: palette.moss }, itemStyle: { color: palette.white, borderColor: palette.moss, borderWidth: 2 },
        data: monthly.map((row) => valueDatum(row, row.total)),
        markPoint: { symbolSize: 42, label: { fontSize: 8, color: palette.ink }, data: [{ name: "四月失速", coord: ["04月", 724000], value: "-6.4%", itemStyle: { color: palette.coral } }] },
      },
      { name: "目标", type: "line", symbolSize: 5, lineStyle: { width: 2, color: palette.coral, type: "dashed" }, itemStyle: { color: palette.coral }, data: monthly.map((row) => valueDatum(row, row.target)) },
    ],
  }, showEvidence));

  charts.push(mount(roots.mix, {
    tooltip: {
      ...tooltip,
      formatter: (items) => `${escapeHtml(items[0]?.axisValueLabel ?? "")}<br/>${items.map((item) => `${item.marker}${escapeHtml(item.seriesName)} <b>${currency.format(item.value)}</b>`).join("<br/>")}<br/><small>点击区域查看 Evidence</small>`,
    },
    legend: { top: 2, right: 5, selectedMode: true, textStyle: { color: palette.slate, fontSize: 9 } },
    grid: { left: 52, right: 24, top: 48, bottom: 40, containLabel: true },
    xAxis: { type: "category", boundaryGap: false, data: monthly.map((row) => row.monthLabel), axisLabel, axisTick: { show: false }, axisLine },
    yAxis: { type: "value", axisLabel: { ...axisLabel, formatter: (value) => currency.format(value) }, splitLine, axisLine },
    series: [
      ["门店", "retail", "#c9dfd1"], ["平台", "marketplace", palette.mint], ["直营", "direct", palette.moss],
    ].map(([name, field, color]) => ({
      name, type: "line", stack: "channel", symbol: "none", lineStyle: { width: 1, color }, areaStyle: { color, opacity: 1 },
      emphasis: { focus: "series" }, data: monthly.map((row) => valueDatum({ ...row, channel: name, value: row[field] }, row[field])),
    })),
  }, showEvidence));

  const resize = () => charts.forEach((chart) => chart.resize());
  window.addEventListener("resize", resize);
  return { dispose: () => { window.removeEventListener("resize", resize); charts.forEach((chart) => chart.dispose()); } };
}
