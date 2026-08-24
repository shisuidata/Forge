import { Chart } from "@antv/g2";
import { Renderer as SVGRenderer } from "@antv/g-svg";
import { categoryPareto, categoryRanking, evidenceFor, monthly, monthlyLong, palette, trendLong } from "../data.js";

const currency = new Intl.NumberFormat("zh-CN", { style: "currency", currency: "CNY", notation: "compact", maximumFractionDigits: 0 });
const theme = {
  type: "classic",
  color: palette.moss,
  view: { viewFill: "transparent" },
};

function chartAt(root) {
  return new Chart({ container: root, autoFit: true, height: 340, renderer: new SVGRenderer() });
}

function evidenceClick(chart, showEvidence) {
  chart.on("element:click", (event) => {
    const datum = event.data?.data;
    if (datum?.evidence) showEvidence(evidenceFor(datum));
  });
}

function tooltip(mark, fields) {
  return mark.tooltip(fields.map((field) => ({
    name: field.name,
    field: field.field,
    valueFormatter: field.currency ? (value) => currency.format(Number(value)) : undefined,
  })));
}

export async function renderEngine({ roots, showEvidence }) {
  const charts = [];

  const ranking = chartAt(roots.ranking);
  ranking.theme(theme);
  ranking.coordinate({ transform: [{ type: "transpose" }] });
  ranking.scale("color", { domain: ["leader", "base", "other"], range: [palette.lime, palette.moss, "#d7a28e"] });
  ranking.legend("color", false);
  ranking.axis("x", { title: false, tick: false, labelFontSize: 10, labelFill: palette.slate });
  ranking.axis("y", { title: false, labelFormatter: "~s", gridStroke: palette.grid, labelFill: palette.slate });
  const rankingMark = ranking.interval()
    .data(categoryRanking.map((row, index) => ({ ...row, tone: index === 0 ? "leader" : index === categoryRanking.length - 1 ? "other" : "base" })))
    .encode("x", "category").encode("y", "sales").encode("color", "tone")
    .style("radiusTopRight", 6).style("radiusBottomRight", 6).style("cursor", "pointer");
  tooltip(rankingMark, [{ name: "品类", field: "category" }, { name: "销售额", field: "sales", currency: true }, { name: "订单数", field: "orders" }]);
  evidenceClick(ranking, showEvidence);
  charts.push(ranking);

  const maxCategorySales = Math.max(...categoryPareto.map((row) => row.sales));
  const paretoData = categoryPareto.map((row) => ({ ...row, shortCategory: row.category.slice(0, 2), salesRatio: row.sales / maxCategorySales * .46 }));
  const pareto = chartAt(roots.pareto);
  pareto.theme(theme);
  pareto.scale("y", { domain: [0, 1] });
  pareto.axis("x", { title: false, tick: false, labelFontSize: 9, labelFill: palette.slate });
  pareto.axis("y", { title: false, labelFormatter: ".0%", gridStroke: palette.grid, labelFill: palette.slate });
  const bars = pareto.interval().data(paretoData).encode("x", "shortCategory").encode("y", "salesRatio")
    .style("fill", palette.pale).style("radiusTopLeft", 5).style("radiusTopRight", 5).style("cursor", "pointer");
  tooltip(bars, [{ name: "品类", field: "category" }, { name: "销售额", field: "sales", currency: true }]);
  const contribution = pareto.line().data(paretoData).encode("x", "shortCategory").encode("y", "cumulative")
    .style("stroke", palette.moss).style("lineWidth", 3).style("cursor", "pointer");
  tooltip(contribution, [{ name: "品类", field: "category" }, { name: "累计贡献", field: "cumulative" }]);
  pareto.point().data(paretoData).encode("x", "shortCategory").encode("y", "cumulative")
    .style("fill", palette.white).style("stroke", palette.moss).style("lineWidth", 2).style("r", 4).style("cursor", "pointer");
  pareto.lineY().data([.8]).style("stroke", palette.coral).style("lineDash", [6, 5]);
  pareto.text().data([{ shortCategory: "电子", cumulative: .82, label: "80% 覆盖线" }])
    .encode("x", "shortCategory").encode("y", "cumulative").encode("text", "label")
    .style("fill", palette.coral).style("fontSize", 9).style("fontWeight", 700);
  evidenceClick(pareto, showEvidence);
  charts.push(pareto);

  const trend = chartAt(roots.trend);
  trend.theme(theme);
  trend.scale("color", { domain: ["实际", "目标"], range: [palette.moss, palette.coral] });
  trend.scale("y", { domain: [650000, 930000] });
  trend.axis("x", { title: false, tick: false, labelFill: palette.slate });
  trend.axis("y", { title: false, labelFormatter: "~s", gridStroke: palette.grid, labelFill: palette.slate });
  trend.legend("color", { position: "top", itemLabelFontSize: 10 });
  const trendLine = trend.line().data(trendLong).encode("x", "monthLabel").encode("y", "value").encode("color", "series")
    .style("lineWidth", 3).style("cursor", "pointer");
  tooltip(trendLine, [{ name: "月份", field: "month" }, { name: "系列", field: "series" }, { name: "销售额", field: "value", currency: true }]);
  trend.point().data(trendLong).encode("x", "monthLabel").encode("y", "value").encode("color", "series")
    .style("fill", palette.white).style("lineWidth", 2).style("r", 4).style("cursor", "pointer");
  trend.text().data([{ monthLabel: "04月", value: 724000, label: "低于目标 6.4%" }])
    .encode("x", "monthLabel").encode("y", "value").encode("text", "label")
    .style("dy", -15).style("fill", palette.coral).style("fontWeight", 700).style("fontSize", 10);
  evidenceClick(trend, showEvidence);
  charts.push(trend);

  const mix = chartAt(roots.mix);
  mix.theme(theme);
  mix.scale("color", { domain: ["门店", "平台", "直营"], range: ["#c9dfd1", palette.mint, palette.moss] });
  mix.axis("x", { title: false, tick: false, labelFill: palette.slate });
  mix.axis("y", { title: false, labelFormatter: "~s", gridStroke: palette.grid, labelFill: palette.slate });
  mix.legend("color", { position: "top", itemLabelFontSize: 10 });
  const area = mix.area().data(monthlyLong).transform({ type: "stackY" })
    .encode("x", "monthLabel").encode("y", "value").encode("color", "channel")
    .style("fillOpacity", .92).style("cursor", "pointer");
  tooltip(area, [{ name: "月份", field: "month" }, { name: "渠道", field: "channel" }, { name: "销售额", field: "value", currency: true }]);
  mix.line().data(monthlyLong).transform({ type: "stackY" })
    .encode("x", "monthLabel").encode("y", "value").encode("color", "channel").style("lineWidth", 1);
  evidenceClick(mix, showEvidence);
  charts.push(mix);

  await Promise.all(charts.map((chart) => chart.render()));
  return { dispose: () => charts.forEach((chart) => chart.destroy()) };
}
