import categoryFixture from "../../../tests/fixtures/chart-storytelling/category-comparison.json" with { type: "json" };
import monthlyFixture from "../../../tests/fixtures/chart-storytelling/time-series.json" with { type: "json" };

export const currency = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "CNY",
  notation: "compact",
  maximumFractionDigits: 1,
});

export const categoryRows = Object.freeze(categoryFixture.query_result.rows.map((row, index) => Object.freeze({
  category: row[0],
  sales: row[1],
  orders: row[2],
  evidence: Object.freeze([`${categoryFixture.query_result.query_run_id}#row:${index + 1}`]),
})));

const totalSales = categoryRows.reduce((sum, row) => sum + row.sales, 0);
let cumulativeSales = 0;
export const categoryPareto = Object.freeze(categoryRows.map((row) => {
  cumulativeSales += row.sales;
  return Object.freeze({ ...row, cumulative: cumulativeSales / totalSales });
}));

export const categoryRanking = Object.freeze([
  ...categoryRows.slice(0, 7),
  Object.freeze({
    category: "其他 3 类",
    sales: categoryRows.slice(7).reduce((sum, row) => sum + row.sales, 0),
    orders: categoryRows.slice(7).reduce((sum, row) => sum + row.orders, 0),
    evidence: Object.freeze(categoryRows.slice(7).flatMap((row) => row.evidence)),
  }),
]);

export const monthlyRows = Object.freeze(monthlyFixture.query_result.rows.map((row, index) => Object.freeze({
  month: row[0],
  monthLabel: `${Number(row[0].slice(5))}月`,
  direct: row[1],
  marketplace: row[2],
  retail: row[3],
  total: row[4],
  target: row[5],
  evidence: Object.freeze([`${monthlyFixture.query_result.query_run_id}#row:${index + 1}`]),
})));

const baseline = monthlyRows.find((row) => row.month === "2026-04");
const comparison = monthlyRows.find((row) => row.month === "2026-06");
if (!baseline || !comparison) throw new Error("focused candidate requires the fixed April and June evidence rows");

const totalGrowth = comparison.total - baseline.total;
const channelDefinitions = [
  ["直营", "direct"],
  ["平台", "marketplace"],
  ["门店", "retail"],
];

export const channelContributions = Object.freeze(channelDefinitions.map(([channel, field]) => {
  const start = baseline[field];
  const end = comparison[field];
  const delta = end - start;
  return Object.freeze({
    channel,
    field,
    baseline: start,
    comparison: end,
    delta,
    share: delta / totalGrowth,
    evidence: Object.freeze([...baseline.evidence, ...comparison.evidence]),
  });
}));

export const evidenceFor = (datum) => Object.freeze({
  label: datum.category ?? datum.channel ?? `${datum.monthLabel ?? datum.month} · ${datum.series ?? "销售额"}`,
  formattedValue: datum.delta !== undefined
    ? `${datum.delta >= 0 ? "+" : ""}${currency.format(datum.delta)} · ${(datum.share * 100).toFixed(0)}% 新增量`
    : currency.format(datum.sales ?? datum.value ?? datum.total ?? 0),
  refs: Object.freeze([...(datum.evidence ?? [])]),
});

export const storyMetrics = Object.freeze({
  totalSales,
  totalSalesLabel: currency.format(totalSales),
  topGap: categoryRows[0].sales - categoryRows[1].sales,
  topGapRatio: (categoryRows[0].sales - categoryRows[1].sales) / categoryRows[1].sales,
  topSixShare: categoryFixture.expected.top_six_share,
  totalGrowth,
  directGrowth: channelContributions[0].delta,
  directGrowthShare: channelContributions[0].share,
  aprilGapRatio: monthlyFixture.expected.april_gap_ratio,
  juneAboveTargetRatio: monthlyFixture.expected.june_above_target_ratio,
});

export const palette = Object.freeze({
  ink: "#14241f",
  paper: "#f3f0e7",
  card: "#fffdf8",
  deep: "#123f31",
  moss: "#2f745d",
  mint: "#82bfa1",
  lime: "#c8ef72",
  coral: "#d96f58",
  pale: "#dce9e1",
  grid: "#dfe5df",
  slate: "#697971",
});
