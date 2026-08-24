import categoryFixture from "../../../tests/fixtures/chart-storytelling/category-comparison.json" with { type: "json" };
import monthlyFixture from "../../../tests/fixtures/chart-storytelling/time-series.json" with { type: "json" };

const compactCurrency = new Intl.NumberFormat("zh-CN", {
  style: "currency",
  currency: "CNY",
  notation: "compact",
  maximumFractionDigits: 0,
});

const categoryRows = categoryFixture.query_result.rows.map((row, index) => ({
  category: row[0],
  sales: row[1],
  orders: row[2],
  evidence: [`${categoryFixture.query_result.query_run_id}#row:${index + 1}`],
}));

let cumulativeSales = 0;
const totalSales = categoryRows.reduce((sum, row) => sum + row.sales, 0);
export const categoryPareto = categoryRows.map((row) => {
  cumulativeSales += row.sales;
  return { ...row, cumulative: cumulativeSales / totalSales };
});

export const categoryRanking = [
  ...categoryRows.slice(0, 7),
  {
    category: "其他 3 类",
    sales: categoryRows.slice(7).reduce((sum, row) => sum + row.sales, 0),
    orders: categoryRows.slice(7).reduce((sum, row) => sum + row.orders, 0),
    evidence: categoryRows.slice(7).flatMap((row) => row.evidence),
  },
];

export const monthly = monthlyFixture.query_result.rows.map((row, index) => ({
  month: row[0],
  monthLabel: `${row[0].slice(5)}月`,
  direct: row[1],
  marketplace: row[2],
  retail: row[3],
  total: row[4],
  target: row[5],
  evidence: [`${monthlyFixture.query_result.query_run_id}#row:${index + 1}`],
}));

export const monthlyLong = monthly.flatMap((row) => [
  { month: row.month, monthLabel: row.monthLabel, channel: "直营", value: row.direct, evidence: row.evidence },
  { month: row.month, monthLabel: row.monthLabel, channel: "平台", value: row.marketplace, evidence: row.evidence },
  { month: row.month, monthLabel: row.monthLabel, channel: "门店", value: row.retail, evidence: row.evidence },
]);

export const trendLong = monthly.flatMap((row) => [
  { month: row.month, monthLabel: row.monthLabel, series: "实际", value: row.total, evidence: row.evidence },
  { month: row.month, monthLabel: row.monthLabel, series: "目标", value: row.target, evidence: row.evidence },
]);

export const evidenceFor = (datum) => ({
  label: datum.category ?? `${datum.monthLabel ?? datum.month} · ${datum.series ?? datum.channel ?? "销售额"}`,
  value: datum.sales ?? datum.value ?? datum.total,
  formattedValue: compactCurrency.format(datum.sales ?? datum.value ?? datum.total ?? 0),
  refs: datum.evidence ?? [],
});

export const storyMeta = {
  totalSales,
  totalSalesLabel: compactCurrency.format(totalSales),
  topThreeShare: categoryFixture.expected.top_three_share,
  topSixShare: categoryFixture.expected.top_six_share,
  aprilGapRatio: monthlyFixture.expected.april_gap_ratio,
  juneAboveTargetRatio: monthlyFixture.expected.june_above_target_ratio,
  growth: monthlyFixture.expected.april_to_june_growth,
  directGrowth: monthlyFixture.expected.direct_growth,
};

export const palette = {
  ink: "#13231e",
  paper: "#f7f4ea",
  moss: "#2f745d",
  mint: "#80bfa0",
  lime: "#c8ef72",
  coral: "#e27a5f",
  pale: "#dce9e1",
  grid: "#dfe5df",
  slate: "#718079",
  white: "#fffdf7",
};
