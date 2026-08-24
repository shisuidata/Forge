import { renderAllowlistedStory } from "./adapter.js";
import {
  categoryRows,
  channelContributions,
  currency,
  monthlyRows,
  storyMetrics,
} from "./data.js";
import { storyViews } from "./story.js";

const params = new URLSearchParams(location.search);
const slideView = params.get("media") === "slide" ? params.get("view") : undefined;
const allowedSlides = new Set(["cover", "ranking", "pareto", "trend", "contribution"]);
if (slideView && allowedSlides.has(slideView)) {
  document.body.classList.add("slide-capture");
  document.body.dataset.capture = slideView;
  const selected = document.querySelector(`[data-slide="${slideView}"]`);
  document.querySelectorAll("main > *").forEach((node) => {
    if (node !== selected) node.remove();
  });
}

const stateNode = document.querySelector("#report-state");
const drawer = document.querySelector("#evidence-drawer");
const dialog = document.querySelector("#data-dialog");
const roots = Object.fromEntries([
  ["ranking", document.querySelector("#chart-ranking")],
  ["pareto", document.querySelector("#chart-pareto")],
  ["trend", document.querySelector("#chart-trend")],
  ["contribution", document.querySelector("#chart-contribution")],
].filter(([, root]) => root));

let controls = { dispose() {}, toggleSeries() {} };

function showEvidence({ label, formattedValue, refs }) {
  if (!drawer) return;
  document.querySelector("#evidence-label").textContent = label;
  document.querySelector("#evidence-value").textContent = formattedValue ?? "";
  document.querySelector("#evidence-refs").textContent = refs.join(" · ");
  drawer.dataset.open = "true";
}

const evidenceByView = Object.freeze({
  ranking: {
    label: "头部两类销售额差距",
    formattedValue: `${currency.format(storyMetrics.topGap)} · ${(storyMetrics.topGapRatio * 100).toFixed(1)}%`,
    refs: storyViews[0].evidenceRefs,
  },
  pareto: {
    label: "六类经营覆盖线",
    formattedValue: `${(storyMetrics.topSixShare * 100).toFixed(1)}%`,
    refs: storyViews[1].evidenceRefs,
  },
  trend: {
    label: "四月偏差与六月恢复",
    formattedValue: "−6.4% → +9.1%",
    refs: storyViews[2].evidenceRefs,
  },
  contribution: {
    label: "四月至六月渠道新增量",
    formattedValue: `+${currency.format(storyMetrics.totalGrowth)} · 直营 ${currency.format(storyMetrics.directGrowth)}`,
    refs: storyViews[3].evidenceRefs,
  },
});

function addCell(row, value, asCode = false) {
  const cell = document.createElement("td");
  const content = asCode ? document.createElement("code") : document.createTextNode(String(value));
  if (asCode) content.textContent = String(value);
  cell.append(content);
  row.append(cell);
}

function buildTable(headers, rows) {
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  headers.forEach((header) => {
    const cell = document.createElement("th");
    cell.textContent = header;
    headRow.append(cell);
  });
  head.append(headRow);
  const body = document.createElement("tbody");
  rows.forEach((values) => {
    const row = document.createElement("tr");
    values.forEach((value, index) => addCell(row, value, index === values.length - 1));
    body.append(row);
  });
  table.append(head, body);
  return table;
}

function tableDefinition(kind) {
  if (kind === "category") {
    return {
      title: "品类销售同源数据",
      headers: ["品类", "销售额", "订单数", "Evidence"],
      rows: categoryRows.map((row) => [row.category, currency.format(row.sales), row.orders.toLocaleString(), row.evidence.join(" · ")]),
    };
  }
  if (kind === "contribution") {
    return {
      title: "四月至六月渠道增量复算",
      headers: ["渠道", "四月", "六月", "增量", "贡献占比", "Evidence"],
      rows: channelContributions.map((row) => [
        row.channel,
        currency.format(row.baseline),
        currency.format(row.comparison),
        `+${currency.format(row.delta)}`,
        `${(row.share * 100).toFixed(1)}%`,
        row.evidence.join(" · "),
      ]),
    };
  }
  return {
    title: "月度实际、目标与渠道同源数据",
    headers: ["月份", "直营", "平台", "门店", "实际", "目标", "Evidence"],
    rows: monthlyRows.map((row) => [
      row.month,
      currency.format(row.direct),
      currency.format(row.marketplace),
      currency.format(row.retail),
      currency.format(row.total),
      currency.format(row.target),
      row.evidence.join(" · "),
    ]),
  };
}

function wireInteractions() {
  document.querySelectorAll(".evidence-button").forEach((button) => {
    button.addEventListener("click", () => showEvidence(evidenceByView[button.dataset.view]));
  });
  document.querySelectorAll(".table-button").forEach((button) => {
    button.addEventListener("click", () => {
      const definition = tableDefinition(button.dataset.table);
      document.querySelector("#table-title").textContent = definition.title;
      document.querySelector("#table-content").replaceChildren(buildTable(definition.headers, definition.rows));
      dialog.showModal();
    });
  });
  document.querySelectorAll("[data-series]").forEach((button) => {
    button.addEventListener("click", () => {
      const selected = button.getAttribute("aria-pressed") === "true";
      button.setAttribute("aria-pressed", String(!selected));
      controls.toggleSeries("trend", button.dataset.series);
    });
  });
  document.querySelector("#print-report")?.addEventListener("click", () => window.print());
  document.querySelector(".drawer-close")?.addEventListener("click", () => drawer.dataset.open = "false");
}

async function start() {
  wireInteractions();
  const startedAt = performance.now();
  try {
    if (Object.keys(roots).length > 0) controls = renderAllowlistedStory({ roots, showEvidence });
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const svgCount = document.querySelectorAll(".chart-host svg").length;
    const canvasCount = document.querySelectorAll(".chart-host canvas").length;
    const elapsed = performance.now() - startedAt;
    if (stateNode) {
      stateNode.className = "report-state ready";
      stateNode.replaceChildren(document.createElement("i"), document.createTextNode(`图表已加载 · ${elapsed.toFixed(1)} ms`));
    }
    document.body.dataset.ready = "true";
    window.__FORGE_ECHARTS_CANDIDATE__ = { ready: true, svgCount, canvasCount, elapsed, slideView: slideView ?? null };
  } catch (error) {
    console.error(error);
    Object.values(roots).forEach((root) => {
      const message = document.createElement("div");
      message.className = "engine-error";
      message.textContent = `图表渲染失败：${String(error.message ?? error)}`;
      root.replaceChildren(message);
    });
    if (stateNode) {
      stateNode.className = "report-state error";
      stateNode.replaceChildren(document.createElement("i"), document.createTextNode("图表渲染失败"));
    }
    document.body.dataset.ready = "false";
    window.__FORGE_ECHARTS_CANDIDATE__ = { ready: false, error: String(error), slideView: slideView ?? null };
  }
}

window.addEventListener("beforeunload", () => controls.dispose());
start();
