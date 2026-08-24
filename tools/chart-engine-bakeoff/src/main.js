import { categoryPareto, monthly } from "./data.js";

const engines = {
  echarts: {
    name: "Apache ECharts 6.1",
    renderer: "SVGRenderer",
    load: () => import("./renderers/echarts.js"),
  },
  vega: {
    name: "Vega-Lite 6.4",
    renderer: "Vega SVG View",
    load: () => import("./renderers/vega.js"),
  },
  g2: {
    name: "AntV G2 5.4",
    renderer: "SVG Renderer",
    load: () => import("./renderers/g2.js"),
  },
};

const roots = {
  ranking: document.querySelector("#chart-ranking"),
  pareto: document.querySelector("#chart-pareto"),
  trend: document.querySelector("#chart-trend"),
  mix: document.querySelector("#chart-mix"),
};
const stateNode = document.querySelector("#build-state");
const drawer = document.querySelector("#evidence-drawer");
let activeDispose = () => {};
let renderSequence = 0;

const escapeHtml = (value) => String(value)
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");

function showEvidence(evidence) {
  document.querySelector("#evidence-label").textContent = evidence.label;
  document.querySelector("#evidence-value").textContent = evidence.formattedValue;
  document.querySelector("#evidence-refs").textContent = evidence.refs.join(" · ");
  drawer.dataset.open = "true";
}

function clearRoots() {
  for (const root of Object.values(roots)) root.replaceChildren();
}

async function selectEngine(engineId) {
  const sequence = ++renderSequence;
  const definition = engines[engineId];
  if (!definition) return;
  activeDispose();
  activeDispose = () => {};
  clearRoots();
  stateNode.className = "build-state";
  stateNode.innerHTML = "<i></i>正在加载本地引擎";
  document.querySelectorAll("[data-engine]").forEach((button) => {
    button.setAttribute("aria-selected", String(button.dataset.engine === engineId));
  });
  document.querySelector("#engine-name").textContent = definition.name;
  document.querySelector("#renderer-name").textContent = definition.renderer;
  document.querySelector("#render-time").textContent = "渲染中";
  document.querySelector("#render-output").textContent = "—";
  history.replaceState(null, "", `?engine=${engineId}`);

  const start = performance.now();
  try {
    const module = await definition.load();
    const result = await module.renderEngine({ roots, showEvidence });
    if (sequence !== renderSequence) {
      result.dispose?.();
      return;
    }
    activeDispose = result.dispose ?? (() => {});
    const elapsed = performance.now() - start;
    const svgCount = document.querySelectorAll(".chart-host svg").length;
    const canvasCount = document.querySelectorAll(".chart-host canvas").length;
    document.querySelector("#render-time").textContent = `${elapsed.toFixed(1)} ms`;
    document.querySelector("#render-output").textContent = `${svgCount} SVG · ${canvasCount} Canvas`;
    stateNode.className = "build-state ready";
    stateNode.innerHTML = `<i></i>${definition.name} 已就绪`;
    window.__FORGE_BAKEOFF__ = { engineId, elapsed, svgCount, canvasCount, ready: true };
  } catch (error) {
    console.error(error);
    for (const root of Object.values(roots)) {
      const message = document.createElement("div");
      message.className = "engine-error";
      message.textContent = `渲染失败：${String(error.message ?? error)}`;
      root.replaceChildren(message);
    }
    stateNode.className = "build-state error";
    stateNode.innerHTML = "<i></i>渲染失败";
    window.__FORGE_BAKEOFF__ = { engineId, ready: false, error: String(error) };
  }
}

function tableMarkup(kind) {
  if (kind === "category") {
    return `<table><thead><tr><th>品类</th><th>销售额</th><th>订单数</th><th>Evidence</th></tr></thead><tbody>${categoryPareto.map((row) => `<tr><td>${escapeHtml(row.category)}</td><td>¥${row.sales.toLocaleString()}</td><td>${row.orders.toLocaleString()}</td><td><code>${escapeHtml(row.evidence.join(" · "))}</code></td></tr>`).join("")}</tbody></table>`;
  }
  return `<table><thead><tr><th>月份</th><th>直营</th><th>平台</th><th>门店</th><th>实际</th><th>目标</th><th>Evidence</th></tr></thead><tbody>${monthly.map((row) => `<tr><td>${escapeHtml(row.month)}</td><td>¥${row.direct.toLocaleString()}</td><td>¥${row.marketplace.toLocaleString()}</td><td>¥${row.retail.toLocaleString()}</td><td>¥${row.total.toLocaleString()}</td><td>¥${row.target.toLocaleString()}</td><td><code>${escapeHtml(row.evidence.join(" · "))}</code></td></tr>`).join("")}</tbody></table>`;
}

document.querySelectorAll("[data-engine]").forEach((button) => {
  button.addEventListener("click", () => selectEngine(button.dataset.engine));
});
document.querySelectorAll(".table-button").forEach((button) => {
  button.addEventListener("click", () => {
    const kind = button.dataset.table;
    document.querySelector("#table-title").textContent = kind === "category" ? "品类销售同源数据" : "月度渠道同源数据";
    document.querySelector("#table-content").innerHTML = tableMarkup(kind);
    document.querySelector("#data-dialog").showModal();
  });
});
document.querySelector(".drawer-close").addEventListener("click", () => drawer.dataset.open = "false");
window.addEventListener("beforeunload", () => activeDispose());

const requested = new URLSearchParams(location.search).get("engine");
selectEngine(engines[requested] ? requested : "echarts");
