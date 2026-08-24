import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import { buildAllowlistedOption } from "../src/adapter.js";
import { channelContributions, storyMetrics } from "../src/data.js";
import { semanticFormatPolicy, validateCallout, validateInlineTokens } from "../src/semantics.js";
import { storyViews, validateStoryViews } from "../src/story.js";

const root = resolve(import.meta.dirname, "..");

test("the focused story is evidence-bound and contains four distinct decisions", () => {
  assert.equal(validateStoryViews(storyViews), undefined);
  assert.equal(storyViews.length, 4);
  assert.equal(new Set(storyViews.map((view) => view.decisionQuestion)).size, 4);
  assert.equal(storyViews.at(-1).kind, "period_delta");
  assert.deepEqual(storyViews.at(-1).evidenceRefs, ["qr_monthly_story#row:4", "qr_monthly_story#row:6"]);
});

test("the incremental contribution view reconciles to the QueryResult", () => {
  assert.deepEqual(channelContributions.map((row) => row.delta), [87_000, 53_000, 34_000]);
  assert.equal(channelContributions.reduce((sum, row) => sum + row.delta, 0), 174_000);
  assert.equal(storyMetrics.totalGrowth, 174_000);
  assert.equal(storyMetrics.directGrowth, 87_000);
  assert.equal(storyMetrics.directGrowthShare, 0.5);
  assert.ok(channelContributions.every((row) => row.evidence.join("|") === "qr_monthly_story#row:4|qr_monthly_story#row:6"));
});

test("the adapter rejects arbitrary views and emits only allowlisted ECharts options", () => {
  for (const view of storyViews) {
    const option = buildAllowlistedOption(view.viewId);
    assert.equal(option.animation, false);
    assert.equal(option.aria.enabled, true);
    assert.ok(Array.isArray(option.series));
  }
  assert.throws(() => buildAllowlistedOption("model_supplied_option"), /unsupported chart view/);
  const contribution = buildAllowlistedOption("contribution");
  assert.deepEqual(contribution.yAxis.data, ["直营", "平台", "门店"]);
  assert.deepEqual(contribution.series[0].data.map((point) => point.datum.delta), [87_000, 53_000, 34_000]);
});

test("the candidate ships one engine and does not implement chart geometry", () => {
  const pkg = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  assert.deepEqual(Object.keys(pkg.dependencies), ["echarts"]);
  const source = ["index.html", "src/main.js", "src/adapter.js", "src/style.css"]
    .map((name) => readFileSync(resolve(root, name), "utf8"))
    .join("\n");
  assert.doesNotMatch(source, /<svg\b|createElement\(["'](?:svg|canvas)["']|vega|@antv\/g2/i);
  assert.doesNotMatch(source, /stacked_area|渠道结构/);
  assert.match(source, /SVGRenderer/);
});

test("report shell uses an editorial document structure rather than landing-page composition", () => {
  const html = readFileSync(resolve(root, "index.html"), "utf8");
  assert.match(html, /经营分析报告/);
  assert.match(html, /品类组合与增长诊断/);
  assert.match(html, /执行摘要/);
  assert.match(html, /目录与主要结论/);
  assert.match(html, /<figcaption>/);
  assert.match(html, /4→6 月新增 174K/);
  assert.doesNotMatch(html, /CHART ENGINE LAB|从图表堆砌|library-first bake-off|可信数据报告|EXECUTIVE BRIEF|DECISION NOTE|Renderer 边界|版本边界|class="[^"]*(?:hero|card|feature)/i);
  assert.doesNotMatch(html, /\sstyle=/i);
  assert.ok(html.indexOf("品类组合与增长诊断") < html.indexOf("decision-ranking"));
});

test("inline emphasis and callouts are semantic allowlists, not free styling", () => {
  assert.equal(semanticFormatPolicy.underline, "link_or_evidence_only");
  assert.equal(semanticFormatPolicy.modelMarkup, false);
  assert.equal(validateInlineTokens([
    { type: "text", text: "覆盖标准从" },
    { type: "superseded", text: "固定 Top 5", revisionRef: "criteria:r2", replacementRef: "criteria:r3" },
    { type: "strong", text: "覆盖 80% 销售额" },
    { type: "emphasis", text: "描述性分析" },
    { type: "evidence", text: "来源", evidenceRef: "qr_category_story#row:1" },
  ]), undefined);
  assert.match(validateInlineTokens([
    { type: "superseded", text: "旧标准" },
  ]) ?? "", /requires revision/);
  assert.match(validateInlineTokens([
    { type: "strong", text: "结论", color: "#ff0000" },
  ]) ?? "", /unsupported style field/);
  assert.equal(validateCallout({
    kind: "decision",
    title: "判断",
    tokens: [{ type: "strong", text: "直营贡献 50%" }],
    evidenceRefs: ["qr_monthly_story#row:4", "qr_monthly_story#row:6"],
  }), undefined);
  assert.match(validateCallout({
    kind: "decision",
    title: "判断",
    tokens: [{ type: "text", text: "没有证据" }],
    evidenceRefs: [],
  }) ?? "", /requires evidence/);
});
