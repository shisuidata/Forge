import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import { categoryPareto, categoryRanking, monthly, monthlyLong, storyMeta } from "../src/data.js";

test("all engines consume one evidence-bounded normalized story", () => {
  assert.equal(categoryPareto.length, 10);
  assert.equal(categoryRanking.length, 8);
  assert.equal(monthly.length, 6);
  assert.equal(monthlyLong.length, 18);
  assert.equal(categoryPareto.at(-1).cumulative, 1);
  assert.equal(categoryRanking.reduce((sum, row) => sum + row.sales, 0), storyMeta.totalSales);
  for (const row of monthly) {
    assert.equal(row.direct + row.marketplace + row.retail, row.total);
    assert.match(row.evidence[0], /^qr_monthly_story#row:[1-6]$/);
  }
});

test("the bakeoff is self-hosted and does not implement a fourth chart engine", () => {
  const root = resolve(import.meta.dirname, "..");
  const sources = ["../index.html", "../src/main.js", "../src/renderers/echarts.js", "../src/renderers/vega.js", "../src/renderers/g2.js"]
    .map((path) => readFileSync(resolve(import.meta.dirname, path), "utf8"))
    .join("\n");
  assert.doesNotMatch(sources, /https?:\/\//);
  assert.doesNotMatch(sources, /createElementNS|<svg|CanvasRenderingContext2D/);
  const packageJson = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  assert.deepEqual(
    Object.keys(packageJson.dependencies).filter((name) => ["echarts", "vega", "vega-lite", "@antv/g2"].includes(name)).sort(),
    ["@antv/g2", "echarts", "vega", "vega-lite"],
  );
});
