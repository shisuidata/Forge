import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import {
  type ChartV2QueryResult,
  validateChartV2AgainstQueryResult,
  validateChartV2Payload,
  validateChartV2Story,
} from "../src/chart-artifacts-v2.js";

function fixture(name: string): Record<string, unknown> {
  return JSON.parse(readFileSync(resolve(process.cwd(), "../../tests/fixtures/chart-storytelling", name), "utf8")) as Record<string, unknown>;
}

function payloads(name: string): Record<string, unknown>[] {
  const charts = fixture(name).charts;
  assert.ok(Array.isArray(charts));
  return charts.map((chart) => (chart as Record<string, unknown>).payload as Record<string, unknown>);
}

function queryResult(name: string): ChartV2QueryResult {
  return fixture(name).query_result as ChartV2QueryResult;
}

function setPath(target: unknown, path: Array<string | number>, value: unknown): void {
  let cursor = target;
  for (const segment of path.slice(0, -1)) {
    if (Array.isArray(cursor) && typeof segment === "number") cursor = cursor[segment];
    else if (typeof cursor === "object" && cursor !== null && typeof segment === "string") {
      cursor = (cursor as Record<string, unknown>)[segment];
    } else throw new Error(`invalid mutation path at ${String(segment)}`);
  }
  const final = path.at(-1);
  if (Array.isArray(cursor) && typeof final === "number") cursor[final] = value;
  else if (typeof cursor === "object" && cursor !== null && typeof final === "string") {
    (cursor as Record<string, unknown>)[final] = value;
  } else throw new Error(`invalid final mutation path at ${String(final)}`);
}

test("ChartArtifact v2 accepts the category and time-series evidence stories", () => {
  for (const name of ["category-comparison.json", "time-series.json"]) {
    const story = payloads(name);
    const query = queryResult(name);
    assert.equal(validateChartV2Story(story), undefined);
    for (const payload of story) {
      assert.equal(validateChartV2Payload(payload), undefined);
      assert.equal(validateChartV2AgainstQueryResult(payload, query), undefined);
    }
  }
});

test("ChartArtifact v2 rejects an annotation whose evidence is outside the chart lineage", () => {
  const payload = structuredClone(payloads("category-comparison.json")[0]!);
  const annotations = payload.annotations as Array<Record<string, unknown>>;
  annotations[0]!.evidence_refs = ["qr_other#row:1"];
  assert.match(validateChartV2Payload(payload) ?? "", /annotation lineage/);
});

test("ChartArtifact v2 rejects model-controlled markup or script URLs", () => {
  const payload = structuredClone(payloads("category-comparison.json")[0]!);
  payload.title = "<script>alert(1)</script>";
  assert.match(validateChartV2Payload(payload) ?? "", /unsafe|markup/);
});

test("ChartArtifact v2 story rejects repeated decision views and non-ready quality", () => {
  const story = payloads("category-comparison.json");
  const duplicate = structuredClone(story[0]!);
  assert.match(validateChartV2Story([story[0]!, duplicate]) ?? "", /decision questions/);
  const degraded = structuredClone(story[0]!);
  (degraded.quality_status as Record<string, unknown>).status = "degraded";
  assert.match(validateChartV2Story([degraded]) ?? "", /quality-ready/);
});

test("ChartArtifact v2 fails closed on duplicate grain, truncation, and unknown unit", () => {
  const payload = payloads("category-comparison.json")[0]!;
  const duplicateGrain = structuredClone(queryResult("category-comparison.json"));
  duplicateGrain.rows[1]![0] = duplicateGrain.rows[0]![0];
  assert.match(validateChartV2AgainstQueryResult(payload, duplicateGrain) ?? "", /unique chart grain|labels/);

  const truncated = structuredClone(queryResult("category-comparison.json"));
  truncated.truncated = true;
  assert.match(validateChartV2AgainstQueryResult(payload, truncated) ?? "", /truncated/);

  const unknownUnit = structuredClone(payload);
  (unknownUnit.unit as Record<string, unknown>).kind = "unknown";
  (unknownUnit.unit as Record<string, unknown>).symbol = null;
  assert.match(validateChartV2AgainstQueryResult(unknownUnit, queryResult("category-comparison.json")) ?? "", /unit is invalid/);
});

test("ChartArtifact v2 rejects discontinuous time and stacked totals that do not reconcile", () => {
  const story = payloads("time-series.json");
  const discontinuous = structuredClone(queryResult("time-series.json"));
  discontinuous.rows[3]![0] = "2026-07";
  assert.match(validateChartV2AgainstQueryResult(story[0]!, discontinuous) ?? "", /continuous and ordered/);

  const inconsistent = structuredClone(queryResult("time-series.json"));
  inconsistent.rows[0]![1] = Number(inconsistent.rows[0]![1]) + 1;
  assert.match(validateChartV2AgainstQueryResult(story[1]!, inconsistent) ?? "", /reconcile/);
});

test("versioned ChartArtifact v2 negative fixture remains fail-closed", () => {
  const corpus = fixture("negative-cases.json");
  const cases = corpus.cases as Array<{
    case_id: string;
    fixture: string;
    chart_index: number;
    target: "chart" | "query";
    path: Array<string | number>;
    value: unknown;
    expected_pattern: string;
  }>;
  assert.equal(cases.length, 8);
  for (const candidate of cases) {
    const chart = structuredClone(payloads(candidate.fixture)[candidate.chart_index]!);
    const query = structuredClone(queryResult(candidate.fixture));
    setPath(candidate.target === "chart" ? chart : query, candidate.path, candidate.value);
    const error = validateChartV2AgainstQueryResult(chart, query) ?? "";
    assert.match(error, new RegExp(candidate.expected_pattern), candidate.case_id);
  }
});
