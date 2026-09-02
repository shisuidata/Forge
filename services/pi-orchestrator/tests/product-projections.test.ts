import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  productDisplayStates,
  productProjectionSchemas,
  productProjectionV1Schema,
  type ProductProjectionContractName,
  validateProductProjection,
} from "../src/product-projections.js";

const fixturesPath = new URL(
  "../../../agent/contracts/product-projection-fixtures.v1.json",
  import.meta.url,
);
const generatedSchemaPath = new URL(
  "../../../agent/contracts/product-projection-v1.schema.json",
  import.meta.url,
);

type FixtureCase = {
  case_id: string;
  value: Record<string, unknown>;
};

type Mutation = {
  op: "set" | "append";
  path: Array<string | number>;
  value: unknown;
};

const fixtures = JSON.parse(readFileSync(fixturesPath, "utf8")) as {
  schema_version: 1;
  valid: Record<ProductProjectionContractName, FixtureCase[]>;
  invalid: Array<{
    case_id: string;
    base_case_id: string;
    contract: ProductProjectionContractName;
    expected_code: string;
    mutation: Mutation;
  }>;
};

function mutate(value: Record<string, unknown>, mutation: Mutation): Record<string, unknown> {
  const result = structuredClone(value);
  let target: unknown = result;
  for (const segment of mutation.path.slice(0, -1)) {
    if (Array.isArray(target) && typeof segment === "number") {
      target = target[segment];
    } else if (typeof target === "object" && target !== null && typeof segment === "string") {
      target = (target as Record<string, unknown>)[segment];
    } else {
      throw new Error(`invalid mutation path at ${String(segment)}`);
    }
  }
  const final = mutation.path.at(-1);
  if (mutation.op === "append") {
    if (typeof target !== "object" || target === null || typeof final !== "string") {
      throw new Error("append mutation target is invalid");
    }
    const array = (target as Record<string, unknown>)[final];
    if (!Array.isArray(array)) throw new Error("append mutation requires an array");
    array.push(structuredClone(mutation.value));
    return result;
  }
  if (Array.isArray(target) && typeof final === "number") {
    target[final] = structuredClone(mutation.value);
  } else if (typeof target === "object" && target !== null && typeof final === "string") {
    (target as Record<string, unknown>)[final] = structuredClone(mutation.value);
  } else {
    throw new Error("set mutation target is invalid");
  }
  return result;
}

function validById(): Map<string, { contract: ProductProjectionContractName; value: Record<string, unknown> }> {
  const entries: Array<[string, { contract: ProductProjectionContractName; value: Record<string, unknown> }]> = [];
  for (const [contract, cases] of Object.entries(fixtures.valid) as Array<
    [ProductProjectionContractName, FixtureCase[]]
  >) {
    for (const candidate of cases) {
      entries.push([candidate.case_id, { contract, value: candidate.value }]);
    }
  }
  return new Map(entries);
}

test("Product Projection v1 fixtures cover every public contract and product state", () => {
  assert.equal(fixtures.schema_version, 1);
  assert.deepEqual(
    Object.keys(fixtures.valid).sort(),
    Object.keys(productProjectionSchemas).sort(),
  );
  const serialized = JSON.stringify(fixtures.valid);
  for (const state of [
    "needs_input",
    "waiting_decision",
    "running",
    "partial",
    "ready",
    "failed",
    "completed",
  ]) {
    assert.match(serialized, new RegExp(`\\b${state}\\b`), state);
  }
  assert.ok(productDisplayStates.includes("forbidden"));
  assert.ok(productDisplayStates.includes("offline"));
});

test("Product Projection v1 accepts all shared positive fixtures", () => {
  for (const [contract, cases] of Object.entries(fixtures.valid) as Array<
    [ProductProjectionContractName, FixtureCase[]]
  >) {
    assert.ok(cases.length > 0, contract);
    for (const candidate of cases) {
      assert.deepEqual(
        validateProductProjection(contract, candidate.value),
        [],
        `${contract}: ${candidate.case_id}`,
      );
    }
  }
});

test("Product Projection v1 rejects shared contract and semantic mutations with stable codes", () => {
  const valid = validById();
  assert.ok(fixtures.invalid.length >= 12);
  for (const candidate of fixtures.invalid) {
    const base = valid.get(candidate.base_case_id);
    assert.ok(base, candidate.base_case_id);
    assert.equal(base.contract, candidate.contract, candidate.case_id);
    const errors = validateProductProjection(
      candidate.contract,
      mutate(base.value, candidate.mutation),
    );
    assert.ok(
      errors.includes(candidate.expected_code),
      `${candidate.case_id}: expected ${candidate.expected_code}, got ${errors.join(", ")}`,
    );
  }
});

test("Product Projection generated JSON Schema stays synchronized with TypeBox", () => {
  const generated = JSON.parse(readFileSync(generatedSchemaPath, "utf8"));
  assert.deepEqual(generated, productProjectionV1Schema);
});

test("Product Projection v1 fails closed on cross-scope, secret-like and broken-lineage fixtures", () => {
  const requiredCases = new Set([
    "conversation_cross_scope_rejected",
    "secret_like_field_rejected",
    "presentation_artifact_must_exist",
    "partial_requires_reason",
    "oversized_report_title",
  ]);
  const actual = new Set(fixtures.invalid.map((candidate) => candidate.case_id));
  for (const caseId of requiredCases) assert.ok(actual.has(caseId), caseId);
});
