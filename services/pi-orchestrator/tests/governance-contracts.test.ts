import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  governanceCoverage,
  type GovernanceActionCatalogV1,
  type GovernanceContractName,
  validateGovernanceContract,
} from "../src/governance-contracts.js";

const fixturesPath = new URL(
  "../../../agent/contracts/governance-contract-fixtures.v1.json",
  import.meta.url,
);
const catalogPath = new URL(
  "../../../agent/contracts/governance-action-catalog.v1.json",
  import.meta.url,
);

const fixtures = JSON.parse(readFileSync(fixturesPath, "utf8")) as {
  valid: Record<GovernanceContractName, unknown[]>;
  invalid: Record<GovernanceContractName, Array<{ case: string; value: unknown }>>;
};
const catalog = JSON.parse(
  readFileSync(catalogPath, "utf8"),
) as GovernanceActionCatalogV1;

const contractNames: GovernanceContractName[] = [
  "resource_ref_v1",
  "principal_context_v1",
  "agent_mandate_v1",
  "policy_decision_v1",
  "datasource_binding_v1",
  "registry_binding_v1",
];

test("TypeBox Governance contracts accept and reject the shared JSON fixture corpus", () => {
  assert.deepEqual(Object.keys(fixtures.valid).sort(), [...contractNames].sort());
  assert.deepEqual(Object.keys(fixtures.invalid).sort(), [...contractNames].sort());

  for (const name of contractNames) {
    for (const value of fixtures.valid[name]) {
      assert.equal(validateGovernanceContract(name, value), true, `${name} valid fixture`);
    }
    for (const invalidCase of fixtures.invalid[name]) {
      assert.equal(
        validateGovernanceContract(name, invalidCase.value),
        false,
        `${name}: ${invalidCase.case}`,
      );
    }
  }
});

test("Governance Action Catalog has a measurable 100% supported-action denominator", () => {
  assert.equal(validateGovernanceContract("governance_action_catalog_v1", catalog), true);
  const actionNames = catalog.actions.map((action) => action.action);
  assert.equal(new Set(actionNames).size, actionNames.length);
  assert.deepEqual(governanceCoverage(catalog), {
    supported: 14,
    governed: 14,
    coverage: 1,
  });
  assert.equal(catalog.unsupported_high_risk_behavior, "fail_closed");

  for (const action of catalog.actions.filter((candidate) => candidate.risk_level === "high")) {
    assert.equal(action.required_context.principal, true);
    assert.equal(action.required_context.policy_decision, "required");
    assert.equal(action.required_context.human_decision, "required");
    assert.equal(action.failure_policy, "deny");
  }
});
