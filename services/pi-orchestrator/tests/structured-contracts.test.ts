import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { validateStructuredArtifact, type StructuredArtifactType } from "../src/contracts.js";
import {
  ArtifactSubmissionError, createAdvisorySubmissionTool, createClarificationSubmissionTool,
} from "../src/structured-artifact-tools.js";

const corpus = JSON.parse(readFileSync(
  new URL("../../../agent/contracts/structured-artifact-fixtures.v1.json", import.meta.url), "utf8",
)) as { cases: Array<{ id: string; contract: string; accepted: boolean; instance: Record<string, unknown> }> };

for (const item of corpus.cases) {
  test(`shared structured artifact semantics: ${item.id}`, () => {
    const artifactType = item.contract.replace(/_artifact$/, "") as StructuredArtifactType;
    assert.equal(validateStructuredArtifact(artifactType, item.instance) === undefined, item.accepted);
  });
}

test("submission rejects invalid boundary data without terminating or retaining it", async () => {
  const submission = createClarificationSubmissionTool();
  for (const id of ["clarification-invalid-date", "payload-unknown-property"]) {
    const payload = corpus.cases.find((item) => item.id === id)!.instance.payload;
    await assert.rejects(
      () => submission.tool.execute("invalid", payload as never, undefined, undefined, {} as never),
      ArtifactSubmissionError,
    );
    assert.equal(submission.getSubmitted(), undefined);
  }
});

test("query-evidence advisory cannot substitute supplied context for query rows", async () => {
  const contextRef = `ctx_${"a".repeat(24)}`;
  const submission = createAdvisorySubmissionTool({
    skillName: "funnel-analysis", allowedEvidenceRefs: new Set([contextRef, "qr_corpus#row:1"]),
    requiresQueryEvidence: true,
  });
  const base = corpus.cases.find((item) => item.id === "advisory-valid")!.instance.payload;
  const payload = { ...(base as Record<string, unknown>), skill_name: "funnel-analysis" };
  await assert.rejects(
    () => submission.tool.execute("context", payload as never, undefined, undefined, {} as never),
    ArtifactSubmissionError,
  );
  assert.equal(submission.getSubmitted(), undefined);
  const accepted = { ...payload, findings: [{ statement: "Observed value is one.", evidence_refs: ["qr_corpus#row:1"], confidence: "high" }] };
  const result = await submission.tool.execute("query", accepted as never, undefined, undefined, {} as never);
  assert.equal(result.terminate, true);
});
