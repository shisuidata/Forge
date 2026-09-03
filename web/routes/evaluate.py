"""Public, versioned Evaluate API for external query candidates."""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator

from forge.evaluate import evaluate_query_candidate
from forge.evaluation_runs import (
    EvaluationRunNotFound,
    EvaluationRunStore,
    create_evaluation_run,
)
from web.auth import require_api_auth

router = APIRouter(
    prefix="/api/v1",
    dependencies=[Depends(require_api_auth)],
    tags=["Evaluate"],
)


class ResultSetV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: list[str] = Field(max_length=256)
    rows: list[list[Any]] = Field(max_length=10_000)


class EvaluateRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    question: str = Field(min_length=1, max_length=10_000)
    candidate: dict[str, Any]
    dialect: str = Field(default="auto", min_length=1, max_length=32)
    allowed_tables: list[str] | None = Field(default=None, max_length=10_000)
    expected_result: ResultSetV1 | None = None
    actual_result: ResultSetV1 | None = None

    @model_validator(mode="after")
    def result_pair_is_complete(self) -> "EvaluateRequestV1":
        if (self.expected_result is None) != (self.actual_result is None):
            raise ValueError("expected_result and actual_result must be provided together")
        return self


class FailureV1(BaseModel):
    stage: str
    code: str
    retryable: bool


class CandidateSummaryV1(BaseModel):
    input_kind: Literal["direct_sql", "forge_json", "unknown"]
    candidate_revision: str
    producer_revision: str


class PolicyVerdictV1(BaseModel):
    verdict: Literal["allow_review", "deny"]
    review_required: bool
    execution_authorized: Literal[False]


class ResultComparisonV1(BaseModel):
    status: Literal["not_requested", "not_run", "passed", "failed"]
    correct: bool | None
    verdict: str
    column_mapping: list[int] | None
    contract_revision: str | None


class EvaluationLineageV1(BaseModel):
    request_hash: str
    candidate_revision: str
    producer_revision: str
    assurance_revision: str
    policy_revision: str
    registry_revision: str | None
    assurance_report_hash: str | None
    sql_hash: str | None
    result_contract_revision: str | None


class EvaluateResponseV1(BaseModel):
    evaluation_id: str
    schema_version: Literal[1]
    status: Literal["passed", "failed"]
    candidate: CandidateSummaryV1
    policy: PolicyVerdictV1
    failure: FailureV1 | None
    result_comparison: ResultComparisonV1
    compiled_sql: str | None
    assurance: dict[str, Any] | None
    lineage: EvaluationLineageV1
    evidence_refs: list[str]


@router.post("/evaluate", response_model=EvaluateResponseV1)
def evaluate(req: EvaluateRequestV1) -> dict[str, Any]:
    """Evaluate without executing SQL or bypassing the QueryRun approval path."""
    return evaluate_query_candidate(req.model_dump())


class DatasetRevisionV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=256)
    revision: str = Field(min_length=1, max_length=256)


class ProducerRevisionV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1, max_length=256)
    model: str = Field(min_length=1, max_length=256)
    revision: str = Field(min_length=1, max_length=256)
    prompt_revision: str = Field(min_length=1, max_length=256)
    retrieval_revision: str = Field(min_length=1, max_length=256)


class ExpectedOutcomeV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["passed", "failed"]
    failure_code: str | None = None

    @model_validator(mode="after")
    def failure_code_matches_status(self) -> "ExpectedOutcomeV1":
        if self.status == "passed" and self.failure_code is not None:
            raise ValueError("passed outcome cannot declare failure_code")
        if self.status == "failed" and not self.failure_code:
            raise ValueError("failed outcome requires failure_code")
        return self


class EvaluationCaseV1(EvaluateRequestV1):
    case_id: str = Field(min_length=1, max_length=128)
    expected_outcome: ExpectedOutcomeV1


class EvaluationSuiteV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    suite_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=256)
    dataset: DatasetRevisionV1
    producer: ProducerRevisionV1
    retry_policy_revision: str = Field(min_length=1, max_length=256)
    timeout_policy_revision: str = Field(min_length=1, max_length=256)
    cases: list[EvaluationCaseV1] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def cases_are_reproducible(self) -> "EvaluationSuiteV1":
        case_ids = [case.case_id for case in self.cases]
        if len(set(case_ids)) != len(case_ids):
            raise ValueError("case_id values must be unique")
        total_rows = sum(
            len(result.rows)
            for case in self.cases
            for result in (case.expected_result, case.actual_result)
            if result is not None
        )
        if total_rows > 100_000:
            raise ValueError("evaluation suite exceeds 100000 result rows")
        for case in self.cases:
            if case.candidate.get("producer_revision") != self.producer.revision:
                raise ValueError(
                    "candidate producer_revision must equal suite producer revision"
                )
        return self


class RegressionGateV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_new_failures: int = Field(default=0, ge=0)
    max_pass_rate_drop: float = Field(default=0.0, ge=0.0, le=1.0)


class EvaluationRunRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1]
    suite: EvaluationSuiteV1 | None = None
    suite_revision: str | None = Field(
        default=None, pattern=r"^sha256:[a-f0-9]{64}$"
    )
    baseline_run_id: str | None = Field(default=None, pattern=r"^evr_[a-f0-9]{32}$")
    regression_gate: RegressionGateV1 = Field(default_factory=RegressionGateV1)

    @model_validator(mode="after")
    def suite_source_is_unambiguous(self) -> "EvaluationRunRequestV1":
        if (self.suite is None) == (self.suite_revision is None):
            raise ValueError("provide exactly one of suite or suite_revision")
        return self


class EvaluationAggregateV1(BaseModel):
    total_cases: int
    passed_cases: int
    failed_cases: int
    pass_rate: float
    evaluation_status_counts: dict[str, int]
    failure_code_counts: dict[str, int]


class ObservedOutcomeV1(BaseModel):
    status: Literal["passed", "failed"]
    failure_code: str | None


class EvaluationOutcomeV1(BaseModel):
    case_id: str
    status: Literal["passed", "failed"]
    expected: ExpectedOutcomeV1
    observed: ObservedOutcomeV1
    evaluation: EvaluateResponseV1


class EvaluationConfigurationV1(BaseModel):
    dataset: DatasetRevisionV1
    case_selection_revision: str
    evaluation_basis_revision: str
    producer: ProducerRevisionV1
    retry_policy_revision: str
    timeout_policy_revision: str
    evaluator_revision: str
    metric_revision: str
    candidate_contract_revision: str
    assurance_revision: str
    policy_revision: str
    registry_revisions: list[str]
    dialects: list[str]


class RegressionResultV1(BaseModel):
    status: Literal["not_requested", "passed", "failed", "not_comparable"]
    release_gate: Literal["not_evaluated", "passed", "failed"]
    baseline_run_id: str | None
    comparable: bool | None
    incompatible_dimensions: list[str]
    new_failures: list[str]
    recovered_cases: list[str]
    pass_rate_delta: float | None
    gate: RegressionGateV1


class EvaluationRunManifestV1(BaseModel):
    run_id: str
    schema_version: Literal[1]
    status: Literal["completed"]
    suite_revision: str
    suite: EvaluationSuiteV1
    configuration: EvaluationConfigurationV1
    aggregate: EvaluationAggregateV1
    regression: RegressionResultV1
    outcomes: list[EvaluationOutcomeV1]
    created_at: str


def _evaluation_store() -> EvaluationRunStore:
    return EvaluationRunStore()


@router.post("/evaluation-runs", response_model=EvaluationRunManifestV1)
def create_run(req: EvaluationRunRequestV1) -> dict[str, Any]:
    store = _evaluation_store()
    try:
        suite = (
            req.suite.model_dump()
            if req.suite is not None
            else store.get_suite(req.suite_revision or "")
        )
        return create_evaluation_run(
            store,
            suite,
            baseline_run_id=req.baseline_run_id,
            regression_gate=req.regression_gate.model_dump(),
        )
    except EvaluationRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/evaluation-runs/{run_id}", response_model=EvaluationRunManifestV1)
def get_run(run_id: str) -> dict[str, Any]:
    try:
        return _evaluation_store().get_run(run_id)
    except EvaluationRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/evaluation-suites/{suite_revision}", response_model=EvaluationSuiteV1)
def get_suite(suite_revision: str) -> dict[str, Any]:
    try:
        return _evaluation_store().get_suite(suite_revision)
    except EvaluationRunNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
