"""Internal benchmark-v2 API for the authenticated Pi orchestrator."""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, StrictInt

from forge.benchmark_v2 import RESULT_COMPARATOR_REVISION
from forge.benchmark_service import BenchmarkCaseNotFound, CandidateEvaluation, evaluate_candidate
from forge.hard_accuracy_benchmark import _FULL_SUITE_ID, load_suite
from web.auth import require_pi_service_auth

router = APIRouter(
    prefix="/api/internal/benchmark-v2",
    dependencies=[Depends(require_pi_service_auth)],
)


class ContextRequest(BaseModel):
    case_id: str
    protocol_revision: str


class EvaluateRequest(BaseModel):
    case_id: str
    protocol_revision: str
    metric_revision: str
    arm: Literal["forge", "direct"]
    output: Any
    context_snapshot: dict[str, Any]


class ProtocolRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str
    model: str
    case_ids: list[str]
    confirm_model_calls: StrictInt
    protocol_manifest: dict[str, Any] | None = None


@router.post("/protocol")
def protocol_projection(req: ProtocolRequest):
    from forge.bird_benchmark import preflight
    try:
        return preflight(**req.model_dump())
    except (ValueError, OSError, RuntimeError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


def _suite():
    return load_suite(_FULL_SUITE_ID)




@router.get("/suite")
def suite_projection():
    suite = _suite()
    return {
        "metric_revision": RESULT_COMPARATOR_REVISION,
        "suite": suite["manifest"],
        "cases": [
            {
                "case_id": case["case_id"],
                "question_id": case["question_id"],
                "db_id": case["db_id"],
                "difficulty": case["difficulty"],
                "question": case["question"],
                "evidence": case["evidence"],
            }
            for case in suite["cases"]
        ],
    }


@router.post("/context")
def context_projection(req: ContextRequest):
    from forge.bird_benchmark import _contexts, context_hashes, verify_case
    try:
        manifest = verify_case(req.protocol_revision, req.case_id)
        suite = _suite()
        context = _contexts(suite, [req.case_id], manifest["date_context"], manifest["grain_context"],
                            manifest["value_context"], manifest["forge_prompt_revision"])[req.case_id]
        if context_hashes({req.case_id: context})[req.case_id] != manifest["context_hashes"][req.case_id]:
            raise ValueError("Frozen context drift")
        verify_case(req.protocol_revision, req.case_id)
        return {**context, "protocol_revision": req.protocol_revision}
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc



@router.post("/evaluate")
def evaluate_arm(req: EvaluateRequest):
    if req.metric_revision != RESULT_COMPARATOR_REVISION:
        raise HTTPException(status_code=409, detail="Result comparator revision mismatch")
    from forge.bird_benchmark import verify_case
    try:
        verify_case(req.protocol_revision, req.case_id)
        result = evaluate_candidate(CandidateEvaluation(**req.model_dump()), _suite())
        verify_case(req.protocol_revision, req.case_id)
    except BenchmarkCaseNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, OSError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {**result, "protocol_revision": req.protocol_revision}
