export const BENCHMARK_RUN_STATUSES = [
  "queued", "running", "pausing", "paused", "stopping", "stopped", "completed", "failed", "interrupted",
] as const;
export type BenchmarkRunStatus = (typeof BENCHMARK_RUN_STATUSES)[number];
export type BenchmarkArm = "forge" | "direct";
export type BenchmarkNodeStatus = "pending" | "running" | "passed" | "failed" | "skipped" | "cancelled";

export const BENCHMARK_FAILURE_STAGES = [
  "context", "generation", "parse", "candidate_contract", "compile",
  "assurance", "execution", "result_contract", "official_ea",
] as const;
export type BenchmarkFailureStage = (typeof BENCHMARK_FAILURE_STAGES)[number];

export const BENCHMARK_FAILURE_CODES = [
  "retrieval_insufficient", "context_failed", "agent_failed", "generation_empty", "malformed_output",
  "candidate_contract_invalid", "compile_failed", "readonly_violation", "sql_parse_failed",
  "unknown_table", "unknown_column", "unknown_schema_reference", "dialect_unsupported",
  "execution_timeout", "execution_failed", "result_row_count_mismatch",
  "result_column_count_mismatch", "result_column_alignment_ambiguous",
  "result_order_or_value_mismatch", "result_value_mismatch", "official_ea_mismatch",
] as const;
export type BenchmarkFailureCode = (typeof BENCHMARK_FAILURE_CODES)[number];

export interface BenchmarkFailureV1 {
  stage: BenchmarkFailureStage;
  code: BenchmarkFailureCode;
  retryable: boolean;
}

export interface BenchmarkModelSnapshot {
  provider: string;
  model: string;
  revision: string;
  temperature: number;
  max_output_tokens: number;
}

export interface RetrievalRoundV2 {
  round_index: number;
  top_k: number;
  selected_tables: string[];
  selected_fields: string[];
  relationship_paths: string[];
  concept_coverage: number;
  join_connected: boolean;
  sufficient: boolean;
}

export interface ResultContractV2 {
  required_output_semantics: string[];
  column_order_significant: boolean;
  row_order_significant: boolean;
  duplicate_policy: "set" | "multiset";
  numeric_mode: "exact" | "rounded" | "tolerance";
  numeric_scale: number | null;
  null_policy: "exact";
  expected_grain: string;
  revision: string;
}

export interface ContextSnapshotV2 {
  question: string;
  evidence: string;
  question_concepts: string[];
  tables: string[];
  fields: string[];
  relationships: string[];
  retrieval_rounds: RetrievalRoundV2[];
  sufficiency_status: "sufficient" | "retrieval_insufficient";
  result_contract: ResultContractV2;
  content_hash: string;
}

export interface ArmMetricsV2 {
  generation_ms: number | null;
  prompt_tokens: number;
  completion_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  total_tokens: number;
  compile_status: "pending" | "passed" | "failed" | "not_applicable";
  execution_status: "pending" | "passed" | "failed" | "skipped";
  official_ea: boolean | null;
  contract_accuracy: boolean | null;
  failure?: BenchmarkFailureV1 | null;
  error_code: BenchmarkFailureCode | null;
  sql: string | null;
  output: unknown;
}

export interface BenchmarkCaseProjectionV2 {
  case_id: string;
  question_id: number;
  db_id: string;
  difficulty: string;
  question: string;
  status: BenchmarkNodeStatus;
  current_stage: string;
  context_snapshot: ContextSnapshotV2 | null;
  failure?: BenchmarkFailureV1 | null;
  forge: ArmMetricsV2;
  direct: ArmMetricsV2;
  winner: BenchmarkArm | "tie" | null;
  started_at: string | null;
  completed_at: string | null;
}

export interface BenchmarkDagNodeV2 {
  id: string;
  label: string;
  lane: "shared" | BenchmarkArm | "evaluation";
  status: BenchmarkNodeStatus;
  detail: string;
}

export interface BenchmarkRunProjectionV2 {
  schema_version: 2;
  projection_type: "pi_benchmark_run_v2";
  run_id: string;
  task_run_id: string;
  status: BenchmarkRunStatus;
  model: BenchmarkModelSnapshot;
  suite_id: string;
  total_cases: number;
  completed_cases: number;
  total_calls: number;
  completed_calls: number;
  current_case: { case_id: string; question: string } | null;
  controls: { can_pause: boolean; can_resume: boolean; can_stop: boolean };
  dag: BenchmarkDagNodeV2[];
  metrics: Record<string, unknown>;
  cases: BenchmarkCaseProjectionV2[];
  sequence: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface BenchmarkLogV2 {
  log_id: number;
  run_id: string;
  case_id: string | null;
  arm: BenchmarkArm | "shared" | null;
  stage: string;
  level: "info" | "success" | "warning" | "error";
  message: string;
  payload: Record<string, unknown>;
  created_at: string;
}
