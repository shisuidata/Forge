import { randomUUID } from "node:crypto";

import {
  validateAdvisoryPayload,
  validateAnalysisPayload,
  validateClarificationPayload,
  validateMetricDefinitionPayload,
  validateQueryResultPayload,
  validateRenderedOutputPayload,
} from "./structured-artifact-tools.js";

export type ArtifactType =
  | "clarification"
  | "metric_definition"
  | "query_result"
  | "analysis"
  | "advisory"
  | "rendered_output";

export interface Artifact<TPayload extends Record<string, unknown> = Record<string, unknown>> {
  artifact_id: string;
  artifact_type: ArtifactType;
  schema_version: 1;
  task_run_id: string;
  producer: string;
  created_at: string;
  payload: TPayload;
}

export interface CreateArtifactInput<TPayload extends Record<string, unknown>> {
  artifactType: ArtifactType;
  taskRunId: string;
  producer: string;
  payload: TPayload;
}

export interface ArtifactStore {
  create<TPayload extends Record<string, unknown>>(
    input: CreateArtifactInput<TPayload>,
  ): Artifact<TPayload>;
  list(taskRunId: string): Artifact[];
  latest(taskRunId: string, artifactType: ArtifactType): Artifact | undefined;
}

export function validateArtifactInput<TPayload extends Record<string, unknown>>(
  input: CreateArtifactInput<TPayload>,
): void {
  const validators: Record<ArtifactType, (value: unknown) => string | undefined> = {
    clarification: validateClarificationPayload,
    metric_definition: validateMetricDefinitionPayload,
    query_result: validateQueryResultPayload,
    analysis: validateAnalysisPayload,
    advisory: validateAdvisoryPayload,
    rendered_output: validateRenderedOutputPayload,
  };
  const validationError = validators[input.artifactType](input.payload);
  if (validationError !== undefined) {
    throw new Error(`Invalid ${input.artifactType} Artifact: ${validationError}`);
  }
  if (input.artifactType === "query_result" && input.producer !== "forge") {
    throw new Error("QueryResultArtifact producer must be forge");
  }
}

export class InMemoryArtifactStore implements ArtifactStore {
  readonly #artifacts = new Map<string, Artifact[]>();

  create<TPayload extends Record<string, unknown>>(
    input: CreateArtifactInput<TPayload>,
  ): Artifact<TPayload> {
    validateArtifactInput(input);
    const artifact: Artifact<TPayload> = {
      artifact_id: `ar_${randomUUID().replaceAll("-", "")}`,
      artifact_type: input.artifactType,
      schema_version: 1,
      task_run_id: input.taskRunId,
      producer: input.producer,
      created_at: new Date().toISOString(),
      payload: structuredClone(input.payload),
    };
    const artifacts = this.#artifacts.get(input.taskRunId) ?? [];
    artifacts.push(artifact);
    this.#artifacts.set(input.taskRunId, artifacts);
    return structuredClone(artifact);
  }

  list(taskRunId: string): Artifact[] {
    return (this.#artifacts.get(taskRunId) ?? []).map((artifact) =>
      structuredClone(artifact),
    );
  }

  latest(taskRunId: string, artifactType: ArtifactType): Artifact | undefined {
    const artifact = [...(this.#artifacts.get(taskRunId) ?? [])]
      .reverse()
      .find((candidate) => candidate.artifact_type === artifactType);
    return artifact === undefined ? undefined : structuredClone(artifact);
  }
}
