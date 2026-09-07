import { randomUUID } from "node:crypto";

import { structuredArtifactContracts, type StructuredArtifactType } from "./contracts.js";
import { validateExecutionPlanPayload } from "./planning.js";
import {
  validateChartPayload,
  validatePublicationPayload,
  validateReportBundlePayload,
  validateTechnicalReportPayload,
} from "./report-artifacts.js";

export type ArtifactType =
  | StructuredArtifactType
  | "execution_plan"
  | "chart"
  | "technical_report"
  | "report_bundle"
  | "publication";

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

const artifactValidators: Record<ArtifactType, { validate: (value: unknown) => string | undefined }> = {
  ...structuredArtifactContracts,
  execution_plan: { validate: validateExecutionPlanPayload },
  chart: { validate: validateChartPayload },
  technical_report: { validate: validateTechnicalReportPayload },
  report_bundle: { validate: validateReportBundlePayload },
  publication: { validate: validatePublicationPayload },
};

export function validateArtifactInput<TPayload extends Record<string, unknown>>(
  input: CreateArtifactInput<TPayload>,
): void {
  if (!Object.hasOwn(artifactValidators, input.artifactType)) throw new Error("Unknown Artifact type");
  const validationError = artifactValidators[input.artifactType].validate(input.payload);
  if (validationError !== undefined) {
    throw new Error(`Invalid ${input.artifactType} Artifact: ${validationError}`);
  }
  if (input.artifactType === "query_result" && input.producer !== "forge") {
    throw new Error("QueryResultArtifact producer must be forge");
  }
}

export class InMemoryArtifactStore implements ArtifactStore {
  readonly #artifacts = new Map<string, Artifact[]>();

  constructor(private readonly assertTask?: (taskRunId: string) => void) {}

  checkpoint(): () => void {
    const snapshot = structuredClone(this.#artifacts);
    return () => {
      this.#artifacts.clear();
      for (const [key, value] of snapshot) this.#artifacts.set(key, value);
    };
  }

  create<TPayload extends Record<string, unknown>>(
    input: CreateArtifactInput<TPayload>,
  ): Artifact<TPayload> {
    validateArtifactInput(input);
    this.assertTask?.(input.taskRunId);
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
