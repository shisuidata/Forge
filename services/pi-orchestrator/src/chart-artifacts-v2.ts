export type ChartV2Type = "horizontal_bar" | "line" | "stacked_area" | "pareto" | "dot_plot" | "table";
export type ChartPurpose = "ranking" | "trend" | "composition" | "contribution" | "variance" | "relationship";
export type ChartQualityStatus = "ready" | "degraded" | "blocked";

export interface ChartV2EvidenceSeries {
  series_id: string;
  label: string;
  field: string;
  evidence_refs: string[];
}

export interface ChartV2Annotation {
  annotation_id: string;
  type: "callout" | "highlight" | "reference_line" | "range";
  label: string;
  target: { datum_key?: string; series_id?: string; value?: number };
  finding_ref?: string;
  evidence_refs: string[];
}

export interface ChartV2Payload extends Record<string, unknown> {
  chart_id: string;
  chart_type: ChartV2Type;
  title: string;
  purpose: ChartPurpose;
  decision_question: string;
  data_ref: string;
  quality_status: { status: ChartQualityStatus; diagnostics: string[] };
  grain: { key_fields: string[]; display_field: string; unique: true; time_grain?: "day" | "week" | "month" | "quarter" | "year" };
  unit: { kind: "currency" | "number" | "percent" | "duration"; label: string; symbol: string; format: "integer" | "decimal_1" | "decimal_2" | "compact" | "percent_1" };
  encoding: Record<string, { field: string; type: "nominal" | "ordinal" | "quantitative" | "temporal" }>;
  series: ChartV2EvidenceSeries[];
  transforms: Array<Record<string, unknown> & { type: "sort" | "top_n" | "aggregate" | "cumulative_share" }>;
  annotations: ChartV2Annotation[];
  interactions: { tooltip: boolean; series_toggle: boolean; table_toggle: boolean; evidence_link: boolean };
  evidence_refs: string[];
  alt_text: string;
}

const chartTypes = new Set<ChartV2Type>(["horizontal_bar", "line", "stacked_area", "pareto", "dot_plot", "table"]);
const purposes = new Set<ChartPurpose>(["ranking", "trend", "composition", "contribution", "variance", "relationship"]);
const qualityStatuses = new Set<ChartQualityStatus>(["ready", "degraded", "blocked"]);
const unitKinds = new Set(["currency", "number", "percent", "duration"]);
const unitFormats = new Set(["integer", "decimal_1", "decimal_2", "compact", "percent_1"]);
const encodingTypes = new Set(["nominal", "ordinal", "quantitative", "temporal"]);
const timeGrains = new Set(["day", "week", "month", "quarter", "year"]);
const transformTypes = new Set(["sort", "top_n", "aggregate", "cumulative_share"]);
const annotationTypes = new Set(["callout", "highlight", "reference_line", "range"]);
const fieldPattern = /^[A-Za-z_][A-Za-z0-9_.-]{0,127}$/;
const forbiddenMarkup = /<\/?(?:script|style|svg|iframe|object)|(?:javascript|data):/i;
const evidencePattern = /^qr_[A-Za-z0-9_-]+#row:[1-9][0-9]*$/;
const idPattern = /^[a-z][a-z0-9_-]{0,63}$/;
const payloadFields = new Set([
  "chart_id", "chart_type", "title", "purpose", "decision_question", "data_ref",
  "quality_status", "grain", "unit", "encoding", "series", "transforms", "annotations",
  "interactions", "evidence_refs", "alt_text",
]);

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function strings(value: unknown, minimum = 0): value is string[] {
  return Array.isArray(value) && value.length >= minimum &&
    value.every((item) => typeof item === "string" && item.length > 0);
}

function safeText(value: unknown): value is string {
  return typeof value === "string" && value.length > 0 && value.length <= 500 &&
    !value.includes("<") && !value.includes(">") && !forbiddenMarkup.test(value);
}

function evidenceRefs(value: unknown, minimum = 1): value is string[] {
  return strings(value, minimum) && new Set(value).size === value.length &&
    value.every((item) => evidencePattern.test(item));
}

function subset(items: string[], allowed: Set<string>): boolean {
  return items.every((item) => allowed.has(item));
}

function onlyKeys(value: Record<string, unknown>, allowed: ReadonlySet<string>): boolean {
  return Object.keys(value).every((key) => allowed.has(key));
}

function field(value: unknown): value is string {
  return typeof value === "string" && fieldPattern.test(value);
}

export function validateChartV2Payload(value: unknown): string | undefined {
  if (!record(value)) return "chart v2 payload must be an object";
  if (Object.keys(value).some((key) => !payloadFields.has(key))) return "chart v2 contains an unsupported field";
  if (typeof value.chart_id !== "string" || !/^chart_[A-Za-z0-9_-]+$/.test(value.chart_id)) return "chart_id is invalid";
  if (!chartTypes.has(value.chart_type as ChartV2Type)) return "chart_type is invalid";
  if (!safeText(value.title) || !safeText(value.decision_question) || !safeText(value.alt_text)) return "chart text is unsafe or missing";
  if (!purposes.has(value.purpose as ChartPurpose)) return "chart purpose is invalid";
  if (typeof value.data_ref !== "string" || !/^ar_[A-Za-z0-9_-]+$/.test(value.data_ref)) return "chart data_ref is invalid";
  if (!record(value.quality_status) || !onlyKeys(value.quality_status, new Set(["status", "diagnostics"])) ||
      !qualityStatuses.has(value.quality_status.status as ChartQualityStatus) ||
      !strings(value.quality_status.diagnostics)) return "chart quality_status is invalid";
  if (!record(value.grain) || !onlyKeys(value.grain, new Set(["key_fields", "display_field", "unique", "time_grain"])) ||
      value.grain.unique !== true || !strings(value.grain.key_fields, 1) ||
      !(value.grain.key_fields as string[]).every(field) || !field(value.grain.display_field) ||
      (value.grain.time_grain !== undefined && !timeGrains.has(String(value.grain.time_grain)))) {
    return "chart grain must declare a unique key";
  }
  if (!record(value.unit) || !onlyKeys(value.unit, new Set(["kind", "label", "symbol", "format"])) ||
      !unitKinds.has(String(value.unit.kind)) || !unitFormats.has(String(value.unit.format)) ||
      !safeText(value.unit.label) || typeof value.unit.symbol !== "string" || value.unit.symbol.length > 12) {
    return "chart unit is invalid";
  }
  if (!record(value.encoding) || !onlyKeys(value.encoding, new Set(["x", "y", "color", "order", "total"])) ||
      !record(value.encoding.x) || !record(value.encoding.y)) return "chart encoding is invalid";
  for (const channel of Object.values(value.encoding)) {
    if (!record(channel) || !onlyKeys(channel, new Set(["field", "type"])) || !field(channel.field) ||
        !encodingTypes.has(String(channel.type))) return "chart encoding channel is invalid";
  }
  if (!evidenceRefs(value.evidence_refs)) return "chart evidence_refs are invalid";
  const allowedEvidence = new Set(value.evidence_refs);
  if (!Array.isArray(value.series) || value.series.length === 0 || value.series.length > 6) return "chart series are invalid";
  const seriesIds = new Set<string>();
  for (const item of value.series) {
    if (!record(item) || !onlyKeys(item, new Set(["series_id", "label", "field", "evidence_refs"])) ||
        typeof item.series_id !== "string" || !idPattern.test(item.series_id) ||
        seriesIds.has(item.series_id) || !safeText(item.label) || !field(item.field) ||
        !evidenceRefs(item.evidence_refs) || !subset(item.evidence_refs, allowedEvidence)) return "chart series lineage is invalid";
    seriesIds.add(item.series_id);
  }
  if (!Array.isArray(value.transforms) || value.transforms.length > 8) return "chart transforms are invalid";
  for (const transform of value.transforms) {
    if (!record(transform) || !onlyKeys(transform, new Set(["type", "field", "direction", "limit", "remainder_label", "group_by", "operation", "output_field"])) ||
        !transformTypes.has(String(transform.type)) || (transform.field !== undefined && !field(transform.field)) ||
        (transform.output_field !== undefined && !field(transform.output_field))) return "chart transform is invalid";
    if (transform.type === "sort" && (!field(transform.field) || !["ascending", "descending"].includes(String(transform.direction)))) return "sort transform is incomplete";
    if (transform.type === "top_n" && (!field(transform.field) || !Number.isInteger(transform.limit) || Number(transform.limit) < 1 || Number(transform.limit) > 20 || !safeText(transform.remainder_label))) return "top_n transform is incomplete";
    if (transform.type === "cumulative_share" && (!field(transform.field) || !field(transform.output_field))) return "cumulative_share transform is incomplete";
    if (transform.type === "aggregate" && (!field(transform.field) || !field(transform.output_field) || !strings(transform.group_by, 1) || !(transform.group_by as string[]).every(field) || !["sum", "average", "count", "min", "max"].includes(String(transform.operation)))) return "aggregate transform is incomplete";
  }
  if (!Array.isArray(value.annotations) || value.annotations.length > 8) return "chart annotations are invalid";
  for (const annotation of value.annotations) {
    if (!record(annotation) || !onlyKeys(annotation, new Set(["annotation_id", "type", "label", "target", "finding_ref", "evidence_refs"])) ||
        typeof annotation.annotation_id !== "string" || !/^ann_[A-Za-z0-9_-]+$/.test(annotation.annotation_id) ||
        !annotationTypes.has(String(annotation.type)) || !safeText(annotation.label) || !record(annotation.target) ||
        !onlyKeys(annotation.target, new Set(["datum_key", "series_id", "value"])) || Object.keys(annotation.target).length === 0 ||
        (annotation.target.datum_key !== undefined && !safeText(annotation.target.datum_key)) ||
        (annotation.target.value !== undefined && typeof annotation.target.value !== "number") ||
        (annotation.finding_ref !== undefined && (typeof annotation.finding_ref !== "string" || !/^finding_[A-Za-z0-9_-]+$/.test(annotation.finding_ref))) ||
        !evidenceRefs(annotation.evidence_refs) || !subset(annotation.evidence_refs, allowedEvidence)) {
      return "chart annotation lineage is invalid";
    }
    if (typeof annotation.target.series_id === "string" && !seriesIds.has(annotation.target.series_id)) return "chart annotation targets an unknown series";
  }
  if (!record(value.interactions) || !onlyKeys(value.interactions, new Set(["tooltip", "series_toggle", "table_toggle", "evidence_link"]))) return "chart interactions are invalid";
  const interactions = value.interactions;
  if (["tooltip", "series_toggle", "table_toggle", "evidence_link"].some((key) => typeof interactions[key] !== "boolean")) {
    return "chart interactions are invalid";
  }
  const serialized = JSON.stringify(value);
  if (forbiddenMarkup.test(serialized)) return "chart v2 cannot contain markup, script, or data URLs";
  return undefined;
}

export function validateChartV2Story(payloads: unknown[]): string | undefined {
  if (payloads.length === 0 || payloads.length > 4) return "a chart story must contain 1 to 4 charts";
  const questions = new Set<string>();
  const purposesSeen = new Set<string>();
  for (const payload of payloads) {
    const error = validateChartV2Payload(payload);
    if (error !== undefined) return error;
    const chart = payload as ChartV2Payload;
    if (chart.quality_status.status !== "ready") return "only quality-ready charts may enter a published story";
    if (questions.has(chart.decision_question)) return "chart story decision questions must be unique";
    questions.add(chart.decision_question);
    const semanticKey = `${chart.purpose}:${chart.encoding["x"]!.field}:${chart.encoding["y"]!.field}`;
    if (purposesSeen.has(semanticKey)) return "chart story contains a duplicate semantic view";
    purposesSeen.add(semanticKey);
  }
  return undefined;
}

export interface ChartV2QueryResult {
  query_run_id: string;
  columns: string[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
}

function evidenceRow(ref: string, queryRunId: string): number | undefined {
  const prefix = `${queryRunId}#row:`;
  if (!ref.startsWith(prefix)) return undefined;
  const row = Number(ref.slice(prefix.length));
  return Number.isInteger(row) ? row : undefined;
}

function periodIndex(value: string, grain: NonNullable<ChartV2Payload["grain"]["time_grain"]>): number | undefined {
  let match: RegExpExecArray | null;
  if (grain === "year") {
    match = /^(\d{4})$/.exec(value);
    return match === null ? undefined : Number(match[1]);
  }
  if (grain === "quarter") {
    match = /^(\d{4})-Q([1-4])$/.exec(value);
    return match === null ? undefined : Number(match[1]) * 4 + Number(match[2]);
  }
  if (grain === "month") {
    match = /^(\d{4})-(0[1-9]|1[0-2])$/.exec(value);
    return match === null ? undefined : Number(match[1]) * 12 + Number(match[2]);
  }
  if (grain === "week") {
    match = /^(\d{4})-W(0[1-9]|[1-4]\d|5[0-3])$/.exec(value);
    if (match === null) return undefined;
    const year = Number(match[1]);
    const week = Number(match[2]);
    const januaryFourth = Date.UTC(year, 0, 4);
    const isoWeekday = new Date(januaryFourth).getUTCDay() || 7;
    const monday = januaryFourth - (isoWeekday - 1) * 86_400_000 + (week - 1) * 7 * 86_400_000;
    if (new Date(monday + 3 * 86_400_000).getUTCFullYear() !== year) return undefined;
    return Math.floor(monday / (7 * 86_400_000));
  }
  match = /^(\d{4})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$/.exec(value);
  if (match === null) return undefined;
  const milliseconds = Date.parse(`${value}T00:00:00Z`);
  if (Number.isNaN(milliseconds) || new Date(milliseconds).toISOString().slice(0, 10) !== value) return undefined;
  return Math.floor(milliseconds / 86_400_000);
}

function periodsAreContinuous(
  values: string[],
  grain: NonNullable<ChartV2Payload["grain"]["time_grain"]>,
): boolean {
  const indexes = values.map((value) => periodIndex(value, grain));
  return indexes.every((index) => index !== undefined) &&
    indexes.slice(1).every((index, position) => index! - indexes[position]! === 1);
}

export function validateChartV2AgainstQueryResult(
  payload: unknown,
  query: ChartV2QueryResult,
): string | undefined {
  const contractError = validateChartV2Payload(payload);
  if (contractError !== undefined) return contractError;
  const chart = payload as ChartV2Payload;
  if (query.row_count !== query.rows.length || query.rows.some((row) => row.length !== query.columns.length)) {
    return "QueryResult shape is inconsistent";
  }
  if (query.truncated) return "quality-ready charts cannot use a truncated QueryResult";
  if (chart.quality_status.status !== "ready") return "chart is not quality-ready";
  const columnIndexes = new Map(query.columns.map((column, index) => [column, index]));
  const encodingFields = Object.entries(chart.encoding)
    .filter(([channel, spec]) =>
      spec.field !== "series_id" && !(chart.chart_type === "stacked_area" && channel === "y"))
    .map(([, spec]) => spec.field);
  const requiredFields = new Set([
    ...chart.grain.key_fields,
    chart.grain.display_field,
    ...encodingFields,
    ...chart.series.map((series) => series.field),
  ]);
  const derivedFields = new Set(
    chart.transforms
      .map((transform) => transform.output_field)
      .filter((field): field is string => typeof field === "string"),
  );
  if ([...requiredFields].some((field) => !columnIndexes.has(field) && !derivedFields.has(field))) {
    return "chart field is absent from QueryResult lineage";
  }
  const grainIndexes = chart.grain.key_fields.map((field) => columnIndexes.get(field));
  if (grainIndexes.some((index) => index === undefined)) return "grain key must come from QueryResult";
  const grainKeys = query.rows.map((row) => JSON.stringify(grainIndexes.map((index) => row[index!])));
  if (new Set(grainKeys).size !== grainKeys.length) return "QueryResult rows do not have a unique chart grain";
  const displayIndex = columnIndexes.get(chart.grain.display_field);
  if (displayIndex !== undefined) {
    const labels = query.rows.map((row) => row[displayIndex]);
    if (labels.some((label) => typeof label !== "string" || label.length === 0)) {
      return "visible chart grain must be a non-empty string";
    }
    if (new Set(labels).size !== labels.length) return "visible chart grain labels must be unique";
  }
  const temporalChannel = Object.values(chart.encoding).find((channel) => channel.type === "temporal");
  if (temporalChannel !== undefined) {
    if (chart.grain.time_grain === undefined) return "temporal chart grain requires time_grain";
    const temporalIndex = columnIndexes.get(temporalChannel.field);
    if (temporalIndex === undefined) return "temporal field is absent from QueryResult";
    const values = query.rows.map((row) => String(row[temporalIndex]));
    if (!periodsAreContinuous(values, chart.grain.time_grain)) {
      return "temporal chart grain must be continuous and ordered";
    }
  }
  const refs = [
    ...chart.evidence_refs,
    ...chart.series.flatMap((series) => series.evidence_refs),
    ...chart.annotations.flatMap((annotation) => annotation.evidence_refs),
  ];
  if (refs.some((ref) => {
    const row = evidenceRow(ref, query.query_run_id);
    return row === undefined || row < 1 || row > query.row_count;
  })) {
    return "chart evidence is outside the supplied QueryResult";
  }
  if (chart.chart_type === "stacked_area") {
    const yIndex = columnIndexes.get(chart.encoding["total"]?.field ?? "");
    const seriesIndexes = chart.series.map((series) => columnIndexes.get(series.field));
    if (yIndex !== undefined && seriesIndexes.every((index) => index !== undefined)) {
      const invalidTotal = query.rows.some((row) => {
        const expected = row[yIndex];
        const actual = seriesIndexes.reduce((sum, index) => sum + Number(row[index!] ?? Number.NaN), 0);
        return typeof expected !== "number" || !Number.isFinite(actual) || Math.abs(expected - actual) > 1e-6;
      });
      if (invalidTotal) return "stacked series do not reconcile to the encoded total";
    }
  }
  return undefined;
}
