const allowedKinds = new Set(["ranking", "pareto", "trend", "period_delta"]);
const allowedSources = new Set(["category", "monthly"]);
const allowedKeys = new Set([
  "viewId", "kind", "purpose", "decisionQuestion", "source", "unit", "evidenceRefs",
  "baselineKey", "comparisonKey", "seriesFields",
]);
const evidencePattern = /^qr_[A-Za-z0-9_-]+#row:[1-9][0-9]*$/;

export const storyViews = Object.freeze([
  Object.freeze({
    viewId: "ranking",
    kind: "ranking",
    purpose: "ranking",
    decisionQuestion: "头部品类是否形成足够优势，值得单点押注？",
    source: "category",
    unit: "currency",
    evidenceRefs: Object.freeze(["qr_category_story#row:1", "qr_category_story#row:2"]),
  }),
  Object.freeze({
    viewId: "pareto",
    kind: "pareto",
    purpose: "contribution",
    decisionQuestion: "覆盖 80% 销售额需要经营多少品类？",
    source: "category",
    unit: "percent",
    evidenceRefs: Object.freeze([
      "qr_category_story#row:1", "qr_category_story#row:2", "qr_category_story#row:3",
      "qr_category_story#row:4", "qr_category_story#row:5", "qr_category_story#row:6",
    ]),
  }),
  Object.freeze({
    viewId: "trend",
    kind: "trend",
    purpose: "trend",
    decisionQuestion: "销售何时偏离目标，恢复是否持续？",
    source: "monthly",
    unit: "currency",
    evidenceRefs: Object.freeze(["qr_monthly_story#row:4", "qr_monthly_story#row:6"]),
  }),
  Object.freeze({
    viewId: "contribution",
    kind: "period_delta",
    purpose: "contribution",
    decisionQuestion: "四月至六月的 174K 新增量分别来自哪些渠道？",
    source: "monthly",
    unit: "currency",
    baselineKey: "2026-04",
    comparisonKey: "2026-06",
    seriesFields: Object.freeze(["direct_sales", "marketplace_sales", "retail_sales"]),
    evidenceRefs: Object.freeze(["qr_monthly_story#row:4", "qr_monthly_story#row:6"]),
  }),
]);

export function validateStoryViews(views) {
  if (!Array.isArray(views) || views.length < 1 || views.length > 4) return "story must contain 1 to 4 views";
  const ids = new Set();
  const questions = new Set();
  for (const view of views) {
    if (!view || typeof view !== "object" || Array.isArray(view)) return "view must be an object";
    if (Object.keys(view).some((key) => !allowedKeys.has(key))) return "view contains a non-allowlisted field";
    if (typeof view.viewId !== "string" || ids.has(view.viewId)) return "view id must be unique";
    if (!allowedKinds.has(view.kind) || !allowedSources.has(view.source)) return "view kind or source is not allowlisted";
    if (typeof view.purpose !== "string" || typeof view.decisionQuestion !== "string" || questions.has(view.decisionQuestion)) return "decision question must be unique";
    if (!Array.isArray(view.evidenceRefs) || view.evidenceRefs.length === 0 || view.evidenceRefs.some((ref) => !evidencePattern.test(ref))) return "view evidence is invalid";
    if (view.kind === "period_delta") {
      if (view.baselineKey !== "2026-04" || view.comparisonKey !== "2026-06") return "period delta is outside the approved comparison window";
      if (JSON.stringify(view.seriesFields) !== JSON.stringify(["direct_sales", "marketplace_sales", "retail_sales"])) return "period delta series are not allowlisted";
    } else if (view.baselineKey !== undefined || view.comparisonKey !== undefined || view.seriesFields !== undefined) {
      return "period delta fields are not allowed on this view";
    }
    ids.add(view.viewId);
    questions.add(view.decisionQuestion);
  }
  return undefined;
}

const validationError = validateStoryViews(storyViews);
if (validationError) throw new Error(validationError);

export const viewById = (viewId) => storyViews.find((view) => view.viewId === viewId);
