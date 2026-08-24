const inlineTypes = new Set(["text", "strong", "emphasis", "superseded", "link", "evidence", "code", "mark"]);
const calloutKinds = new Set(["info", "decision", "warning", "limitation"]);
const safeText = (value) => typeof value === "string" && value.length > 0 && value.length <= 500 && !/[<>]/.test(value);
const revisionPattern = /^(?:criteria|report|definition|registry):[A-Za-z0-9._-]+$/;
const evidencePattern = /^qr_[A-Za-z0-9_-]+#row:[1-9][0-9]*$/;

export const semanticFormatPolicy = Object.freeze({
  inlineTypes: Object.freeze([...inlineTypes]),
  calloutKinds: Object.freeze([...calloutKinds]),
  underline: "link_or_evidence_only",
  superseded: "requires_revision_and_replacement_lineage",
  modelMarkup: false,
  modelClassName: false,
  modelColor: false,
});

export function validateInlineTokens(tokens) {
  if (!Array.isArray(tokens) || tokens.length === 0 || tokens.length > 80) return "inline content must contain 1 to 80 tokens";
  for (const token of tokens) {
    if (!token || typeof token !== "object" || Array.isArray(token) || !inlineTypes.has(token.type)) return "inline token type is not allowlisted";
    if (!safeText(token.text)) return "inline token text is unsafe or missing";
    const allowed = new Set(["type", "text"]);
    if (token.type === "superseded") {
      allowed.add("revisionRef");
      allowed.add("replacementRef");
      if (!revisionPattern.test(token.revisionRef ?? "") || !revisionPattern.test(token.replacementRef ?? "")) return "superseded text requires revision and replacement lineage";
    }
    if (token.type === "link") {
      allowed.add("href");
      if (typeof token.href !== "string" || (!token.href.startsWith("#") && !/^https:\/\//.test(token.href))) return "link target is invalid";
    }
    if (token.type === "evidence") {
      allowed.add("evidenceRef");
      if (!evidencePattern.test(token.evidenceRef ?? "")) return "evidence token requires a QueryResult row";
    }
    if (token.type === "mark") {
      allowed.add("semantic");
      if (!["definition", "review_required"].includes(token.semantic)) return "mark token requires an approved semantic";
    }
    if (Object.keys(token).some((key) => !allowed.has(key))) return "inline token contains an unsupported style field";
  }
  return undefined;
}

export function validateCallout(callout) {
  if (!callout || typeof callout !== "object" || Array.isArray(callout)) return "callout must be an object";
  if (Object.keys(callout).some((key) => !["kind", "title", "tokens", "evidenceRefs"].includes(key))) return "callout contains an unsupported style field";
  if (!calloutKinds.has(callout.kind) || !safeText(callout.title)) return "callout kind or title is invalid";
  const tokenError = validateInlineTokens(callout.tokens);
  if (tokenError) return tokenError;
  if (!Array.isArray(callout.evidenceRefs) || callout.evidenceRefs.some((ref) => !evidencePattern.test(ref))) return "callout evidence is invalid";
  if (callout.kind === "decision" && callout.evidenceRefs.length === 0) return "decision callout requires evidence";
  return undefined;
}
