import { Type, type Static, type TSchema } from "typebox";
import { Value } from "typebox/value";

const id = Type.String({ minLength: 1, maxLength: 256 });
const organizationId = Type.String({ minLength: 1, maxLength: 128 });
const workspaceId = Type.String({ minLength: 1, maxLength: 128 });
const dateTime = Type.String({ format: "date-time" });

const accountablePrincipalSchema = Type.Object(
  {
    principal_id: id,
    principal_type: Type.Union([
      Type.Literal("human"),
      Type.Literal("team"),
      Type.Literal("organization"),
    ]),
  },
  { additionalProperties: false },
);

export const resourceRefV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    resource_type: Type.Union([
      Type.Literal("organization"),
      Type.Literal("workspace"),
      Type.Literal("datasource"),
      Type.Literal("registry"),
      Type.Literal("schema"),
      Type.Literal("table"),
      Type.Literal("column"),
      Type.Literal("query_result"),
      Type.Literal("report"),
      Type.Literal("export"),
      Type.Literal("model"),
      Type.Literal("skill"),
      Type.Literal("audit"),
    ]),
    resource_id: id,
    organization_id: organizationId,
    workspace_id: Type.Union([workspaceId, Type.Null()]),
    parent_resource_id: Type.Union([id, Type.Null()]),
    resource_revision: Type.Union([id, Type.Null()]),
  },
  { additionalProperties: false },
);

export const principalContextV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    principal_context_id: Type.String({ pattern: "^pc_[A-Za-z0-9_-]+$" }),
    actor_principal: Type.Object(
      {
        principal_id: id,
        principal_type: Type.Union([
          Type.Literal("human"),
          Type.Literal("service"),
          Type.Literal("agent"),
        ]),
      },
      { additionalProperties: false },
    ),
    accountable_principal: accountablePrincipalSchema,
    organization_id: organizationId,
    workspace_id: workspaceId,
    authentication_context: Type.Object(
      {
        method: Type.Union([
          Type.Literal("local"),
          Type.Literal("oidc"),
          Type.Literal("service_key"),
          Type.Literal("oauth"),
          Type.Literal("workload_identity"),
        ]),
        assurance_level: Type.Union([
          Type.Literal("single_factor"),
          Type.Literal("multi_factor"),
          Type.Literal("service_asserted"),
          Type.Literal("workload_asserted"),
        ]),
        authenticated_at: dateTime,
        session_id_hash: Type.Union([
          Type.String({ pattern: "^sha256:[a-f0-9]{64}$" }),
          Type.Null(),
        ]),
      },
      { additionalProperties: false },
    ),
    delegation_chain: Type.Array(
      Type.Object(
        {
          delegation_id: Type.String({ pattern: "^dlg_[A-Za-z0-9_-]+$" }),
          delegator_principal_id: id,
          delegate_principal_id: id,
          mandate_id: Type.Union([
            Type.String({ pattern: "^md_[A-Za-z0-9_-]+$" }),
            Type.Null(),
          ]),
          issued_at: dateTime,
          expires_at: dateTime,
        },
        { additionalProperties: false },
      ),
      { maxItems: 8 },
    ),
    issued_at: dateTime,
    expires_at: dateTime,
  },
  { additionalProperties: false },
);

export const delegatedMandateV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    mandate_id: Type.String({ pattern: "^md_[A-Za-z0-9_-]+$" }),
    revision: Type.Integer({ minimum: 1 }),
    delegate_principal: Type.Object(
      {
        principal_id: id,
        principal_type: Type.Union([
          Type.Literal("service"),
          Type.Literal("agent"),
        ]),
      },
      { additionalProperties: false },
    ),
    delegator_principal: accountablePrincipalSchema,
    accountable_principal: accountablePrincipalSchema,
    organization_id: organizationId,
    workspace_id: workspaceId,
    purpose: Type.String({ minLength: 1, maxLength: 1000 }),
    task_run_id: Type.String({ pattern: "^tr_[A-Za-z0-9_-]+$" }),
    audience: Type.String({ pattern: "^[a-z][a-z0-9_.:-]{0,127}$" }),
    capabilities: Type.Array(
      Type.String({ pattern: "^[a-z][a-z0-9_.:-]{0,127}$" }),
      { minItems: 1, maxItems: 64, uniqueItems: true },
    ),
    resource_scope: Type.Array(resourceRefV1Schema, { minItems: 1, maxItems: 128 }),
    budget_ref: Type.Union([id, Type.Null()]),
    approval_policy_ref: id,
    can_delegate: Type.Literal(false),
    status: Type.Union([
      Type.Literal("active"),
      Type.Literal("revoked"),
      Type.Literal("expired"),
    ]),
    issued_at: dateTime,
    expires_at: dateTime,
  },
  { additionalProperties: false },
);

export const policyDecisionV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    policy_decision_id: Type.String({ pattern: "^pd_[A-Za-z0-9_-]+$" }),
    revision: Type.Integer({ minimum: 1 }),
    subject_principal_id: id,
    mandate_id: Type.Union([
      Type.String({ pattern: "^md_[A-Za-z0-9_-]+$" }),
      Type.Null(),
    ]),
    action: Type.String({
      pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
      maxLength: 128,
    }),
    resource: resourceRefV1Schema,
    effect: Type.Union([
      Type.Literal("allow"),
      Type.Literal("deny"),
      Type.Literal("conditional"),
    ]),
    reason_code: Type.String({ pattern: "^[a-z][a-z0-9_.:-]{0,127}$" }),
    reason: Type.String({ minLength: 1, maxLength: 1000 }),
    obligations: Type.Array(
      Type.Object(
        {
          obligation_id: Type.String({ pattern: "^obl_[A-Za-z0-9_-]+$" }),
          obligation_type: Type.Union([
            Type.Literal("approval"),
            Type.Literal("mask"),
            Type.Literal("row_filter"),
            Type.Literal("audit"),
            Type.Literal("expiry"),
            Type.Literal("budget"),
            Type.Literal("read_only"),
          ]),
          enforcement_point: Type.Union([
            Type.Literal("pi"),
            Type.Literal("forge"),
            Type.Literal("report_service"),
            Type.Literal("database"),
          ]),
          description: Type.String({ minLength: 1, maxLength: 500 }),
        },
        { additionalProperties: false },
      ),
      { maxItems: 32 },
    ),
    policy_revision: id,
    decision_authority: accountablePrincipalSchema,
    evaluated_at: dateTime,
    expires_at: Type.Union([dateTime, Type.Null()]),
  },
  { additionalProperties: false },
);

export const datasourceBindingV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    datasource_binding_id: Type.String({ pattern: "^dsb_[A-Za-z0-9_-]+$" }),
    revision: Type.Integer({ minimum: 1 }),
    organization_id: organizationId,
    workspace_id: workspaceId,
    datasource: Type.Object(
      {
        schema_version: Type.Literal(1),
        resource_type: Type.Literal("datasource"),
        resource_id: id,
        organization_id: organizationId,
        workspace_id: workspaceId,
        parent_resource_id: Type.Union([id, Type.Null()]),
        resource_revision: Type.Union([id, Type.Null()]),
      },
      { additionalProperties: false },
    ),
    policy_revision: id,
    status: Type.Union([
      Type.Literal("draft"),
      Type.Literal("active"),
      Type.Literal("retired"),
    ]),
    valid_from: dateTime,
    valid_until: Type.Union([dateTime, Type.Null()]),
  },
  { additionalProperties: false },
);

export const registryBindingV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    registry_binding_id: Type.String({ pattern: "^rgb_[A-Za-z0-9_-]+$" }),
    revision: Type.Integer({ minimum: 1 }),
    organization_id: organizationId,
    workspace_id: workspaceId,
    registry: Type.Object(
      {
        schema_version: Type.Literal(1),
        resource_type: Type.Literal("registry"),
        resource_id: id,
        organization_id: organizationId,
        workspace_id: workspaceId,
        parent_resource_id: Type.Union([id, Type.Null()]),
        resource_revision: Type.Union([id, Type.Null()]),
      },
      { additionalProperties: false },
    ),
    datasource_binding_id: Type.String({ pattern: "^dsb_[A-Za-z0-9_-]+$" }),
    registry_revision: id,
    policy_revision: id,
    status: Type.Union([
      Type.Literal("draft"),
      Type.Literal("active"),
      Type.Literal("retired"),
    ]),
    valid_from: dateTime,
    valid_until: Type.Union([dateTime, Type.Null()]),
  },
  { additionalProperties: false },
);

export const governanceActionCatalogV1Schema = Type.Object(
  {
    schema_version: Type.Literal(1),
    catalog_id: Type.Literal("forge-governance-actions"),
    revision: Type.String({ pattern: "^v[0-9]+\\.[0-9]+\\.[0-9]+$" }),
    status: Type.Union([
      Type.Literal("draft"),
      Type.Literal("active"),
      Type.Literal("retired"),
    ]),
    published_at: dateTime,
    unsupported_high_risk_behavior: Type.Literal("fail_closed"),
    actions: Type.Array(
      Type.Object(
        {
          action: Type.String({
            pattern: "^[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)+$",
            maxLength: 128,
          }),
          support_status: Type.Union([
            Type.Literal("supported"),
            Type.Literal("planned"),
            Type.Literal("unsupported"),
          ]),
          contract_status: Type.Union([
            Type.Literal("specified"),
            Type.Literal("incomplete"),
          ]),
          runtime_enforcement_status: Type.Union([
            Type.Literal("not_integrated"),
            Type.Literal("partial"),
            Type.Literal("enforced"),
          ]),
          owner: Type.Union([
            Type.Literal("pi"),
            Type.Literal("pi_governance"),
            Type.Literal("forge"),
            Type.Literal("report_service"),
          ]),
          executor: Type.Union([
            Type.Literal("pi"),
            Type.Literal("forge"),
            Type.Literal("report_service"),
          ]),
          risk_level: Type.Union([
            Type.Literal("low"),
            Type.Literal("medium"),
            Type.Literal("high"),
            Type.Literal("prohibited"),
          ]),
          required_context: Type.Object(
            {
              principal: Type.Literal(true),
              mandate: Type.Union([
                Type.Literal("required"),
                Type.Literal("conditional"),
                Type.Literal("not_required"),
              ]),
              policy_decision: Type.Union([
                Type.Literal("required"),
                Type.Literal("conditional"),
                Type.Literal("not_required"),
              ]),
              human_decision: Type.Union([
                Type.Literal("required"),
                Type.Literal("conditional"),
                Type.Literal("not_required"),
              ]),
              binding_refs: Type.Array(
                Type.Union([Type.Literal("datasource"), Type.Literal("registry")]),
                { uniqueItems: true },
              ),
            },
            { additionalProperties: false },
          ),
          truth_source: Type.Union([
            Type.Literal("pi_task_store"),
            Type.Literal("pi_governance_store"),
            Type.Literal("pi_skill_policy_store"),
            Type.Literal("forge_query_store"),
            Type.Literal("forge_registry_store"),
            Type.Literal("forge_model_control_store"),
            Type.Literal("forge_report_store"),
            Type.Literal("forge_memory_store"),
          ]),
          failure_policy: Type.Union([
            Type.Literal("deny"),
            Type.Literal("pause"),
            Type.Literal("needs_input"),
          ]),
        },
        { additionalProperties: false },
      ),
      { minItems: 1, maxItems: 128 },
    ),
  },
  { additionalProperties: false },
);

export const governanceContractSchemas = {
  resource_ref_v1: resourceRefV1Schema,
  principal_context_v1: principalContextV1Schema,
  delegated_mandate_v1: delegatedMandateV1Schema,
  policy_decision_v1: policyDecisionV1Schema,
  datasource_binding_v1: datasourceBindingV1Schema,
  registry_binding_v1: registryBindingV1Schema,
  governance_action_catalog_v1: governanceActionCatalogV1Schema,
} as const satisfies Record<string, TSchema>;

export type GovernanceContractName = keyof typeof governanceContractSchemas;
export type PrincipalContextV1 = Static<typeof principalContextV1Schema>;
export type DelegatedMandateV1 = Static<typeof delegatedMandateV1Schema>;
export type PolicyDecisionV1 = Static<typeof policyDecisionV1Schema>;
export type ResourceRefV1 = Static<typeof resourceRefV1Schema>;
export type DatasourceBindingV1 = Static<typeof datasourceBindingV1Schema>;
export type RegistryBindingV1 = Static<typeof registryBindingV1Schema>;
export type GovernanceActionCatalogV1 = Static<typeof governanceActionCatalogV1Schema>;

export function validateGovernanceContract(name: GovernanceContractName, value: unknown): boolean {
  return Value.Check(governanceContractSchemas[name], value);
}

export function governanceCoverage(catalog: GovernanceActionCatalogV1): {
  supported: number;
  specified: number;
  enforced: number;
  contractCoverage: number;
  runtimeCoverage: number;
} {
  const supportedActions = catalog.actions.filter(
    (action) => action.support_status === "supported",
  );
  const specified = supportedActions.filter(
    (action) => action.contract_status === "specified",
  ).length;
  const enforced = supportedActions.filter(
    (action) => action.runtime_enforcement_status === "enforced",
  ).length;
  return {
    supported: supportedActions.length,
    specified,
    enforced,
    contractCoverage: supportedActions.length === 0 ? 0 : specified / supportedActions.length,
    runtimeCoverage: supportedActions.length === 0 ? 0 : enforced / supportedActions.length,
  };
}

/* M0.5 review-only composition semantics. This is not wired to a production PEP. */
type ReviewObject = Record<string, any>;

function reviewTime(value: unknown): number | null {
  if (typeof value !== "string") return null;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? null : parsed;
}

function reviewActiveAt(start: unknown, end: unknown, at: number): boolean {
  const startTime = reviewTime(start);
  const endTime = end === null ? null : reviewTime(end);
  return startTime !== null && startTime <= at && (endTime === null || at < endTime);
}

function reviewPrincipalKey(principal: ReviewObject): string {
  return JSON.stringify([principal?.principal_id, principal?.principal_type]);
}

function reviewResourceKey(resource: ReviewObject): string {
  return JSON.stringify([
    resource?.resource_type,
    resource?.resource_id,
    resource?.organization_id,
    resource?.workspace_id,
    resource?.resource_revision,
  ]);
}

export function validateGovernanceReviewTrace(trace: ReviewObject): string[] {
  const errors: string[] = [];
  const reject = (code: string, condition: boolean): void => {
    if (condition && !errors.includes(code)) errors.push(code);
  };
  const contractFields: Array<[string, GovernanceContractName]> = [
    ["principal_context", "principal_context_v1"],
    ["delegated_mandate", "delegated_mandate_v1"],
    ["policy_decision", "policy_decision_v1"],
    ["datasource_binding", "datasource_binding_v1"],
    ["registry_binding", "registry_binding_v1"],
  ];
  for (const [field, contract] of contractFields) {
    reject(`contract.${field}`, !validateGovernanceContract(contract, trace[field]));
  }
  if (errors.length > 0) return errors;

  const principal = trace.principal_context;
  const mandate = trace.delegated_mandate;
  const policy = trace.policy_decision;
  const datasource = trace.datasource_binding;
  const registry = trace.registry_binding;
  const action = trace.action_request ?? {};
  const approval = trace.approval_snapshot ?? {};
  const query = trace.query_lineage ?? {};
  const request = trace.request_binding ?? {};
  const extensions = trace.extensions ?? {};
  const at = reviewTime(trace.as_of);
  if (at === null) return ["trace.as_of_invalid"];

  const org = principal.organization_id;
  const workspace = principal.workspace_id;
  const actor = principal.actor_principal;
  const accountable = principal.accountable_principal;
  reject("principal.scope_mismatch", [mandate, datasource, registry].some(
    (item) => item.organization_id !== org || item.workspace_id !== workspace,
  ));
  reject("principal.accountability_mismatch", reviewPrincipalKey(mandate.accountable_principal) !== reviewPrincipalKey(accountable));
  reject("principal.accountable_scope_mismatch", accountable.principal_type === "organization" && accountable.principal_id !== org);
  reject("principal.not_active", !reviewActiveAt(principal.issued_at, principal.expires_at, at));
  const authenticatedAt = reviewTime(principal.authentication_context.authenticated_at);
  const principalIssuedAt = reviewTime(principal.issued_at);
  reject("principal.authentication_time_invalid", authenticatedAt === null || principalIssuedAt === null || authenticatedAt > principalIssuedAt);

  const chain = principal.delegation_chain as ReviewObject[];
  reject("delegation.chain_invalid", chain.length !== 1);
  if (chain.length === 1) {
    const delegation = chain[0]!;
    reject("delegation.mandate_mismatch", delegation.mandate_id !== mandate.mandate_id);
    reject("delegation.principal_mismatch", delegation.delegator_principal_id !== mandate.delegator_principal.principal_id || delegation.delegate_principal_id !== mandate.delegate_principal.principal_id);
    reject("delegation.not_active", !reviewActiveAt(delegation.issued_at, delegation.expires_at, at));
    const intervals = [
      reviewTime(delegation.issued_at), reviewTime(delegation.expires_at),
      reviewTime(principal.issued_at), reviewTime(principal.expires_at),
      reviewTime(mandate.issued_at), reviewTime(mandate.expires_at),
    ];
    reject("delegation.interval_mismatch", intervals.some((item) => item === null) || !(
      Math.max(intervals[2]!, intervals[4]!) <= intervals[0]!
      && intervals[0]! < intervals[1]!
      && intervals[1]! <= Math.min(intervals[3]!, intervals[5]!)
    ));
  }

  reject("mandate.actor_mismatch", reviewPrincipalKey(mandate.delegate_principal) !== reviewPrincipalKey(actor));
  reject("mandate.status_invalid", mandate.status !== "active");
  reject("mandate.not_active", !reviewActiveAt(mandate.issued_at, mandate.expires_at, at));
  reject("mandate.task_mismatch", mandate.task_run_id !== action.task_run_id);
  reject("mandate.audience_mismatch", mandate.audience !== action.audience);
  reject("mandate.purpose_mismatch", mandate.purpose !== action.purpose);
  reject("mandate.capability_missing", !mandate.capabilities.includes(action.action));
  const actionResourceKey = reviewResourceKey(action.resource ?? {});
  reject("resource.scope_mismatch", action.resource?.organization_id !== org || action.resource?.workspace_id !== workspace || policy.resource.organization_id !== org || policy.resource.workspace_id !== workspace || mandate.resource_scope.some((resource: ReviewObject) => resource.organization_id !== org || resource.workspace_id !== workspace));
  reject("mandate.resource_out_of_scope", !mandate.resource_scope.some((resource: ReviewObject) => reviewResourceKey(resource) === actionResourceKey));
  reject("budget.unresolved", mandate.budget_ref !== null);

  reject("policy.subject_mismatch", policy.subject_principal_id !== actor.principal_id);
  reject("policy.mandate_mismatch", policy.mandate_id !== mandate.mandate_id);
  reject("policy.action_mismatch", policy.action !== action.action);
  reject("policy.resource_mismatch", reviewResourceKey(policy.resource) !== actionResourceKey);
  reject("policy.effect_not_allow", policy.effect !== "allow");
  reject("policy.not_active", !reviewActiveAt(policy.evaluated_at, policy.expires_at, at));
  reject("policy.revision_mismatch", policy.policy_revision !== datasource.policy_revision || policy.policy_revision !== registry.policy_revision);
  const obligationTypes = new Set(policy.obligations.map((item: ReviewObject) => item.obligation_type));
  reject("policy.approval_obligation_missing", action.action === "query.execute" && !obligationTypes.has("approval"));
  reject("policy.approval_ref_mismatch", mandate.approval_policy_ref !== action.approval_policy_ref);

  reject("binding.datasource_not_active", datasource.status !== "active" || !reviewActiveAt(datasource.valid_from, datasource.valid_until, at));
  reject("binding.datasource_scope_mismatch", datasource.datasource.organization_id !== org || datasource.datasource.workspace_id !== workspace);
  reject("binding.registry_not_active", registry.status !== "active" || !reviewActiveAt(registry.valid_from, registry.valid_until, at));
  reject("binding.registry_scope_mismatch", registry.registry.organization_id !== org || registry.registry.workspace_id !== workspace);
  reject("binding.datasource_link_mismatch", registry.datasource_binding_id !== datasource.datasource_binding_id || registry.registry.parent_resource_id !== datasource.datasource.resource_id);
  reject("binding.registry_revision_mismatch", registry.registry_revision !== registry.registry.resource_revision);

  reject("approval.status_invalid", approval.status !== "approved");
  reject("approval.authority_invalid", !["human", "team", "organization"].includes(approval.authority?.principal_type));
  reject("approval.authority_mismatch", reviewPrincipalKey(approval.authority ?? {}) !== reviewPrincipalKey(policy.decision_authority));
  reject("approval.action_invalid", approval.action !== "query.approve");
  reject("approval.task_mismatch", approval.task_run_id !== action.task_run_id);
  reject("approval.query_mismatch", approval.query_run_id !== query.query_run_id);
  reject("approval.binding_mismatch", approval.datasource_binding_id !== datasource.datasource_binding_id || approval.registry_binding_id !== registry.registry_binding_id);
  reject("approval.policy_mismatch", approval.policy_revision !== policy.policy_revision);
  const hashPattern = /^sha256:[a-f0-9]{64}$/;
  reject("approval.hash_invalid", !hashPattern.test(String(approval.sql_hash ?? "")) || !hashPattern.test(String(approval.assurance_report_hash ?? "")));
  reject("approval.not_active", !reviewActiveAt(approval.decided_at, approval.expires_at, at));
  reject("approval.time_mismatch", approval.decided_at !== query.approved_at);

  reject("query.task_mismatch", query.task_run_id !== action.task_run_id);
  reject("query.scope_mismatch", query.organization_id !== org || query.workspace_id !== workspace);
  reject("query.datasource_mismatch", query.datasource_id !== datasource.datasource.resource_id);
  reject("query.registry_mismatch", query.registry_revision !== registry.registry_revision);
  reject("query.policy_mismatch", query.policy_revision !== policy.policy_revision);
  reject("query.approver_mismatch", query.approver_principal_id !== approval.authority?.principal_id);
  reject("query.sql_hash_mismatch", query.sql_hash !== approval.sql_hash);
  reject("query.assurance_hash_mismatch", query.assurance_report_hash !== approval.assurance_report_hash);
  reject("query.status_invalid", query.status !== "completed");
  reject("query.action_sequence_invalid", JSON.stringify(query.actions) !== JSON.stringify(["query.prepare", "query.approve", "query.execute"]));
  const preparedAt = reviewTime(query.prepared_at);
  const approvedAt = reviewTime(query.approved_at);
  const executedAt = reviewTime(query.executed_at);
  reject("query.time_order_invalid", preparedAt === null || approvedAt === null || executedAt === null || !(preparedAt <= approvedAt && approvedAt <= executedAt && executedAt <= at));

  reject("request.binding_missing", typeof request.request_id !== "string" || !request.request_id.startsWith("req_") || !hashPattern.test(String(request.idempotency_key_hash ?? "")));
  reject("request.task_mismatch", request.task_run_id !== action.task_run_id);
  reject("request.audience_mismatch", request.audience !== action.audience);
  reject("request.not_active", !reviewActiveAt(request.issued_at, request.expires_at, at));
  const approvalTime = reviewTime(approval.decided_at);
  const policyTime = reviewTime(policy.evaluated_at);
  const requestTime = reviewTime(request.issued_at);
  reject("request.time_order_invalid", approvalTime === null || policyTime === null || requestTime === null || executedAt === null || !(approvalTime <= policyTime && policyTime <= requestTime && requestTime <= executedAt && executedAt <= at));

  reject("extensions.not_explicit", JSON.stringify(Object.keys(extensions).sort()) !== JSON.stringify(["context", "economics"]) || extensions.economics !== null || extensions.context !== null);
  return errors;
}
