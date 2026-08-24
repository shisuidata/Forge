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
