import type { AuthorizedSkillName } from "./skills.js";

export interface TeamSkillPolicy {
  org_id: string;
  team_id: string;
  enabled_skills: AuthorizedSkillName[];
  version: number;
  updated_at: string;
  updated_by: string;
}

export interface SkillPolicyStore {
  get(orgId: string, teamId: string): TeamSkillPolicy | undefined;
  isEnabled(orgId: string, teamId: string, skillName: AuthorizedSkillName): boolean;
  configure(input: {
    orgId: string;
    teamId: string;
    enabledSkills: AuthorizedSkillName[];
    expectedVersion: number;
    actor: string;
  }): TeamSkillPolicy;
}

export class SkillPolicyConflictError extends Error {}

export class InMemorySkillPolicyStore implements SkillPolicyStore {
  readonly #policies = new Map<string, TeamSkillPolicy>();
  readonly #defaults: AuthorizedSkillName[];

  constructor(defaults: readonly AuthorizedSkillName[]) {
    this.#defaults = [...defaults];
  }

  get(orgId: string, teamId: string): TeamSkillPolicy | undefined {
    const policy = this.#policies.get(`${orgId}\0${teamId}`);
    return policy === undefined ? undefined : structuredClone(policy);
  }

  isEnabled(orgId: string, teamId: string, skillName: AuthorizedSkillName): boolean {
    return (this.get(orgId, teamId)?.enabled_skills ?? this.#defaults).includes(skillName);
  }

  configure(input: {
    orgId: string;
    teamId: string;
    enabledSkills: AuthorizedSkillName[];
    expectedVersion: number;
    actor: string;
  }): TeamSkillPolicy {
    const key = `${input.orgId}\0${input.teamId}`;
    const current = this.#policies.get(key);
    const version = current?.version ?? 0;
    if (version !== input.expectedVersion) {
      throw new SkillPolicyConflictError(`Skill policy version mismatch: expected ${input.expectedVersion}, current ${version}`);
    }
    const policy: TeamSkillPolicy = {
      org_id: input.orgId,
      team_id: input.teamId,
      enabled_skills: [...new Set(input.enabledSkills)].sort(),
      version: version + 1,
      updated_at: new Date().toISOString(),
      updated_by: input.actor,
    };
    this.#policies.set(key, policy);
    return structuredClone(policy);
  }
}
