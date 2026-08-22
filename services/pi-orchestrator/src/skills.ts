import { access, readFile } from "node:fs/promises";
import { join } from "node:path";

import {
  DefaultResourceLoader,
  SettingsManager,
  type Skill,
} from "@earendil-works/pi-coding-agent";

export const AUTHORIZED_SKILL_NAMES = [
  "data-requirement-clarifier",
  "metric-definition-reviewer",
  "business-root-cause-analysis",
  "data-analysis-report-writer",
  "exploratory-data-analysis",
  "funnel-analysis",
  "retention-cohort-analysis",
  "ab-test-analysis",
  "sql-reviewer",
  "data-quality-rule-generator",
  "table-design-advisor",
  "data-lineage-impact-analyzer",
  "dashboard-reviewer",
  "data-presentation-architect",
  "daily-report-writer",
  "weekly-monthly-report-writer",
  "data-doc-writer",
  "data-incident-postmortem-writer",
  "data-tool-integration-planner",
  "market-research-analyst",
  "feature-engineering-advisor",
  "model-evaluation-reviewer",
  "stream-pipeline-designer",
] as const;

export type AuthorizedSkillName = (typeof AUTHORIZED_SKILL_NAMES)[number];

export type AdvisorySkillName = Exclude<
  AuthorizedSkillName,
  | "data-requirement-clarifier"
  | "metric-definition-reviewer"
  | "business-root-cause-analysis"
  | "data-analysis-report-writer"
>;
export const CORE_WORKFLOW_SKILL_NAMES = AUTHORIZED_SKILL_NAMES.slice(0, 4) as readonly AuthorizedSkillName[];
export const ADVISORY_SKILL_NAMES = AUTHORIZED_SKILL_NAMES.slice(4) as readonly AdvisorySkillName[];

export const EVIDENCE_REQUIRED_SKILL_NAMES = [
  "exploratory-data-analysis",
  "funnel-analysis",
  "retention-cohort-analysis",
  "ab-test-analysis",
] as const satisfies readonly AuthorizedSkillName[];

export const MVP_SKILL_NAMES = AUTHORIZED_SKILL_NAMES;
export type MvpSkillName = AuthorizedSkillName;

export interface SkillRuntimeResources {
  loader: DefaultResourceLoader;
  skills: Skill[];
}

export async function loadMvpSkillResources(options: {
  cwd: string;
  agentDir: string;
  skillsRoot: string;
}): Promise<SkillRuntimeResources> {
  const allowedNames = new Set<string>(AUTHORIZED_SKILL_NAMES);
  const skillPaths = AUTHORIZED_SKILL_NAMES.map((name) =>
    join(options.skillsRoot, "skills", name),
  );

  await Promise.all(
    skillPaths.map(async (path) => {
      await access(join(path, "SKILL.md"));
    }),
  );

  const loader = new DefaultResourceLoader({
    cwd: options.cwd,
    agentDir: options.agentDir,
    settingsManager: SettingsManager.inMemory({
      enableSkillCommands: false,
    }),
    additionalSkillPaths: skillPaths,
    noSkills: true,
    noExtensions: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    systemPromptOverride: () =>
      [
        "你是 Forge 数据任务平台的任务编排器。",
        "你只负责澄清需求、选择已授权 Skill、推进任务状态并调用受控工具。",
        "你不得直接访问数据库、生成绕过 Forge 的可执行 SQL、伪造用户审批或把未确认内容写入组织知识。",
        "查询规划、编译、权限检查和执行必须委托给 Forge 可信执行层。",
      ].join("\n"),
    skillsOverride: (current) => ({
      skills: current.skills.filter((skill) => allowedNames.has(skill.name)),
      diagnostics: current.diagnostics,
    }),
  });

  await loader.reload();
  const result = loader.getSkills();
  if (result.diagnostics.length > 0) {
    const details = result.diagnostics
      .map((diagnostic) => `${diagnostic.type}: ${diagnostic.message}`)
      .join("; ");
    throw new Error(`Skill discovery failed: ${details}`);
  }

  const loadedNames = new Set(result.skills.map((skill) => skill.name));
  const missing = AUTHORIZED_SKILL_NAMES.filter((name) => !loadedNames.has(name));
  const unexpected = result.skills
    .map((skill) => skill.name)
    .filter((name) => !allowedNames.has(name));

  if (missing.length > 0 || unexpected.length > 0) {
    throw new Error(
      `Invalid skill whitelist: missing=[${missing.join(", ")}], unexpected=[${unexpected.join(", ")}]`,
    );
  }

  return { loader, skills: result.skills };
}

export async function loadStageSkillResources(options: {
  cwd: string;
  agentDir: string;
  skillsRoot: string;
  skillName: MvpSkillName;
}): Promise<SkillRuntimeResources> {
  const skillPath = join(options.skillsRoot, "skills", options.skillName);
  const skillFile = join(skillPath, "SKILL.md");
  const skillContent = await readFile(skillFile, "utf8");
  const loader = new DefaultResourceLoader({
    cwd: options.cwd,
    agentDir: options.agentDir,
    settingsManager: SettingsManager.inMemory({ enableSkillCommands: false }),
    additionalSkillPaths: [skillPath],
    noSkills: true,
    noExtensions: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    systemPromptOverride: () =>
      [
        "你是 Forge 数据任务平台中一个隔离的专业 Skill 执行器。",
        `当前且唯一授权的 Skill 是 ${options.skillName}。`,
        "严格按照下方 Skill 全文工作。你不能访问文件、Shell 或数据库，不能生成或执行 SQL。",
        "必须调用当前唯一的 submit_* Artifact Tool 结束任务；不得在自由文本中伪造 JSON。",
        "不能确定的业务规则必须写入 assumptions 或 open_questions，不得当成事实。",
        "",
        "<AUTHORIZED_SKILL>",
        skillContent,
        "</AUTHORIZED_SKILL>",
        "平台 Artifact Contract 高于 Skill 中的 Markdown 输出示例。不得输出 Markdown 作为最终结果；必须通过唯一 submit_* Tool 提交。",
      ].join("\n"),
    skillsOverride: (current) => ({
      skills: current.skills.filter((skill) => skill.name === options.skillName),
      diagnostics: current.diagnostics,
    }),
  });
  await loader.reload();
  const result = loader.getSkills();
  if (
    result.diagnostics.length > 0 ||
    result.skills.length !== 1 ||
    result.skills[0]?.name !== options.skillName
  ) {
    throw new Error(`Stage Skill isolation failed: ${options.skillName}`);
  }
  return { loader, skills: result.skills };
}
