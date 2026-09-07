import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestContext } from "node:test";

import { MVP_SKILL_NAMES } from "../src/skills.js";

export async function createSkillFixture(context: TestContext) {
  const directory = await mkdtemp(join(tmpdir(), "forge-pi-skills-"));
  context.after(() => rm(directory, { recursive: true, force: true }));
  const skillsRoot = join(directory, "package");
  const agentDir = join(directory, "agent");
  await mkdir(agentDir);
  const documents = new Map<string, string>();
  for (const name of [...MVP_SKILL_NAMES, "unlisted-skill"]) {
    const skillDirectory = join(skillsRoot, "skills", name);
    await mkdir(skillDirectory, { recursive: true });
    const content = `---
name: ${name}
description: Isolated runtime contract fixture for ${name}.
---

# ${name}

Keep unresolved business rules explicit.
Only the authorized artifact tool may complete this stage.
Fixture boundary marker: ${name}.
`;
    await writeFile(join(skillDirectory, "SKILL.md"), content);
    documents.set(name, content);
  }
  return { skillsRoot, agentDir, documents };
}
