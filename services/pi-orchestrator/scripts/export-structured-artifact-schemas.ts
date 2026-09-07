import { writeFileSync } from "node:fs";
import { structuredArtifactSchemas } from "../src/contracts.js";

for (const [name, schema] of Object.entries(structuredArtifactSchemas)) {
  const target = new URL(`../../../agent/contracts/${name.replaceAll("_", "-")}-artifact.schema.json`, import.meta.url);
  writeFileSync(target, `${JSON.stringify(schema, null, 2)}\n`, "utf8");
  console.log(target.pathname);
}
