import { writeFileSync } from "node:fs";

import { productProjectionV1Schema } from "../src/product-projections.js";

const target = new URL("../../../agent/contracts/product-projection-v1.schema.json", import.meta.url);
writeFileSync(target, `${JSON.stringify(productProjectionV1Schema, null, 2)}\n`, "utf8");
console.log(target.pathname);
