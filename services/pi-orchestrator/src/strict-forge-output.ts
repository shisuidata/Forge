import { createHash } from "node:crypto";
import { Compile } from "typebox/compile";
import type { TSchema } from "typebox";

interface Schema {
  [key: string]: unknown;
  type?: string | string[];
  $ref?: string;
  properties?: Record<string, Schema>;
  required?: string[];
  definitions?: Record<string, Schema>;
  oneOf?: Schema[];
  anyOf?: Schema[];
  items?: Schema;
  enum?: unknown[];
}

export class StrictForgeOutputError extends Error {}
const querySlots: Record<string, true> = { query: true, recursive_term: true, subquery: true };
const annotationKeys: Record<string, true> = { $schema: true, title: true, description: true, examples: true, default: true };

const object = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** Native OpenAI strict schemas support recursive refs/unions that Pi's generic converter rejects. */
export function createStrictForgeOutput(canonical: Schema, descriptions: Readonly<Record<string, string>> = {}) {
  const resolve = (node: Schema): Schema => {
    if (!node.$ref) return node;
    const prefix = "#/definitions/";
    if (!node.$ref.startsWith(prefix)) throw new StrictForgeOutputError("Unsupported Forge schema reference");
    const definition = canonical.definitions?.[node.$ref.slice(prefix.length)];
    if (!definition) throw new StrictForgeOutputError("Unresolved Forge schema reference");
    return resolve(definition);
  };
  const allowsNull = (schema: Schema): boolean => {
    const node = resolve(schema);
    return node.type === "null" || (Array.isArray(node.type) && node.type.includes("null"))
      || (node.oneOf ?? node.anyOf ?? []).some(allowsNull);
  };
  const queryNode = (node: Schema, key: string): Schema => {
    if (node.type !== "object" || node.properties) return node;
    // These are existing recursively compiled Forge-query slots, not arbitrary JSON maps.
    if (querySlots[key] !== true) throw new StrictForgeOutputError("Unsupported open object in Forge schema");
    return canonical;
  };
  const build = (node: Schema, key = ""): Schema => {
    if (queryNode(node, key) === canonical && node !== canonical) return { $ref: "#" };
    if (node.$ref) {
      resolve(node);
      return { $ref: node.$ref.replace("#/definitions/", "#/$defs/") };
    }
    const result: Schema = {};
    for (const [name, value] of Object.entries(node)) {
      if (annotationKeys[name] !== true && !["definitions", "properties", "required", "oneOf", "anyOf", "items"].includes(name)) {
        result[name] = value;
      }
    }
    const alternatives = node.oneOf ?? node.anyOf;
    if (alternatives) result.anyOf = alternatives.map((child) => build(child));
    if (Array.isArray(node.type)) {
      delete result.type;
      result.anyOf = node.type.map((type) => ({ type }));
    }
    if (node.items) result.items = build(node.items);
    if (node.properties) {
      const required = new Set(node.required ?? []);
      result.properties = {};
      for (const [name, property] of Object.entries(node.properties)) {
        const strict = build(property, name);
        result.properties[name] = required.has(name) || allowsNull(property)
          ? strict : { anyOf: [strict, { type: "null" }] };
      }
      result.required = Object.keys(node.properties);
      result.additionalProperties = false;
    }
    if (node.definitions) result.$defs = Object.fromEntries(
      Object.entries(node.definitions).map(([name, child]) => [name, build(child)]),
    );
    return result;
  };
  const schema = build(canonical);
  for (const [key, description] of Object.entries(descriptions)) {
    if (Object.hasOwn(schema.properties ?? {}, key) && description.trim()) {
      schema.properties![key]!.description = description;
    }
  }
  const validator = Compile(schema as TSchema);
  const revision = `sha256:${createHash("sha256").update(JSON.stringify(schema)).digest("hex")}`;

  const matches = (value: unknown, source: Schema): boolean => {
    const node = resolve(source);
    if (node.enum && !node.enum.includes(value)) return false;
    if (node.oneOf || node.anyOf) return (node.oneOf ?? node.anyOf)!.some((child) => matches(value, child));
    const types = Array.isArray(node.type) ? node.type : [node.type];
    const type = value === null ? "null" : Array.isArray(value) ? "array" : typeof value;
    if (!types.includes(type) && !(type === "number" && types.includes("integer") && Number.isInteger(value))) return false;
    if (!object(value) || !node.properties) return true;
    return Object.keys(value).every((name) => Object.hasOwn(node.properties!, name))
      && (node.required ?? []).every((name) => Object.hasOwn(value, name))
      && Object.entries(node.properties).every(([name, property]) =>
        !property.enum || property.enum.includes(value[name]));
  };
  const decode = (value: unknown, source: Schema, key = ""): unknown => {
    const node = queryNode(resolve(source), key);
    const alternatives = node.oneOf ?? node.anyOf;
    if (alternatives) {
      const branches = alternatives.filter((child) => matches(value, child));
      if (branches.length !== 1) throw new StrictForgeOutputError("Ambiguous or invalid strict Forge value");
      return decode(value, branches[0]!);
    }
    if (Array.isArray(value)) {
      if (!node.items) throw new StrictForgeOutputError("Unexpected Forge array");
      return value.map((child) => decode(child, node.items!));
    }
    if (!object(value)) return value;
    if (!node.properties) throw new StrictForgeOutputError("Unexpected Forge object");
    const required = new Set(node.required ?? []);
    const result: Record<string, unknown> = {};
    for (const [name, child] of Object.entries(value)) {
      const property = node.properties[name];
      if (!Object.hasOwn(node.properties, name) || !property) throw new StrictForgeOutputError("Unexpected strict Forge property");
      // SQL NULL in ScalarVal/default is data; only transport-only nullable optionals disappear.
      if (child === null && !required.has(name) && !allowsNull(property)) continue;
      result[name] = decode(child, property, name);
    }
    return result;
  };
  return {
    schema,
    revision,
    decode(value: Record<string, unknown>): Record<string, unknown> {
      if (!validator.Check(value)) throw new StrictForgeOutputError("Arguments do not satisfy the strict Forge schema");
      return decode(value, canonical) as Record<string, unknown>;
    },
  };
}

export function requireStrictForgeApi(model: { api: string; compat?: unknown }): void {
  if (!["openai-codex-responses", "openai-responses", "openai-completions"].includes(model.api)
    || (object(model.compat) && model.compat.supportsStrictMode === false)) {
    throw new StrictForgeOutputError(`Strict Forge output is unsupported by API ${model.api}`);
  }
}

/** Called after other Pi payload hooks. Never silently use a non-strict tool or another tool. */
export function requireStrictForgePayload(payload: unknown, api: string, schema: unknown): unknown {
  requireStrictForgeApi({ api });
  if (!object(payload) || !Array.isArray(payload.tools)) throw new StrictForgeOutputError("Missing Forge tool payload");
  const completions = api === "openai-completions";
  const tools = payload.tools.filter((tool: unknown) => {
    if (!object(tool)) return false;
    const definition = completions ? tool.function : tool;
    return object(definition) && definition.name === "emit_forge_query";
  });
  if (tools.length !== 1) throw new StrictForgeOutputError("Expected one emit_forge_query tool");
  const tool = tools[0] as Record<string, unknown>;
  const definition = (completions ? tool.function : tool) as Record<string, unknown>;
  if (tool.type !== "function") throw new StrictForgeOutputError("Expected a JSON function tool");
  definition.parameters = schema;
  definition.strict = true;
  payload.tools = [tool];
  payload.parallel_tool_calls = false;
  payload.tool_choice = completions
    ? { type: "function", function: { name: "emit_forge_query" } }
    : { type: "function", name: "emit_forge_query" };
  return payload;
}
