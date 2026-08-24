import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";
import { assets, reports, tasks } from "../src/data.js";

const root = resolve(import.meta.dirname, "..");
const index = readFileSync(resolve(root, "index.html"), "utf8");
const source = readFileSync(resolve(root, "src/main.js"), "utf8");
const styles = readFileSync(resolve(root, "src/style.css"), "utf8");

const allowedTaskStatuses = new Set(["waiting_approval", "needs_input", "analyzing", "rendering", "completed", "failed"]);

test("prototype fixtures cover the critical task and report states", () => {
  assert.equal(new Set(tasks.map((task) => task.id)).size, tasks.length);
  assert.ok(tasks.every((task) => allowedTaskStatuses.has(task.status)));
  assert.deepEqual(new Set(tasks.map((task) => task.status)), allowedTaskStatuses);
  assert.ok(tasks.find((task) => task.status === "waiting_approval")?.sql);
  assert.ok(tasks.find((task) => task.status === "failed")?.failure);
  assert.ok(reports.some((report) => report.status === "ready"));
  assert.ok(reports.some((report) => report.status === "rendering"));
  assert.ok(assets.tables.length >= 4 && assets.metrics.length >= 4 && assets.drafts.length >= 2);
});

test("product shell exposes the approved top-level information architecture", () => {
  for (const label of ["工作台", "新建任务", "任务", "报告", "数据资产", "管理"]) assert.match(source, new RegExp(label));
  for (const route of ["/workspace", "/new", "/tasks", "/reports", "/data", "/admin"]) assert.match(source, new RegExp(route.replace("/", "\\/")));
  assert.match(source, /概览.*数据与 SQL.*分析.*报告.*活动记录/s);
});

test("prototype is isolated, local, and explicit about demo behavior", () => {
  assert.doesNotMatch(index + source + styles, /cdn\.|unpkg|jsdelivr|fonts\.googleapis/i);
  assert.doesNotMatch(source, /\bfetch\s*\(|XMLHttpRequest|WebSocket/);
  assert.match(source, /不连接生产查询、审批或配置/);
  assert.match(source, /不会操作真实数据，也不会生成审批或审计记录/);
  assert.match(source, /企业多用户授权尚未开放/);
});

test("shell content does not use marketing or landing-page language", () => {
  const rejected = ["重新定义", "一站式", "赋能", "更智能", "更专业", "可信运行时在线", "从图表堆砌", "EXECUTIVE BRIEF", "DECISION NOTE"];
  for (const phrase of rejected) assert.doesNotMatch(source + index, new RegExp(phrase, "i"));
  assert.doesNotMatch(source, /class=\\?"[^"`]*(?:hero|feature-card)/i);
});

test("visual tokens include all bounded product states and reduced motion", () => {
  for (const token of ["--green", "--amber", "--red", "--violet", ".status-badge", ".state-banner", ".empty-state", ".approval-dialog"]) assert.match(styles, new RegExp(token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")));
  assert.match(styles, /prefers-reduced-motion/);
  assert.match(index, /Content-Security-Policy/);
});
