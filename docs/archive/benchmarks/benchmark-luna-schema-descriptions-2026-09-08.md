# Forge 里程碑：本轮 Schema 说明增强实验得分超过 Direct SQL

> 日期：2026-09-08 · 实验：REQ-065 / Forge-20260908-02 · 归档：REQ-066 / Forge-20260908-03
> 状态：实验完成、局部得分领先；预注册采用门槛未通过，候选保持默认 `off`。本记录不是版本发布。

## 1. 突破是什么

在本轮固定的 **8 道开发题、两次重复**中，启用 `interfaces-v1` 的 Forge EX 与 Contract 均为 **12/16（75%）**，同期 Direct SQL 为 **10/16（62.5%）**。两个重复分别都是 **6/8 vs 5/8**。

因此，可以记录：**在这轮限定样本实验中，增强后的 Forge 得分超过了 Direct SQL，高 12.5 个百分点。**

不能记录为“16 道独立题”“首次超过 Direct SQL”“全面领先”或“统计显著优势”。历史定向实验已有局部领先；本次价值是：把关键 DSL 说明作为独立变量，观察到两个重复均有总分领先，并留下完整的真实调用与原响应重放证据。总分重复领先不等于同一题的改善稳定复现。

事实源：[实跑 JSON](benchmark-luna-schema-descriptions-2026-09-08.json)；前置证据：[准备卡](benchmark-luna-schema-descriptions-ready-2026-09-08.json)、[生成优化研究](forge-json-generation-research-2026-09-08.json)。本简报不另行评分、不覆盖原报告。

## 2. 到底做了哪些优化

### 2.1 本轮唯一生成变量：补全五个接口的模型可见语义

REQ-064 的代码审计发现：当前 Benchmark 加载静态 `forge/schema.json`，既有 strict 转换会去掉原 Schema 的 72 条 `description`。合法 JSON 约束仍然存在，但“定义了什么、绑定了什么、导出了什么”不能仅靠结构合法性表达。这是当前链路的已知说明缺口；它是否导致某道错题，仍不能仅凭审计判定。

没有恢复全部旧说明，也没有继续堆题目范例。新增共享目录 [`forge-output-descriptions-v1.json`](../../../agent/contracts/forge-output-descriptions-v1.json)，只审核并加入以下五个根字段说明：

| 字段 | 增加的语义说明 | 防止混淆的边界 |
|---|---|---|
| `scan` | 本查询块的 FROM 来源；引用本地列前先通过 scan/joins 绑定表、CTE 或派生查询；合法祖先作用域相关引用仍可用。 | 声明 CTE 不等于将它加入 FROM。 |
| `cte` | 每个子查询的 `select` 定义对下游导出的列；下游只能引用导出名。 | CTE 内部字段不自动对外可见。 |
| `agg` | 聚合定义属于当前块，用精确 `as` 别名引用。 | 定义聚合不等于选入结果，也不等于导出给下一层。 |
| `window` | 窗口定义属于当前块，使用精确别名；同层用 `qualify` 过滤；下游需要 `select` 导出。 | 隐藏的窗口辅助值不能被下一层直接引用。 |
| `select` | 当前块的输出接口；CTE 显式导出下游所需列，最终块只返回问题要求的答案列。 | 中间计数、辅助值不应无要求地进入最终答案。 |

[`createStrictForgeOutput`](../../../services/pi-orchestrator/src/strict-forge-output.ts) 复用既有转换器，构造 strict Schema 后附加经审核的说明。模型可见 Schema 从 **8,034 增至 9,171 字节（+1,137）**；字节增量不是 token 用量。

**没有改变**：canonical DSL、字段顺序、Prompt、Direct 输入、两臂上下文、模型/SDK、Compiler、Assurance、Gold、评分规则。没有自动补列、补 JOIN、重排键序、答案规划、追加返修、多候选选优或模型更换。不能把未实施的研究建议列作本轮优化动作。

### 2.2 配套工程：让单变量实验可信、可拒绝、可重放

这些是实验完整性改进，不应另算准确率收益：

- **显式 opt-in**：`--schema-descriptions` 默认 `off`；`interfaces-v1` 必须同时声明 `--variable schema_descriptions`，不偷改默认路径。
- **同一版本化来源**：共享目录同时绑定 canonical Schema 与实际 wire Schema 的 hash；Python 与 Pi 消费同一来源，wire 指纹由真实 TypeScript 转换复算，不再维护另一份转换器。
- **冻结与比较**：`bird-protocol-v5` 将说明模式、目录与 wire 修订纳入冻结。新因素只允许已声明的 mode/wire 差异，不能夹带 Prompt、上下文、Compiler、SDK 或模型漂移；旧 `schema` 因素仍要求相同候选。
- **运行时隔离**：每个 Run 使用自己的已验证 profile，启动、派发与重开都绑定原合同。未知模式、目录/Schema 漂移、未声明因素、旧冻结续跑均失败关闭。
- **真实调用留证**：沿既有 Pi Runtime 执行，限制原 64 次，捕获原始 SSE、HTTP 回执、候选、用量与状态；再用既有 Python replay/compare 重放，不靠手工挑选成功响应。

源码入口：[`bird_benchmark.py`](../../../forge/bird_benchmark.py)、[`cli.py`](../../../forge/cli.py)、[`benchmark-contracts.ts`](../../../services/pi-orchestrator/src/benchmark-contracts.ts)、[`benchmark-runtime.ts`](../../../services/pi-orchestrator/src/benchmark-runtime.ts)。

### 2.3 历史基础，不是本轮新增变量

此前已分别完成 Structured Tool/Prompt 与舍入规则优化（REQ-029）、qualified identifier/HAVING alias 等确定性 Compiler 修复（REQ-030/031）、原生 `strict:true` 工具输出及传输 null 归一化（REQ-038）、省略 AS 别名解析维护（REQ-039）。这些为本轮提供既有基础，**不能把历史收益累加到这次 12/16 上**，也不能把本轮差值归因于全部历史动作。

同样，先前分母/CTE 范例出现的回退和失败结论不改写。本轮没有叠加这些候选。历史逐轮边界见[报告导航](../../README.md#accuracy--benchmark-逐轮报告)。

## 3. 实验设计与完整结果

- 模型：`openai-codex/gpt-5.6-luna`，既有订阅 OAuth；模型/SDK 修订由原卡与实际生成合同固定。
- 目标题：`md-009`、`md-079`、`md-199`、`md-289`；跨库控制题：`md-002`、`md-052`、`md-312`、`md-386`。这是已曝光的开发筛查集，不是独立 H。
- A：`off`；B：`interfaces-v1`。执行顺序 **A1 → B1 → B2 → A2**，比较 A1/B1 与 A2/B2。
- 8 题 × 2 条件 × 2 臂 × 2 重复 = **64 次真实请求**。每臂单回合、默认采样、`max_output_tokens=null`、timeout 120 秒；零重试、换题、补跑或额外 canary。
- 真实生成时间：2026-09-08 10:10–10:14（+08:00）。全部 HTTP 200、64 个候选、4 个 Run 均 completed；评分与用量未知均为 0。

| 条件 / 重复 | Forge EX | Forge Contract | Direct EX | Direct Contract |
|---|---:|---:|---:|---:|
| A1（off） | 5/8 | 5/8 | 5/8 | 5/8 |
| B1（interfaces-v1） | 6/8 | 6/8 | 5/8 | 5/8 |
| B2（interfaces-v1） | 6/8 | 6/8 | 5/8 | 5/8 |
| A2（off） | 5/8 | 5/8 | 5/8 | 5/8 |
| A 合计 | 10/16 | 10/16 | 10/16 | 10/16 |
| **B 合计** | **12/16** | **12/16** | **10/16** | **10/16** |

EX 是原实验的执行结果匹配口径，不是“SQL 能运行”的比例；JSON 中沿用 `official_ea` 字段。Contract 单独记录，本轮恰好与 EX 一致，不能据此将两指标合并。

## 4. 收益、回退与成本一起保留

| 对照 → 处理的 Forge 变化 | EX / Contract |
|---|---|
| 第一重复 `md-289` | 错 → 对 |
| 第二重复 `md-079`、`md-199` | 错 → 对 |
| 第二重复控制题 `md-052` | **对 → 错** |

没有同一目标题在两个重复中都实现错→对。处理组控制题合计 **7/8**，未达到 **8/8**。因此，稳定目标题收益、零回退、控制全对三项预注册门槛未通过；不因合计分数增加而事后放宽。

| 成本 / 执行指标 | A | B | B 相对 A |
|---|---:|---:|---:|
| 双臂总 tokens | 109,155 | 112,860 | +3.39% |
| Forge tokens | 71,044 | 75,251 | 原始量均保留 |
| Forge 每个 EX 正确答案 tokens | 7,104.40 | 6,270.92 | −11.73% |
| Forge 生成 P95 | 20,737.9 ms | 19,614.3 ms | −5.42% |
| Forge 执行成功 | 13/16 | 16/16 | 不替代 EX |

总计 **222,015 tokens**，用量未知 0。以上相对成本门槛通过，但不能抵消正确性门槛失败；成本改善是 B 相对 A 的 Forge 效率，不是声称比 Direct 更省。P95 是本轮小样本实测，不代表生产性能。订阅人民币费用不可核算，不承诺免费。

## 5. 验证证据与公开边界

**生成前准备（历史验证，本次文档整理不复跑）**：45/45 独立 SQLite 正反行为检查；500 题默认 context/schema/instructions 不变；16 个保存候选在两条件下 SQL、结果、EX/Contract 不变；真实 Pi SDK 本地合成 SSE 验证。合成回放不是新模型生成。

**本轮真实生成与重放**：64 份原 SSE 的候选及 provider 用量逐一匹配 Pi 保存记录；64 臂 SQL、编译执行状态、EX、Contract 重放无差异；两个重复协议比较可比。没有在分析阶段新增模型调用。

**本次提交前维护验证（2026-09-08，11:04 +08:00 前完成）**：

- `.venv/bin/python -m pytest tests/test_bird_benchmark.py -q`：81 passed；1 个第三方弃用 warning。
- 在 `services/pi-orchestrator/` 执行 `npm run typecheck`：通过。
- 同目录 `node --import tsx --test tests/benchmark-runtime.test.ts tests/strict-forge-output.test.ts`：53 passed；Node SQLite experimental warning。
- 此次不是全套回归或新真实模型实验；测试数不与准备期全套记录混算。

公开报告中的本地证据路径不是公网下载地址。原始 SSE、SQLite、订阅凭据与第三方数据集不入 Git；保存的脚本仅是审计方法，不是可自动消费当前调用授权的入口。实跑报告内个人主目录作最小脱敏，原件与公开字节 hash 对照见[公开处理记录](../engineering/schema-descriptions-publication-2026-09-08.json)。嵌入的 `report_sha256` 仍描述原件，不冒充公开副本 hash。

## 6. 决定与可复用表达

**记为一个限定样本内的工程里程碑；记录领先，也保留失败门槛。候选继续默认 off。**

REQ-066归档时仅授权候选分支commit/push；后续REQ-067已明确授权合并main并push，并约定今后日常工作默认只在main、不自行新建分支。已执行快进合并，最终提交/远端结果以Git记录为准。这不是软件版本发布：不打tag、不创建Release、不部署安装、不新增模型调用；H、R0.6与历史500题结论不变。

可复用文案见[Devlog 草稿](../../devlog/2026-09-08-forge-schema-descriptions-milestone.md)。这份草稿可用于后续沟通，但本次不执行平台分发。
