# Luna限定列取值证据：16次配对实验

日期：2026-09-06 · REQ-2026-09-06-054

## 结论

**观察到两臂新增通过，但未达预声明验收门槛；value_context保持默认off，不补跑、不扩样。**

用户单独确认“批准16次对照”。实际16派发、16候选、16臂可评分，未知标签与未知用量均0。Forge EX/Contract从2/4到3/4，Direct从3/4到4/4；目标四臂仅从1/4到3/4，未达到要求的4/4。四控制臂在两条件均正确。

总35572 tokens；处理组比off增加17.85%，低于25%的token验收线，但成本线通过不能替代准确率线。金额未知，不把token统计当实际账单。

## 固定条件与运行身份

- `openai-codex / gpt-5.6-luna`；实际记录revision为`sha256:b18e69afb680689956ba8399d31d67e3f695b07da8aaf5ff44c5148bfc367693`，两条件一致。
- `bird-protocol-v4`；仅切换`value_context.mode=off/observed`。同一限定字段`debit_card_specializing / gasstations / Segment`，最多16值/单值256 UTF-8字节，采集5秒。
- 同一源码、模型记录、生成Contract、Schema、数据、Gold与评分；日期/粒度均off。md-009/md-026/md-018每臂新增1642字节；md-002不附加同名异表证据。
- Gold require_all、生成120秒、provider_default采样、单次派发、零重试；没有追加生成、恢复或替换候选。
- 先off后observed，每条件病例并发1。请求冻结顺序为009/026/002/018；Pi原生实际执行顺序均为002/009/018/026。未改调度器。

| 条件 | Run ID | 生成次数 | 状态 |
|---|---|---:|---|
| off | `pbr_dbbf2c855da043d89f05580b968b8436` | 8 | completed |
| observed | `pbr_75f5e2a5c2f24bb2b29dc1c581f3eb2f` | 8 | completed |

协议revision、完整生成Contract及起止时间见[机器简报](benchmark-luna-value-context-2026-09-06.json)。这是固定已知开发题的单次小样本，不是滚动seed49、独立holdout或总体收益证明。

## 逐题结果

本轮所有臂的EX与Contract判定相同；表中“通过”表示两指标均通过。

| 题目 | 用途 | Forge off→observed | Direct off→observed |
|---|---|---|---|
| md-009 | Discount目标 | 失败→失败 | 失败→通过 |
| md-026 | Premium目标 | 失败→通过 | 通过→通过 |
| md-002 | customers.Segment同名异表控制 | 通过→通过 | 通过→通过 |
| md-018 | gasstations同表其他逻辑控制 | 通过→通过 | 通过→通过 |

新增通过：md-009 Direct、md-026 Forge。无已正确臂回退；但md-009 Forge从“执行成功但答案错”变成“执行失败”，这一执行能力回退单独披露，不能藏在0→0中。

off的md-009两臂使用`discount`，md-026 Forge使用`premium`；observed的目标各臂均使用正确原样`Discount`/`Premium`。这支持本样本的取值绑定改善观察，但没有解决完整查询结构。

## 剩余失败：取值已对，CTE未接入FROM

md-009 observed Forge生成了两个CTE，并在投影引用它们，却仍保留外层`scan: gasstations`，没有FROM/JOIN绑定：

```sql
WITH cze AS (...), svk AS (...)
SELECT cze.cnt - svk.cnt
FROM gasstations
```

以上仅省略两个CTE的展示；完整原SQL、Forge JSON及Assurance报告在机器证据中。两个CTE内的Segment均已改为`Discount`。

- Compiler按原输入生成SQL；没有凭意图补JOIN。
- 共享Assurance v8的只读、解析、Registry门禁均通过此样本。
- 独立只读SQLite执行原SQL仍报`no such column: cze.cnt`；原Gold结果为176。
- 原评分EX=0、Contract=0保留；没有反事实替换、自动修复或重跑。

这是模型漏接关系及现有保障未前置拒绝该作用域错误的证据。下一项更合适的离线维护是**FROM作用域绑定校验**，不是继续堆Prompt或自动补JOIN。本实验未实施该后续修复，以免污染已冻结对照。

该错误属于被测SQL失败，不是生成/基础设施故障；其失败评分保留在完整分母。错误元数据中的retryable没有触发实际重试。

## 预声明门槛

| 门槛 | 结果 |
|---|---|
| observed目标四臂EX/Contract全过 | **失败：3/4** |
| 同期off至少新增1个目标通过臂，且无评分回退 | 通过：新增2臂 |
| 控制四臂两条件均正确 | 通过 |
| 16次均可评分、无未知标签 | 通过；SQL失败计0，不剔除 |
| 全部用量可观察，处理总tokens≤1.25×off | 通过：1.17846倍 |
| 最终采纳 | **不采纳，保持off** |

门槛在生成前由用户确认，未根据结果调整。即使全部通过也只会作为小样本开发信号，不直接推广或推进H/R0.6。

## 用量

| 分支 | off tokens | observed tokens |
|---|---:|---:|
| Forge | 12359 | 14187 |
| Direct | 3970 | 5056 |
| 合计 | 16329 | 19243 |

总35572，未知用量0。合计增幅17.85%；Direct单独约增加27.36%，没有把合计门槛事后改成逐臂门槛。该变化含单次生成波动，不全归因于新增后缀。

## 验证与证据边界

- 16份原始响应hash可复算；16原候选离线replay的SQL、编译/执行状态与EX/Contract均和原Pi一致。
- 原生日志完整：108＋108＝216条；实际provider/model/revision与generation_contract一致。
- 实际CLI compare：comparable=true、生成来源已知。CLI本身只比较保存候选，仍明确generative_gain_claim=false；真实生成来源由本轮原生Run/日志补齐，不把这个标志解释成总体因果证明。
- 另用原SQL、原Gold做全新只读SQLite验证：四题均单列，独立集合/多重集合比较，16项评分一致；不调用Compiler、Assurance或项目评分函数。唯一SQL错误仍为md-009 Forge。
- 16份Prompt由原生持久化上下文与固定Runtime确定性重建，含完整冻结指令，字符数和原生日志一致。**这是重建文本，不是抓取的Provider wire原文**；原生payload hash保留，不伪造网络请求正文。
- 生成前最终回归：Python873 passed/26 skipped；Pi151 passed、typecheck通过。生成期间没有进一步修改Runtime源码。

[准备简报](benchmark-luna-value-context-ready-2026-09-06.md)保留授权前的0调用快照及依赖误同步披露；本文件记录随后单独批准的真实实验。最终两条件均使用相同当前环境。启动时默认空认证目录不ready，尚未创建Run或派发；随后沿用历史显式agentDir，通过SDK使用已有认证，未手动读取/复制/记录凭证内容。

两服务已停止，18772/18773均关闭；Pi专用状态一致性快照已封存，SQLite integrity_check=ok。未commit/push/deploy，原候选/Gold未改写；滚动父Run仍为seed48。

完整工件：`.forge/benchmarks/categorical-value-context-20260906/`。主要证据：两条件原Run/日志/replay、generation-authorization、generation-evidence-validation、generation-independent-sqlite、generation-gate-result及generation-state.snapshot.sqlite3。
