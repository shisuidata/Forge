# 限定列取值证据：离线实现与四题对照准备

日期：2026-09-06 · REQ-2026-09-06-054

## 结论

**默认关闭的取值证据开关已实现、离线验收通过；被测模型调用0，尚未证明准确率收益。** 四题off/observed输入已冻结。16次Luna生成与下述门槛仍待单独授权；不启动seed49、不改变滚动父Run seed48或H/R0.6门禁。

## 实现与边界

- `forge/benchmark_metadata.py`：仅观察公开BIRD SQLite一个显式数据库/表/列；字段须已在该题ContextSnapshot.fields中，不能借同名字段串域。
- 只读BINARY DISTINCT，5秒上限；默认16个非NULL值（可配1–100），单值最多256 UTF-8字节。保留大小写、空字符串；NULL不算值。超限/非文本/错误/漂移不给部分值。
- 拒绝WAL格式数据库，包括已checkpoint且无活动sidecar的数据库：实际反例证明mode=ro仍可能创建-wal/-shm。UTF-16不符合本字节界限测量前提，同样失败关闭。
- 证据附限定列、原样值、提取SQL与参数、数据库/表元数据hash、覆盖范围。只是快照观察，不是业务权威枚举、未来保证或原列相等比较规则。
- 两臂使用同一个后缀，标注为不可信数据；生成上下文不含Gold SQL或结果。不自动lower/LIKE、纠正SQL、改变Schema、ResultContract、Compiler或评分。
- `forge/bird_benchmark.py`、CLI及Context API升至`bird-protocol-v4`，绑定value_evidence与两份完整指令hash。只允许预声明mode变化，字段、上限、数据及共享源码仍固定。Pi继续使用原消费链，无第二套生成器。

## 真实四题输入

字段：`debit_card_specializing / gasstations / Segment`。

观察完整5值：`Discount`、`Noname`、`Other`、`Premium`、`Value for money`。

| 题目 | 用途 | 字段可见 | 每臂新增UTF-8字节 |
|---|---|---:|---:|
| md-009 | Discount目标 | 是 | 1642 |
| md-026 | Premium目标 | 是 | 1642 |
| md-002 | customers.Segment同名异表控制 | 否 | 0 |
| md-018 | gasstations同表其他逻辑控制 | 是 | 1642 |

修改前v3 off与修改后v4 off的八份完整指令逐字一致；两条件基础Schema/ContextSnapshot相同，各题两臂后缀相同。当前两份manifest除模式及关联证据/完整指令hash外一致。

| 冻结 | 协议revision |
|---|---|
| off | `sha256:dc6cd1a7c04fdb847b263238553e57cc7389b46d2976716aa15df18b1a93d05a` |
| observed | `sha256:26631020a73758cf3c40e8318ab3f272cb597790c34cdb61faa83d67e455e66f` |

命令已实际执行：

```bash
FORGE_DISABLE_DOTENV=true .venv/bin/python -m forge.cli benchmark bird freeze \
  --cohort D --provider openai-codex --model gpt-5.6-luna \
  --case-ids md-009 md-026 md-002 md-018 --variable value_context \
  --value-field debit_card_specializing gasstations Segment \
  --value-max-values 16 --value-context observed --out treatment.frozen.json
# off条件保持所有参数，仅切换mode并另存control.frozen.json。
```

上例文件名省略工件目录。冻结中的每条件8次是协议预算字段，不是用户授权或实际调用。

## 离线证明

- 实际CLI：两份v4 validate通过；旧v3拒绝，exit2，不伪造升级或续跑。
- 实际本地鉴权HTTP：返回上下文与CLI逐字一致；无key为401，篡改限定字段为409。
- 实际Pi runtime消费、持久化与HTTP评估：八个合成派发完成；冻结后改动指令在新增派发前拒绝。仅模型/会话生成和Task工厂为合成替身，**不是SDK真实Provider生成或准确率实验**。Forge用固定SELECT夹具，Direct用保存输出；外部网络请求0。
- 原Structured Sol八个候选在off/observed下各做一次真实HTTP评估，16项执行/SQL/EX/Contract一致。两目标原四臂仍1/4，四控制臂仍4/4；没有改写原候选或把旧Sol输出当新Luna基线。
- 最终Python：**873 passed、26 skipped**；现有protobuf弃用warning。Pi：**151 passed，typecheck通过**；Node SQLite实验性warning。
- 保留有意义的回归：大小写/NOCASE、可见性、缓存隔离/失效、NULL/空字符串、高基数、UTF-8字节边界、非文本、路径/物理Schema/来源漂移、超时，以及WAL只读副作用。

## 环境与来源披露

开发子进程误用uv run，同步了已有lock固定的三个包：numpy **2.5.1→2.5.2**、pydantic **2.13.4→2.13.5**、pydantic_core **旧版未记录→2.46.5**。未推定未知旧版，未继续同步或回滚。最终两条件均用同一个当前.venv，且运行完整回归；修改前工件仅用于off字节一致检查，不能充当同环境准确率基线。

数据/Gold快照hash及prompt/schema/compiler/safety/official_snapshot来源组不变；context与evaluator来源组变化（协议/Context路由同文件指纹包含在内），不能声称所有源码hash不变。评分行为由原候选双条件回放证明。

本地API已停止且端口关闭；临时脚本和合成Agent数据库已删除，源码/命令以证据JSON保留。未读取用户凭证，未commit/push/deploy。

## 下一步建议：16次开发canary，尚待授权

同一Luna、同一源码/Schema/数据，4题×2臂×off/observed。Gold require_all；120秒、单次派发、零重试，生成或基础设施失败即停，不补跑、不扩样。

建议在生成前确认以下门槛：

1. observed的md-009/md-026四臂EX与Contract全部通过，且相对**同期off**至少新增1个双指标通过的目标臂，无回退。off若已全对，只记未观察到收益，不换题追分。
2. md-002/md-018四控制臂在两条件均通过；不把md-002这种相同输入的随机波动算处理收益。
3. 全部16次可评分；未知标签不移出分母，原始响应/候选/Prompt hash、错误与usage完整封存。
4. 16次用量均可观察，observed总tokens不超过off的**1.25倍**。任何未知usage不通过成本门槛；这是token开销门槛，**不是金额估计**，实际账单未知。

通过也仅说明这个已知四题开发canary值得继续研究，不自动推广或证明总体收益；否则保持off，不事后调整门槛。

## 证据

[机器简报](benchmark-luna-value-context-ready-2026-09-06.json)；完整工件：`.forge/benchmarks/categorical-value-context-20260906/`。主要证据为control/treatment冻结、http-replay-proof、pi-smoke-final、verification-final、environment-change、smoke-reproduction与file-hashes JSON。
