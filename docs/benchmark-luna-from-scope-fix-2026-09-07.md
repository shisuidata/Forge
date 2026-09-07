# FROM作用域绑定维护：零模型调用测试简报

- 需求：REQ-2026-09-06-055
- 状态：已验证完成
- 本轮模型调用：**0**
- 机器证据：[同名JSON](benchmark-luna-from-scope-fix-2026-09-07.json)

## 结论

共享Assurance已从`query-assurance-v8`升级到`query-assurance-v9`，Policy仍为v9。REQ-054处理组md-009 Forge声明了cze/svk，却用`SELECT cze.cnt - svk.cnt FROM gasstations`引用它们；现在在执行前返回`assurance / unknown_schema_reference`，不再放行到SQLite报错。

**这修复的是漏拒，不是生成准确率。** 不自动补JOIN，不改Compiler、Schema、Prompt、Gold、评分或原候选。value_context保持默认off，不追加生成或启动seed49，滚动父Run仍为seed48。

## 根因与实现

SQLGlot的`Scope.sources`包含可用的CTE定义，不代表它们已在当前FROM/JOIN中绑定。原字段qualify接受了这个前缀，单靠它不能防止本次漏拒。

`forge/assurance.py`在qualify后检查实际关系绑定，并沿SQLGlot作用域传播合法相关环境：

- 本层使用实际FROM来源；子查询可以引用合法外层关系。
- UNION分支与FROM派生源保留各自继承环境，不把本层相邻FROM别名交给非LATERAL派生表。
- CTE按实际使用位置校验；同一CTE在不同相关环境下分别检查，不全局展开所有可用定义。
- 递归按Scope和环境去重；列是否存在仍由原qualify负责。
- 通过时保留原SQL，不将内部qualified AST输出为另一份SQL。

公共Forge JSON、Direct SQL与Benchmark沿用同一共享门禁，没有新增执行旁路或授权。

## 历史候选误拒检查

对固定保存的SQL分别执行v8/v9共享Assurance，不重新生成候选：

| 固定集合 | SQL数 | v8通过 / 拒绝 | v9通过 / 拒绝 | 新增拒绝 |
|---|---:|---:|---:|---|
| R500双臂 | 1000 | 997 / 3 | 997 / 3 | 无 |
| REQ-054旧对照 | 8 | 8 / 0 | 8 / 0 | 无 |
| REQ-054旧处理 | 8 | 8 / 0 | 7 / 1 | md-009 Forge |
| 合计 | **1016** | **1013 / 3** | **1012 / 4** | **仅已知漏拒样本** |

旧正确候选新增误拒为0。R500原有md-006、md-119、md-339 Forge拒绝均未改变，不能计为本轮新增拒绝。

本轮没有重新执行R500查询/Gold或改写其评分；REQ-053已有未知状态保留。13份历史来源文件SHA-256均与维护前一致。

另对REQ-054全部16个原候选执行当前上下文诊断重放：输出、SQL、EX和Contract全部不变。唯一状态变化是处理组md-009 Forge：`execution_status: failed → skipped`，`error_code: unknown_column → unknown_schema_reference`。旧错误只是提前被拒绝，没有变成正确答案。

## 行为与真实入口验证

- 修复前：4个共享SQL反例没有按预期拒绝；使用有效Forge JSON的公共Evaluate反例也错误返回passed。初版无效JSON夹具没有进入目标门禁，不计为复现证据。
- 修复后：CTE绑定、改名、限定星号、合法多层相关、别名遮蔽、递归、派生表祖先相关及CTE不同使用位置均有回归覆盖；合法例实际执行SQLite并比较结果。
- 65个独立SQLite作用域夹具：原qualify已接受且SQLite可执行的查询没有新增误拒；不放宽SQLGlot已有资格校验限制。
- 18项附加静态检查：五个现有Compiler方言的合法/未绑定对照10项、LATERAL Scope 6项、SEMI/ANTI 2项。**未安装或运行外部数据库；非SQLite及显式LATERAL不构成运行时兼容性证明。**
- 真实`forge evaluate`经本地鉴权HTTP：原Forge JSON和原Direct SQL均拒绝，CLI exit 1；合法CTE通过，exit 0，仍为`execution_authorized=false`。
- 合法返回SQL另行在公共BIRD数据库以只读连接执行，结果`[[44], [45]]`与独立SELECT一致。此执行是显式烟测，不是Evaluate授予执行权。
- 未鉴权公共请求返回401；旧冻结因源码漂移返回409。

## 冻结与评分边界

旧v4版本化候选即使显式`--diagnostic`，也不能越过预声明变量去接受评价器源码变化。本轮未放宽该门禁：保留原Run、协议和文件hash，将未修改输出提取到独立`bird-candidates-v1`诊断账本，再使用当前冻结重放。

两份诊断均完整、模型调用0；`compare`仍拒绝晋级。这不是新一轮Luna运行、正式配对收益或R500新成绩。

## 验证与清理

- 聚焦：56 passed。
- Python全套：887 passed / 26 skipped。
- Pi：151 passed；TypeScript typecheck通过。
- 仅见既有protobuf UTC弃用与Node实验性SQLite警告。
- 本地烟测服务已关闭，端口不可连接；未启动Pi生成服务。
- 已移除6个临时脚本/输入文件，复现材料保留在`historical-reproduction.json`和`smoke-fixtures.json`；35份证据文件已记录SHA-256。

证据目录：`.forge/benchmarks/from-scope-assurance-20260906/`。关键文件包括`historical-current-gates.json`、`diagnostic-invariants.json`、`live-http-smoke.json`、`public-cli-smoke.json`、`current-boundary-validation.json`、`full-regression.json`和`artifact-fingerprints.json`。

当前状态、唯一主动计划、Requirement和Benchmark规范已回写。原取值上下文实验未达采纳门槛的结论不变；H及外部采用门禁未推进。
