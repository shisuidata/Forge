# Luna 生成阶段定向诊断简报（2026-09-06）

## 结论

本轮诊断已完成，**未复现超时/取消，也没有准确率提升**。4次生成均正常结束、SQL均可执行；原始Official EX与Contract均为0/4。现有证据不足以给seed48的旧超时归因，不延长120秒上限、不改Prompt/Schema、不追加调用。

这是REQ-051的两道已曝光题定向诊断，不是滚动seed49；seed48仍为滚动父Run，不补跑其未启动题、不回填旧未知usage、不晋级正式基线或H。

## 协议、调用与成本

- 用户选择“执行4次诊断”，授权上限4；实际派发4、完成4，零重试/续跑/替换候选。
- 模型：`openai-codex / gpt-5.6-luna`；日期/粒度均off，每臂120秒、一次调用。两个单题双臂Run按题串行，避免跨题连带取消。
- 实验观察器仅订阅SDK事件并聚合时间/计数；不改变原Prompt、工具Schema、模型输出或Pi状态。4份实际Prompt SHA-256均与seed48对应臂一致；观察器源码hash与预检一致。
- 已观测 **19,831 tokens**，未知用量臂0；其中缓存读取1,664，不能按总tokens直接推导金额。合成流烟测用量不计入此数。
- md-234 Run：`pbr_6de6e90926754293bd6e71f1a41e7e14`；md-447 Run：`pbr_6e942be1bdd6400a8f37649b27b7b4ed`。二者均为completed；这是运行终态，不表示答案正确。

| 题目 | 臂 | 生成耗时 | 可见thinking区间 | 可见工具参数/文本区间 | tokens | EX / Contract |
|---|---|---:|---:|---:|---:|---|
| md-234 | forge | 11.18s | 3.245–7.890s | toolcall 7.890–11.161s | 5,320 | 失败 / 失败 |
| md-234 | direct | 12.98s | 3.385–12.372s | text 12.372–12.972s | 3,555 | 失败 / 失败 |
| md-447 | forge | 23.60s | 2.033–16.080s | toolcall 16.082–23.546s | 6,817 | 失败 / 失败 |
| md-447 | direct | 9.59s | 2.723–7.999s | text 8.000–9.563s | 4,139 | 失败 / 失败 |

区间以SDK `prompt`进入为零点，是首末事件包络，可能包含间隙；生成耗时另含会话准备等运行开销。thinking事件是可见摘要，不是内部推理时长。首次可见片段前的2.03–3.38秒无法区分网络、排队或隐藏推理。Forge工具参数输出包络分别约3.27秒与7.46秒，终止原因为toolUse；Direct均为stop。本轮没有接近120秒上限的样本，不能据此证明尾延迟问题消失。

## 逐题结果与只读差异隔离

### md-234：题目要求次数，Gold未返回次数

问题为“How many times the circuits were held in Austria? Please give their location and coordinates.”；Evidence仅定义坐标与国家过滤。Gold为：

	SELECT DISTINCT location, lat, lng FROM circuits WHERE country = 'Austria'

Gold是2行×3列。Forge返回2行×4列，按地点/坐标合并计数，次数为29、1；Direct返回3行×4列，按circuitId分组，同一组坐标被拆成25与4。两臂均按原评分失败；Forge是列数不符，Direct先触发行数不符。

只读反事实：仅删除COUNT投影，不改其他SQL，两臂Official EX均可匹配Gold；但Direct仍为3行，行多重集不等于Gold。**这不构成删除用户所求次数的理由，也不证明Direct的Contract通过。** 原候选没有修改；不能仅因不匹配Gold就把额外计数认定为生成错误。

### md-447：标签与编码的参考输出差异

两臂均返回57行，第二列选`DOCType`，Gold选择`DOC`。模型可见元数据明确写明：DOC是类别编码；DOCType是该类别的文字描述。例如原候选为“County Office of Education (COE)”，Gold为“00”。

只读反事实：仅把第二个投影从DOCType改成DOC，保留原CTE/平均值范围/过滤/连接，**两臂均得到与Gold完全相同的行多重集**。这把本次结果差异定位到输出表示，而不是平均值公式或执行性能。反事实结果不作为实际候选得分；不自动把编码/标签差异当作业务歧义或澄清触发器，也不硬编码DOC映射。

## 验证与决定

- 真实Pi SDK正常结束、取消两条合成流预检：零网络；随后仅执行获批的4次真实生成。
- 4份Prompt与响应hash核验；观察终止原因/内容类型与Pi原始响应一致；全部4臂用量可见、各派发一次。
- 两份冻结协议下重放4个原候选，SQL、执行/评分/失败状态和原输出一致；另以只读数据库执行独立复核4个原SQL的Official EX。
- 封存2份完整Run、57条日志、4条阶段记录、冻结协议、原始候选重放及Pi数据库快照；快照integrity_check为ok，含2个Run/2个case/57条benchmark日志。
- 实验服务已手动停止，18769/18770均无监听。未修改生产源码；本轮不重复运行既有全套测试，不将前次测试数当成本轮证据。

**不采纳新的生成策略改动。** 本轮不足以解释旧超时，也不能证明REQ-050改善了生成延迟；它只补足当前可观测性并暴露结果契约与参考输出的具体差异。不得为追分删除次数、强换编码、延长上限或继续重抽样。

## 工件

- [机器可读报告与25份工件SHA-256](benchmark-luna-generation-stages-2026-09-06.json)
- 原始实验目录（仅本地，未随仓库发布：`../.forge/benchmarks/luna-generation-stages-20260906/`）
- 阶段、重放与hash验证（仅本地，未随仓库发布：`../.forge/benchmarks/luna-generation-stages-20260906/evidence-validation.json`）
- 原SQL、Gold与未采纳的反事实差异（仅本地，未随仓库发布：`../.forge/benchmarks/luna-generation-stages-20260906/result-differences.json`）

准备卡保留授权前状态；实际授权以`authorization.json`及本报告为准。完整原始响应只保存在工件中；本报告不展开可见thinking摘要或加密内容。
