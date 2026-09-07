# CTE 字段绑定修复简报（2026-09-06）

## 结论

REQ-052已完成两处确定性修复，**真实模型调用0次**：合法CTE查询不再被Compiler错误改写；公共Forge JSON路径复用现有SQL字段校验，拒绝CTE未输出的字段。**本轮没有BIRD准确率提升证据。**

## 修复内容

### Compiler：保留字段所属关系

此前两段CTE专属改写会互相干扰：

- 把显式`picked.id`、`picked.amount`剥成裸列；与同名物理列JOIN时，合法投影、聚合/分组、窗口分区变成歧义SQL。
- 给裸输出强加主CTE前缀；将窗口别名改成`category_sales.rn`，将只属于右侧CTE的`label`改成`picked.label`。

已删除这两段改写和废弃helper，复用原关系绑定与别名展开。显式select仍定义输出；不补聚合列、不猜缺失字段、不按Gold改答案。

五个真实SQLite结果反例均从失败转为通过：投影、聚合/分组、窗口分区、右侧CTE唯一裸列、CTE窗口Top-N及可见输出。原有只检查SQL前缀的窗口测试改为验证结果行，避免“字符串看起来正确，SQL实际不可执行”。

### Assurance：缺失的CTE输出不得送审

公共负例烟测发现：限定引用`picked.missing`可被旧JSON字段检查放过；不存在的CTE输出在SELECT与WHERE中都可能收到`allow_review`。它们未获得执行授权，但仍是字段保障缺口。

`assure_query`末尾现在调用已有`assure_compiled_sql`，使用同一份已加载、已限定权限的Registry，完成只读、SQL解析、字段和作用域校验；既有关系、业务Policy与前置失败证据保留。不新增第二套CTE解析器。

Assurance修订为 **`query-assurance-v8`**，Policy仍为`convention-policy-v9`。两个“物理字段存在但CTE未投影”的SELECT/WHERE反例从漏拒转为拒绝；歧义裸列拒绝对照仍通过。

## 验证结果

| 验证 | 结果 |
|---|---|
| Compiler真实结果回归 | 5个修复前失败，修复后全部通过 |
| 公共CTE字段拒绝 | 2个漏拒已修复；1个歧义拒绝边界保持 |
| 历史固定候选 | 5份Run中的40个Forge候选，五方言共200项编译结果全部不变 |
| 原拒绝候选重放 | md-032/md-199共4臂，SQL、EX、Contract均与原记录一致 |
| 实际CLI/HTTP | 合法CTE窗口返回allow_review；越域/缺列返回deny；均不授权执行 |
| 实际SQL结果 | API返回SQL在隔离SQLite得到(1,10,1)、(1,20,2)、(2,70,1) |
| Python | 825 passed / 26 skipped |
| Pi | 151 passed；TypeScript typecheck通过 |

md-032仍引用未输出的`qualified_events.event_id`，md-199仍引用未输出的`best_lap.best_ms`；两份Forge候选继续被字段校验拒绝。不是借修复给旧错题改判。

固定候选重放使用v8下的新diagnostic协议，原始5份Run文件hash未变。旧冻结在Compiler变更后已因源码漂移被拒绝；不跨版本resume、不宣称正式可比。未重放完整R500，也没有新生成实验。

## 边界与变更记录

- 修改`forge/compiler.py`、`forge/assurance.py`及相关回归测试；接口结构、Prompt、Schema、Gold、评分和120秒/零重试策略不变。
- 公共路径的既有关系/裸列前置规则未放宽：单独烟测中的CTE→物理表JOIN及仅属于右CTE的裸label仍被相应门禁拒绝。Compiler合法不等于通过所有业务Policy；本轮不宣称这类前置限制已解决。
- Evaluate本身不执行SQL；上述结果执行只发生在本次自建的内存数据库，没有连接真实数据源或绕过生产审批。
- 服务已手动停止，18771无监听。原审批/上下文不能冒充v8校验证据。
- seed48仍为滚动父Run；历史成绩、H与外部采用门禁不变。仓库无独立CHANGELOG，本节与Requirement/主动计划记录本次变更；不改写旧实验版本记录。

[完整机器证据及工件校验和](benchmark-luna-cte-binding-fix-2026-09-06.json) · 原始工件（仅本地，未随仓库发布：`../.forge/benchmarks/luna-cte-bindings-20260906/`）
