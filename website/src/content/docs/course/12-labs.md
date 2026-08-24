---
title: 12｜完整实战课程
summary: 用 large Demo 从安装、DSL、Registry、ACL 到生产门禁
---

# 完整实战课程

> 所有 secret 只通过环境变量传入。不要把真实 API Key、数据库密码写进 Markdown、shell history 或 git。

## 公共前置条件

```bash
git clone https://github.com/shisuidata/Forge
cd Forge
bash scripts/bootstrap-dev.sh
source .venv/bin/activate
cp .env.example .env
```

需要真实 LLM 的实验用占位符：

```bash
export LLM_API_KEY='<YOUR_KEY>'
export LLM_PROVIDER='openai'
export LLM_BASE_URL='https://your-provider.example/v1'
export LLM_MODEL='your-model'
```

## 实验 1：启动与开发门禁

**目标**：理解“能启动”和“可生产”不是一回事。

```bash
forge doctor --profile dev
uvicorn main:app --host 127.0.0.1 --port 8000
```

另开终端访问 `http://127.0.0.1:8000/health` 和 `/health/readiness?profile=dev`。

**预期**：health 为 ok；doctor 可能是 warn，但不应因缺生产认证阻断开发。

**常见失败**：依赖未安装、端口占用、审计目录不可写。

**练习**：比较 `dev` 与 `prod` 输出。为什么同一配置在 prod 会 fail？

**答案**：生产将认证、只读确认、LLM、timeout、Secure Cookie、Registry、audit 等提升为强门禁。

## 实验 2：认识 200 表 Demo

**目标**：观察大 Schema 的上下文问题。

```bash
bash scripts/demo-setup.sh
python - <<'PY'
import json
p='tests/datasets/large/schema.registry.json'
r=json.load(open(p))
print('tables =', len(r.get('tables', {})))
print('sample =', list(r.get('tables', {}))[:10])
PY
```

**预期**：看到大规模表集合和 Demo Registry。

**练习**：如果把所有表都放入 prompt，会产生哪三类成本？

**答案**：token、注意力噪声、权限泄漏风险。

## 实验 3：追踪自然语言到 SQL

**目标**：观察 Web Chat 的 review，而不是直接执行。

1. 配置 LLM 后启动服务；
2. 在 `/chat` 提问“统计最近 30 天各城市已完成订单数”；
3. 查看返回的 Forge JSON/SQL；
4. 暂不确认，检查 Audit 中 pending 状态；
5. 确认后观察结果、row_count 和 execution time。

**常见失败**：Provider 不支持 tool calling、Schema 过大导致输出截断、数据库未配置。

**练习**：在确认前修改问题中的时间范围，为什么应重新生成而不是直接改结果？

**答案**：结果必须由可审核 SQL 可重复地产生。

## 实验 4：手工编译 DSL

**目标**：验证 Compiler 的确定性和 anti join 语义。

```bash
cat > /tmp/forge-query.json <<'JSON'
{
  "scan": "orders",
  "joins": [{
    "type": "anti",
    "table": "refunds",
    "on": {"left": "orders.id", "right": "refunds.order_id"}
  }],
  "select": ["orders.id"]
}
JSON
forge compile /tmp/forge-query.json
forge compile /tmp/forge-query.json
```

**预期**：两次输出一致。具体 SQL 以当前 Compiler 为准。

**练习**：解释 `NOT IN` 在子查询含 NULL 时的风险。

**答案**：SQL 三值逻辑会让比较变为 UNKNOWN，可能意外过滤全部行；显式 anti 语义更安全。

## 实验 5：建设 Registry

**目标**：新增一个指标、歧义规则和字段约定。

在独立练习目录复制 `registry/data`，不要直接污染共享 Demo。新增：

- 指标 `paid_gmv`；
- “销售额”的 disambiguation；
- `orders.total_amount` 与明细金额的 convention。

运行：

```bash
pytest tests/test_metric_validator.py tests/test_staging_sync.py -q
```

**练习**：为什么衍生指标应引用原子指标，而不是复制两段 SQL？

**答案**：减少口径重复和漂移，便于依赖检查与复用。

## 实验 6：向量检索与 BM25 降级

**目标**：理解降级不是失败。

- 配置 `EMBED_API_KEY` 时运行一次相关表检索；
- 临时在单独 shell 中取消该变量，观察 BM25 路径；
- 比较 top-k 和 golden tables。

```bash
pytest tests/test_registry_context.py -q
```

**练习**：为什么不能只比较 token 减少比例？

**答案**：压缩若漏掉关键 JOIN 表，会降低最终正确率；必须同时评估 recall@k 和 EA。

## 实验 7：审核、取消、审计与反馈

**目标**：走通可信执行闭环。

1. 生成 SQL 后点击取消，查看 audit 状态；
2. 重新提问并确认；
3. 对结果提交“准确/不准确”反馈；
4. 在 Admin 的 Audit、Sessions、Staging/Knowledge 页面查看记录。

```bash
pytest tests/test_api.py tests/test_audit.py tests/test_feedback.py -q
```

**练习**：为什么 cancelled 也要审计？

**答案**：取消反映用户不信任、需求变化或 SQL 风险，是改进和合规证据的一部分。

## 实验 8：Team ACL

**目标**：验证权限在生成前生效。

在 Admin 创建团队、加入测试用户，只授权部分表；用该用户提出涉及未授权表的问题。

**预期**：未授权表不进入可见 Schema/Tool 上下文，系统给出权限相关提示，而不是泄漏完整结构后才执行失败。

```bash
pytest tests/test_agent_runtime.py tests/test_api.py -q
```

**练习**：表 ACL 能否代替行级权限？

**答案**：不能；行列级限制与脱敏应由数据库或专门权限层执行。

## 实验 9：最小质量回归

**目标**：区分结构正确、执行正确和业务正确。

```bash
pytest tests/test_compiler.py tests/test_compiler_extended.py tests/test_compiler_window.py -q
pytest tests/test_executor.py tests/test_lint.py -q
```

阅读 `tests/text-to-sql-failures/`，将失败归为生成、业务或算法逻辑错误。

**练习**：编译测试全部通过，能否说明客户问数准确？

**答案**：不能；还需要客户 Schema、Registry 和执行结果的 accuracy suite。

## 实验 10：PoC 到生产门禁

**目标**：理解交付证据包。

```bash
forge poc init /tmp/forge-customer-poc
forge poc validate /tmp/forge-customer-poc
forge doctor --profile poc --json
python scripts/provider_smoke.py --json --out /tmp/provider-smoke.json
```

有真实只读测试库时，再运行 production smoke。不要为了“变绿”伪造 `DATABASE_READONLY_CONFIRMED=true`。

**练习**：列出生产必须有而 Demo 可以没有的三项。

**答案示例**：真实只读账号/副本、认证与 HTTPS、客户 Registry/golden questions；还包括持久化备份、监控和回滚。

## 课程项目

选择一个小型业务数据库：

1. 同步 Schema；
2. 建 5 个指标、3 条歧义规则、3 条字段约定；
3. 建 30 条 golden questions；
4. 运行 3 次重复测试并报告 EA(any)、EA(all)、Run ACC；
5. 提交架构图、威胁模型、readiness 结果和失败分类；
6. 明确写出拒绝回答或需要人工分析的问题。
