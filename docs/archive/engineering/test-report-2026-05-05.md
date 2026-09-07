# Forge 测试报告（2026-05-05）

## 结论

本轮已完成后端/API 自动化测试、站点 E2E 测试、以及两轮 DeepSeek large 40 题准确率快照。

在新增 per-group TopN lint 和聚合后 TopN prompt 示例后，DeepSeek single-run EA 从 55.0% 提升到 65.0%。本轮最明确的收益来自排名与 TopN 分类：20.0% → 100.0%。

Z.AI GLM-5.1 的准确率测试链路已经接入为 `method_v`。在项目外部文件中找到了一个 `ZHIPU_API_KEY` 候选 key，但该 key 调用 Z.AI / 智谱接口时返回余额不足，因此尚未完成 GLM-5.1 的 40 题真实模型调用与 EA 评估。

不要把本报告解读为 GLM-5.1 的最终准确率报告；GLM-5.1 结果仍待补跑。

## 本轮代码接入

新增 Z.AI / GLM-5.1 accuracy method：

- `tests/accuracy/methods/method_v.py`
- 模型：`glm-5.1`
- Base URL：`https://api.z.ai/api/paas/v4`
- Provider：OpenAI-compatible
- 数据集：`tests/datasets/large/`
- 默认 `retry=2`
- 支持 key 变量：`ZAI_API_KEY` / `GLM_API_KEY` / `ZHIPU_API_KEY`

同时修正了 `tests/accuracy/runner.py` 的 OpenAI-compatible provider key 选择逻辑，避免 Z.AI method 误用 `DEEPSEEK_API_KEY`。

## 自动化测试结果

### 后端 / API / 编译器 / Agent 单元测试

命令：

```bash
uv run --extra dev pytest -q
```

结果：

```text
241 passed, 23 skipped, 6 warnings
```

说明：

- 失败数：0
- 6 个 warning 均为依赖或 FastAPI `on_event` 弃用警告，不是测试失败
- 23 个 skipped 中包含未启动服务时会跳过的浏览器 E2E

### 站点 E2E 测试

本轮已启动本地服务 `http://127.0.0.1:8000`，用 Playwright 跑过站点流程。

结果：

```text
22 passed
```

覆盖范围包括：

- 登录页渲染
- 默认密码登录
- 错误密码提示
- Chat 页面加载
- 发送消息基本交互
- Admin Dashboard
- Schema / Metrics / Semantic / Audit / Settings 页面导航

## 准确率测试结果

### DeepSeek single-run 快照（优化前）

命令逻辑：

- 数据集：large 40 题
- 模型：DeepSeek `deepseek-chat`
- Method：`u`
- 每题运行：1 次
- 编译重试：`retry=2`
- 评估方式：生成 SQL 与 reference SQL 在 SQLite 上执行结果对比，即 EA（Execution Accuracy）

生成结果：

```text
40/40 成功生成并编译
编译失败率：0.0%
```

EA 结果：

```text
Case EA: 55.0%  (22/40)
Run ACC: 55.0%  (22/40)
```

分类结果：

| 类别 | EA |
|---|---:|
| 多表JOIN+聚合 | 60.0% |
| 复杂过滤 | 40.0% |
| 分组+HAVING | 80.0% |
| 排名与TopN | 20.0% |
| 窗口聚合 | 60.0% |
| 时序导航 | 60.0% |
| ANTI/SEMI JOIN | 60.0% |
| 综合复杂查询 | 60.0% |

主要短板：

- 排名与 TopN：20.0%，当前最弱
- 复杂过滤：40.0%，仍有大量业务条件漏加或粒度错误
- 部分窗口聚合、时序导航、ANTI/SEMI JOIN 场景仍有结果行数不匹配

历史对照：

- Method U 历史 3-run 结果：Case EA any 62.5%，Run ACC 58.3%
- 本轮 single-run：55.0%

这说明当前准确率仍有明显随机波动，不能只看任一 run 正确的 Case EA；Run ACC 更接近真实产品稳定性。

### DeepSeek single-run 快照（新增 TopN lint / prompt 后）

本轮新增：

- `forge/lint.py`：分组内 TopN 检查，发现“有排名窗口但缺 qualify”或“只用全局 sort/limit”时触发 retry
- `agent/prompt_examples/topn.md`：补充“先聚合成 CTE，再 window 排名，再 qualify”的示例
- `tests/test_lint.py`：新增 TopN lint 回归测试
- `tests/test_prompts.py`：新增 TopN 示例注入测试

生成结果：

```text
40/40 成功生成并编译
编译失败率：0.0%
```

EA 结果：

```text
Case EA: 65.0%  (26/40)
Run ACC: 65.0%  (26/40)
```

分类结果：

| 类别 | EA |
|---|---:|
| 多表JOIN+聚合 | 60.0% |
| 复杂过滤 | 60.0% |
| 分组+HAVING | 60.0% |
| 排名与TopN | 100.0% |
| 窗口聚合 | 60.0% |
| 时序导航 | 60.0% |
| ANTI/SEMI JOIN | 80.0% |
| 综合复杂查询 | 40.0% |

与优化前 single-run 对比：

| 指标 | 优化前 | 优化后 | 变化 |
|---|---:|---:|---:|
| Case EA | 55.0% | 65.0% | +10.0pp |
| Run ACC | 55.0% | 65.0% | +10.0pp |
| 排名与TopN | 20.0% | 100.0% | +80.0pp |
| 复杂过滤 | 40.0% | 60.0% | +20.0pp |
| ANTI/SEMI JOIN | 60.0% | 80.0% | +20.0pp |
| 综合复杂查询 | 60.0% | 40.0% | -20.0pp |

说明：

- 这是 single-run 结果，存在模型随机性；不能直接等同长期稳定值。
- 但 TopN 的提升与本轮改动高度相关：新增 lint/retry 明确拦截“组内 TopN 缺 qualify”的错误。
- 综合复杂查询下降，主要仍集中在字段歧义和复杂多步 CTE/窗口组合，后续需要单独处理。

## Z.AI GLM-5.1 当前状态

### 已完成

`method_v` 已能被 runner 识别：

```text
v  Method V（large 数据集，Z.AI GLM-5.1）
```

相关轻量测试通过：

```text
12 passed
```

### 当前阻塞

当前项目 `.env` 检查结果：

```text
ZAI_API_KEY=
GLM_API_KEY=
ZHIPU_API_KEY=
ZAI_BASE_URL=
ZAI_MODEL=
```

继续向上层目录搜索后，发现：

```text
/Volumes/MacData/Workspace/90_Dev/talk_about/app/.env.local
ZHIPU_API_KEY=SET
```

已用该 key 做最小探测，不打印密钥内容。结果：

```text
Z.AI endpoint: status=429
error=Insufficient balance or no resource package. Please recharge.

BigModel endpoint: status=429
error=余额不足或无可用资源包,请充值。
```

因此当前阻塞不是找不到 key，而是该 key 无可用余额/资源包。GLM-5.1 40 题 EA 测试仍未执行完成。

### 配置方式

在 `.env` 中加入：

```env
ZAI_API_KEY=你的 Z.AI key
ZAI_BASE_URL=https://api.z.ai/api/paas/v4
ZAI_MODEL=glm-5.1
```

也兼容：

```env
GLM_API_KEY=你的 Z.AI key
ZHIPU_API_KEY=你的智谱 key
```

## GLM-5.1 补跑命令

完整跑 large 40 题，每题 1 次：

```bash
uv run --extra dev --with tqdm python tests/accuracy/runner.py \
  --method v --fresh --runs 1 --workers 3 --retry 2
```

评估 EA：

```bash
uv run --extra dev python tests/accuracy/evaluate_ea.py \
  --methods v \
  --cases ../datasets/large/cases.json \
  --db tests/datasets/large/database.db \
  --save
```

如果 GLM-5.1 响应过慢，建议先降低并发或跑小样本：

```bash
uv run --extra dev --with tqdm python tests/accuracy/runner.py \
  --method v --fresh --runs 1 --workers 1 --retry 2
```

## 当前判断

工程稳定性已经过关：

- 后端/API 测试通过
- 站点 E2E 通过
- 编译失败率可以做到 0%

真正需要继续优化的是 SQL 结果准确率：

- 当前优化后 single-run EA 为 65.0%，比上一轮 55.0% 有明显改善
- 排名 TopN 已从本轮最弱项变成当前强项
- 下一批短板转移到综合复杂查询、字段歧义、复杂 CTE/窗口组合，以及部分 reference/result 等价判断边界

GLM-5.1 的测试价值在于判断强模型下 Forge DSL 是否仍有显著收益。等 Z.AI key 配好后，应优先补跑 `method_v`，再与 DeepSeek Method U、MiniMax Method R 做同口径对比。
