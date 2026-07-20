# Forge 客户 PoC 执行手册

## 目标和验收线

PoC 面向单业务域、单只读数据库或只读副本，验证 Forge 在客户真实口径下是否可控，而不是做通用 benchmark 展示。

- 高频问题 20–50 条，至少覆盖核心指标、常见维度、时间窗口、JOIN 和无结果场景。
- Run ACC `90%+`，核心指标分类 `90%+`，编译失败率接近零。
- 所有 SQL 执行前人工审核；数据库权限、超时、行数上限和审计全部开启。
- 每个失败都进入 Registry、compiler/lint、方言兼容或模型推理边界之一。

## 目录与数据准备

在客户私有环境创建独立目录，不把 schema、问题集、结果或密钥提交到 Forge 公共仓库：

```text
customer-poc/
├── registry/
│   ├── schema.registry.json
│   ├── metrics.registry.yaml
│   ├── disambiguations.registry.yaml
│   └── field_conventions.registry.yaml
├── cases.json
├── database.db
└── results/
```

仓库提供了 `customer-poc-template/` 作为起点，包含 `cases.example.json`、`failure_triage.template.md`、`delivery_report.template.md` 和空 Registry 文件。复制模板后再填入客户私有内容：

```bash
cp -R customer-poc-template /path/to/customer-poc
```

当前 EA 自动比较器以 SQLite fixture 为标准入口。客户可以使用脱敏后的最小数据副本；真实 PostgreSQL/MySQL 连接用于 compatibility smoke 和最终人工审核，不要把生产数据复制进仓库。

`cases.json` 沿用现有格式：

```json
[
  {
    "id": 1,
    "category": "核心指标",
    "difficulty": 2,
    "question": "各渠道本月支付 GMV 是多少？",
    "reference_sql": "SELECT ..."
  }
]
```

reference SQL 必须由客户数据负责人确认，不能由待评估模型自行生成后直接当作标准答案。

## 执行流程

1. 使用只读连接同步结构层：

```bash
forge sync --db "$DATABASE_URL" --out customer-poc/registry/schema.registry.json
```

2. 录入 5–10 个核心原子指标及必要衍生指标，补充歧义和字段约定。
3. 复制一个现有 `tests/accuracy/methods/method_*.py`，只调整客户 Registry、cases、provider 和模型，不在文件中写 API Key。
4. 每题至少运行三次：

```bash
python tests/accuracy/runner.py --method <id> --runs 3 --retry 2 --fresh
python tests/accuracy/evaluate_ea.py --methods <id> \
  --cases /absolute/path/customer-poc/cases.json \
  --db /absolute/path/customer-poc/database.db --save
python tests/accuracy/triage_failures.py --method <id> \
  --cases /absolute/path/customer-poc/cases.json
```

5. 根据 `failure_triage.md` 修 Registry 或工程规则，然后完整回归，不只重跑失败题。
6. 交付前运行生产 smoke：

```bash
bash scripts/production-smoke.sh
```

`production-smoke` 默认只对客户数据库做 `SELECT 1`，不会创建表、写数据或执行客户查询；provider smoke 只验证 tool call/schema/compile，不执行 SQL。

## 失败处理

| 根因 | 落点 |
|---|---|
| 字段、枚举或指标口径缺失 | Registry |
| 可形式化的 SQL 生成错误 | compiler / lint / Schema |
| PostgreSQL、MySQL 等行为差异 | 方言兼容测试 |
| 问题本身有歧义 | 澄清规则和产品交互 |
| 需要模型先想到复杂算法 | 标记能力边界，不伪装成可稳定支持 |

每个已修复失败样本都要保留在客户 cases 中。核心指标失败优先级高于普通探索查询。

## 交付物

- 脱敏 schema 与 Registry 版本。
- 客户确认的问题集和 reference SQL。
- `runs.json`、`ea.json`、`failure_triage.md`。
- 数据库 compatibility smoke 结果、provider smoke 结果和 `forge doctor` 输出。
- 已知能力边界、未解决问题、升级和回滚说明。
- 填写完成的 `delivery_report.md`，明确 pass / conditional pass / blocked。
