# Forge 客户 PoC 模板

这个目录是客户私有环境的起点模板。复制到客户项目目录后再填入真实 schema、问题集、脱敏数据和 smoke 结果；不要把客户密钥、原始数据或未脱敏结果提交到 Forge 公共仓库。

## 推荐目录

```text
customer-poc/
├── registry/
│   ├── schema.registry.json
│   ├── metrics.registry.yaml
│   ├── disambiguations.registry.yaml
│   └── field_conventions.registry.yaml
├── cases.json
├── failure_triage.md
├── delivery_report.md
└── results/
```

## 标准流程

1. 用只读账号同步结构层：

```bash
forge sync --db "$DATABASE_URL" --out registry/schema.registry.json
```

2. 让客户数据负责人确认 `cases.json` 中每条 `reference_sql`。
3. 每题至少运行 3 次，保存 `runs.json`、`ea.json` 和失败归因。
4. 把业务口径错误沉淀到 Registry，把可形式化生成错误沉淀到 compiler/lint/schema。
5. 交付前运行：

```bash
bash scripts/production-smoke.sh
```

## 验收线

- 高频问题 Run ACC >= 90%。
- 核心指标分类 >= 90%。
- 编译失败率接近 0。
- `forge doctor` 无 fail。
- SQL 执行默认人工审核，数据库账号由客户 DBA 确认为只读。
