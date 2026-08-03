# Forge 可信问数商业化就绪清单

> 当前结论：Forge 已达到“受控生产落地 / 封闭 Beta”候选标准。Forge 的商业化目标不是单纯生成 SQL，而是解决 AI 参与数据查询时的可信度问题：业务口径可信、生成可信、执行可信、追溯可信、兼容证据可信。正式规模化交付还需要继续补真实客户业务域基准、Registry 管理 UI、规则租户化和多用户权限体系。

---

## 当前已补齐

### 1. 模型效果与准确率基准

large 40 题 DeepSeek V4 Pro Method AF：

- Case EA(any)：`100.0%`（40/40）
- Case EA(all)：`92.5%`（37/40）
- Run ACC：`97.5%`（117/120）
- 编译失败率：`0.0%`（120/120 成功）

这已经超过此前设定的正式交付前建议目标：

- 高频业务查询 Run ACC `90%+`
- Case EA(all) `85%+`
- 编译失败率接近 `0%`

### 2. SQL 执行安全

- 只允许只读 `SELECT/WITH` 查询。
- 禁止多语句堆叠执行。
- 禁止 `DROP/DELETE/UPDATE/INSERT/ALTER/PRAGMA` 等明显非只读关键字。
- 支持通过配置关闭全部 SQL 执行：`EXECUTION_ENABLED=false`。
- 支持通过配置关闭手动 SQL 执行入口：`RAW_SQL_ENABLED=false`。
- 支持部署级结果行数上限：`EXECUTION_MAX_ROWS`。
- 支持文本展示行数上限：`EXECUTION_DISPLAY_ROWS`。
- 支持查询超时：`EXECUTION_TIMEOUT_SECONDS`。
- 支持生产交付前强制确认数据库只读账号：`DATABASE_READONLY_CONFIRMED`。

### 3. 审计闭环

- 审计库路径可配置：`AUDIT_DB_PATH`。
- 审计表记录用户问题、Forge JSON、SQL、状态、错误信息。
- 执行结果记录 `row_count`、`execution_ms`。
- `/api/approve` 执行后会回写最近一条 pending 审计记录。
- `/api/cancel` 会把最近一条 pending 审计记录标记为 cancelled。
- `/api/execute-raw` 会记录执行状态、错误、行数和耗时。
- `/api/feedback` 可提交 SQL/结果错误反馈，进入 `feedback_log` 待处理队列。

### 4. 部署就绪检查

新增接口：

```text
GET /health/readiness
```

检查项包括：

- Auth 是否开启。
- 管理员密码是否仍为空或默认值。
- SQL 执行开启时是否配置了 `DATABASE_URL`。
- LLM API Key 是否配置。
- 手动 SQL 执行入口是否开启。
- 最大返回行数是否过大。
- 查询超时是否配置。
- 是否确认数据库使用只读账号。
- Secure Cookie 是否开启。
- Registry 文件是否齐备。
- 审计目录是否可写。

返回状态：

```text
ok    = 可用
warn  = 可运行但存在生产风险
fail  = 缺少关键配置
```

### 5. Admin 设置

Admin 设置页新增 `SQL 执行安全` 配置区：

- 启用/关闭 SQL 执行。
- 启用/关闭手动 SQL 执行。
- 配置最大返回行数。
- 配置文本展示行数。
- 配置查询超时。
- 确认数据库账号只读。

### 6. 生产部署包

- `Dockerfile` 默认使用非 reload 的生产启动命令。
- `docker-compose.yml` 保留开发热重载命令。
- `docker-compose.prod.yml` 提供生产部署模板、持久化 `.forge`、只读挂载 `registry/data`、健康检查。
- `.env.production.example` 提供生产环境变量模板。
- `.dockerignore` 避免把本地密钥、虚拟环境、测试结果和图表缓存打入镜像。
- `docs/production-deployment.md` 记录生产部署步骤、数据库只读账号 SQL、readiness 检查项和运维建议。
- `forge doctor` 可在 CLI 中检查当前配置是否满足交付要求。

---

## 当前商业化判断

如果“商业化推广”指的是直接大规模 SaaS 售卖，Forge 还没有完成。

如果定义为“可以进入生产环境的受控落地”，当前已经具备条件：

- 模型效果已过线：Run ACC `97.5%`，Case EA(all) `92.5%`。
- 生成链路可维护：自然语言到 Forge JSON，再确定性编译成 SQL。
- SQL 可审计：审核者看到的 SQL 就是实际执行 SQL。
- 执行可控：只读限制、执行开关、行数上限、查询超时、审计记录已经具备。
- 错误可沉淀：lint、Registry、accuracy runner、failure triage 已经形成闭环雏形。
- 部署可落地：已有生产 compose、env 模板、readiness/doctor 检查和生产部署文档。
- 生态可嵌入：外部 Agent 第一版通过 `prepare_query` 获取待审核 SQL，不能绕过 Forge 审核链路直接执行。

更准确的定位是：

```text
可进入受控生产落地 / 封闭 Beta，不建议无约束规模化铺开。
```

---

## 当前仍需补齐

### P0：真实数据库安全兜底

当前已补应用层只读校验、查询超时和只读账号确认检查，但应用层 SQL 校验不能替代数据库权限。`DATABASE_READONLY_CONFIRMED=true` 只能在客户环境已经验证真实只读账号之后设置；`forge doctor --profile poc|prod` 因该项失败时，应视为可信执行门禁未通过，而不是可忽略告警。商业部署仍必须要求：

- Forge 使用只读数据库账号。
- 禁止连接生产主库写账号。
- 配置资源隔离或只读副本。
- 对敏感表做数据库层权限隔离。

### P0：真实业务域基准

large 40 题已经证明技术路线成立，但客户交付还需要：

- 每个客户 schema 建自己的 accuracy suite。
- 核心指标、财务指标、经营看板类问题单独建高优先级测试集。
- 每次线上错误必须能回放、归因、沉淀。

### P1：Registry 管理体验

商业用户需要通过 UI 管理组织知识，而不是直接编辑 YAML：

- 指标定义 CRUD。
- 字段约定 CRUD。
- 歧义消除规则 CRUD。
- 规则影响范围预览。
- 变更历史和回滚。

### P1：规则租户化

本轮为了提升 large schema 准确率，部分高频问题契约已经进入 lint。正式产品里应继续把这些规则下沉为 dataset/tenant-specific Registry 规则：

- 避免全局规则过拟合某个 benchmark。
- 支持不同公司对同一概念有不同字段和口径。
- 让规则可以被管理员审核、启停和回滚。

### P1：运维与部署

当前已有生产部署模板和说明，下一步需要补齐客户交付级运维材料：

- 备份、升级、回滚 runbook。
- 日志采集和告警接入样例。
- 客户网络、反向代理、HTTPS 部署示例。
- 多 LLM provider 配置样例。

### P2：权限与多租户

已有 team table ACL 思路，但商业化还需要：

- Admin 角色和普通用户角色分离。
- API Key 作用域。
- 团队级 Registry 隔离。
- 审计日志按团队过滤。
- 数据源权限和用户权限联动。

---

## 建议落地路径

### 阶段 1：受控生产落地

适合对象：1-3 个设计型客户或内部数据团队。

范围：

- 单业务域。
- 单数据库或只读副本。
- SQL 默认人工审核后执行。
- 所有错误样本每周复盘。

成功标准：

- 客户高频查询 Run ACC `90%+`。
- 核心指标口径全部进入 Registry。
- 用户反馈能稳定转成规则或测试。

### 阶段 2：封闭 Beta

范围：

- 多业务域。
- 多用户。
- Admin UI 管理 Registry。
- 审计和权限完整启用。

成功标准：

- 线上查询可追踪、可回放、可纠错。
- 核心指标查询稳定。
- 错误率持续下降，而不是靠人工兜底。

### 阶段 3：正式商业交付

前置条件：

- 客户业务域准确率稳定达标。
- 安全边界由数据库权限和应用层共同保证。
- 部署、升级、备份、审计流程标准化。
- 有清晰的“不能回答/需要澄清/需要人工审核”机制。

---

## 本轮新增工程改动

- `forge/lint.py`：补充 large schema 字段契约、输出契约、排序契约、过滤口径、窗口和反连接稳定性检查。
- `tests/test_lint.py`：lint 回归测试扩展到 `53 passed`。
- `tests/datasets/large/field_conventions.registry.yaml`：补充 Method AF 所需的字段和结果契约。
- `tests/accuracy/methods/method_ae.py`：稳定性实验方法。
- `tests/accuracy/methods/method_af.py`：当前推荐 DeepSeek V4 Pro 基线。
- `docs/test-report-2026-05-06.md`：更新本轮测试报告。
- `config.py`：生产认证开关支持 `AUTH_ENABLED` 环境变量，补充查询超时和数据库只读确认配置。
- `forge/executor.py`：执行器增加查询超时兜底。
- `main.py`：readiness 增加数据库只读确认、查询超时、Secure Cookie 检查。
- `forge/cli.py`：新增 `forge doctor` 交付检查命令。
- `Dockerfile` / `docker-compose.prod.yml` / `.env.production.example` / `.dockerignore`：补齐生产部署包。
- `docs/production-deployment.md`：新增生产部署说明。
