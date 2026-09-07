# Forge 架构总览

> 本页是当前架构的稳定入口（2026-09-07）。[`architecture-course/`](architecture-course/index.md) 保留早期 Agent/DSL 教材；其中旧调度图不是当前运行时真相。长期目标见 [`platform-architecture.md`](platform-architecture.md)，产品边界见 [`product-north-star.md`](product-north-star.md)。

Forge 是 **Trust Runtime**：接收外部 Direct SQL 或 Forge JSON 候选，提供 Evaluate → Enforce → Explain。生成候选与授予执行权是两件事，Evaluate 可以独立采用而不接 Pi 或模型。

```text
外部候选 / 本地 CLI ───────────────┐
                                 ▼
Web / 飞书 → Pi Task / Attempt → Forge Trust Runtime
              │                  Evaluate：契约、编译、Assurance、结果比较；不执行
              │                  Enforce：身份与 ACL 交集、Policy、不可变审批、只读执行
              └─ Artifact ◀──── Explain：同一 QueryRun 的 Evidence、血缘、完整性与限制
```

Pi 是多阶段 Task/Attempt/Artifact 的权威；Forge QueryRun 是受治理查询及审批/执行的权威。Web 是认证适配和只读投影，不另建一份 Task。Explain 的历史完整性验证不替代 Enforce 的当前授权。

## 核心架构原则

1. **意图与执行分离**：候选可为 Direct SQL 或 Forge JSON；Compiler 只编译后者，不授予执行权。
2. **组织语义外置**：指标、歧义、字段约定和业务上下文进入 Registry。
3. **高风险动作由人决定**：SQL 与组织事实默认审核后执行/入库。
4. **数据库权限是最终边界**：应用层只读检查不能替代只读账号。
5. **兼容声明按证据分层**：compile、sync、execute、smoke、production 分开描述。
6. **诚实标注算法边界**：DSL 能稳定编译已表达意图，不能替模型发明正确算法。

## 主要组件

| 层 | 模块 | 职责 |
|---|---|---|
| 公共能力 | `forge/evaluate.py`, `forge/enforce.py`, `forge/explain.py` | Evaluate / Enforce / Explain；HTTP 适配在 `web/routes/` |
| 当前编排 | `services/pi-orchestrator/src/application.ts` | Task、Attempt、Artifact、取消/重试与阶段状态 |
| 查询真相 | `forge/query_runs.py`, `forge/executor.py` | 审批 hash、幂等、执行 lease、只读执行与结果 |
| 确定性核心 | `forge/compiler.py`, `forge/lint.py`, `forge/assurance.py` | 契约、规则、Registry/ACL 与编译 |
| 方言选择 | `forge/dialects.py` | 纯 requested/resolved/provenance 解析；调用者显式提供可信绑定 |
| Benchmark | `forge/bird_benchmark.py`, `forge/benchmark_service.py` | 冻结协议、离线 replay、context/评分；不依赖 Web DTO |
| 当前入口 | `main.py`, `web/routes/`, `web/pi_client.py` | Web/Pi 渠道、认证、投影和公共 Runtime API |
| Registry / 治理 | `registry/`, `forge/context.py`, `agent/tenant.py` | 权威结构、知识与 tenant ACL；scope 权限只能取交集 |
| 兼容能力 | `agent/agent.py`, `agent/pipeline.py`, `agent/memory/` | 旧 Query/Define、EMS/WMB、Pipeline；不是当前主编排 |
| 交付 | `forge/quickstart.py`, `forge/readiness.py`, `forge/poc.py` | 合成数据 Golden Path、部署门禁与证据 |

## 当前状态与目标状态

- **Current**：Trust Runtime 公共接口、Pi 主 Task 链、QueryRun 审批/执行和 Product BFF。
- **Legacy**：旧 `/api/chat`、`/api/approve`、`/api/cancel` 默认禁用，仅 `LEGACY_AGENT_API_ENABLED=true` 显式回滚；合法 Memory 管理与 prepare-only 能力仍保留。旧 Memory 存储按首次使用初始化，模块导入不连接旧数据库；logging 在 lifespan 配置，飞书 dispatcher 在收到 webhook 时按配置装配。
- **Experimental / 目标**：企业横向平面与完整跨渠道部署不因已有类或接口就视为交付。长期图中的钉钉等能力按各自运行证据验收。
- **方言契约**：Evaluate 不读取生产 URL。Direct 的省略/auto 仅选择 generic SQL parser，不声称已生成目标方言 SQL；Forge JSON 无可信绑定时返回 `dialect_required`，显式支持方言才编译。Enforce 显式传入受信配置/数据源绑定，记录来源而不记录 URL。
- **分发**：wheel/sdist 声明 DSL Schema 与 Web 模板/静态资源，运行时以 `importlib.resources` 读取；生成 charts、数据库和私有配置不入包。CI 使用独立约束快照并在非源码 cwd 非 editable 安装后实际运行 import/CLI/合成 Quickstart，库依赖仍保留范围。
- **兼容证据**：SQLite/PostgreSQL/MySQL 的既有 smoke 与 BigQuery/Snowflake 的编译支持分开，不等同生产认证。本次发行验证使用本地合成 SQLite，不调用模型或外部渠道。

## 教材入口

- [导读与阅读路线](architecture-course/index.md)
- [核心技术优势](architecture-course/03-core-advantages.md)
- [一次查询的完整生命周期](architecture-course/04-query-lifecycle.md)
- [完整实战课程](architecture-course/12-labs.md)
- [目标架构与路线图](architecture-course/13-roadmap.md)

## 深入参考

- [工作原理与 DSL 能力](how-it-works.md)
- [DSL 形式化语义](dsl-semantics.md)
- [Registry](registry.md)
- [兼容性矩阵](compatibility-matrix.md)
- [生产部署](production-deployment.md)
- [基准测试](benchmarks.md)
