# Forge 文档导航

> 目的：区分当前事实、稳定约束、主动计划和历史材料，避免 Coding Agent 把旧计划恢复成当前任务。

## 进入项目先读

| 文档 | 角色 |
|---|---|
| [`current-project-state.md`](current-project-state.md) | 当前产品、阶段、门禁、未关闭验收项和 OMP 入口；默认第一入口 |
| [`product-north-star.md`](product-north-star.md) | 稳定产品价值、正确性边界与非目标 |
| [`forge-enterprise-evolution-plan.md`](forge-enterprise-evolution-plan.md) | 唯一主动计划；当前为 R0 Open-source Trust Runtime Product Cut / Adoption Baseline |
| [`platform-architecture.md`](platform-architecture.md) | Pi、Forge、Skills、Channel 的稳定职责边界 |
| [`requirements-pool.md`](requirements-pool.md) | 追加式需求与决策历史；当前有效产品需求为 `REQ-2026-09-03-025` |

按任务需要读取，不要在每次会话中扫描全部历史文档。

## 稳定工程说明

| 文档 | 角色 |
|---|---|
| [`architecture.md`](architecture.md) | 当前系统架构的精简入口 |
| [`architecture-course/index.md`](architecture-course/index.md) | 从可信问数原理到生产架构的系统教材 |
| [`how-it-works.md`](how-it-works.md) | DSL、Registry、Assurance 和执行流程 |
| [`product-projection-contracts.md`](product-projection-contracts.md) | Product Projection v1 只读 Contract |
| [`production-deployment.md`](production-deployment.md) | 生产部署边界与操作说明 |
| [`agent-integration.md`](agent-integration.md) | 外部 Agent 的受控集成边界 |

## 战略参考，不是自动实施授权

- [`product-design-roadmap-2026-08-25.md`](product-design-roadmap-2026-08-25.md)：长期产品面、对象与方向地图。
- [`ai-native-enterprise-thesis.md`](ai-native-enterprise-thesis.md)：企业 AI Native 假设与待验证论证。
- [`product-axioms.md`](product-axioms.md)：产品和工程决策公理。
- [`commercial-readiness.md`](commercial-readiness.md)、[`commercialization-plan.md`](commercialization-plan.md)：商业化参考，不覆盖当前 R0 开源采用门禁。

## 历史计划与证据

以下文档保留用于溯源，不是当前待办：

- `pi-forge-integration-plan.md`
- `short-term-product-spine-plan-2026-08-25.md`
- `product-spine-sp*-evidence-2026-08-25.md`
- `product-direction-architecture-review-2026-08-24.md`
- `governance-contract-review-2026-08-24.md`
- `delivery-assessment-*.md`
- `devlog/`

文件名中的日期、完成项或旧路线不能替代文档顶部状态和 [`current-project-state.md`](current-project-state.md)。

## 维护规则

- 新产品、体验、架构或业务需求先追加到 `requirements-pool.md`；不覆盖原始表达。
- 用户接受后才进入主动计划；只有影响稳定职责边界时才修改 `platform-architecture.md`。
- 实施进度、验证结果和风险写回主动计划及当前状态页；不要堆进 `AGENTS.md`。
- 历史文档不删除；被替代时明确标记历史/已吸收/未批准。
- 测试数量、候选地址和临时工作区状态属于易变证据，不作为长期导航文案。
