# Web 主体内容文案审计（2026-08-24）

## 结论

按 `REQ-2026-08-24-011` 审计 `web/templates/` 的 19 个最终用户/管理员模板、Web 暴露的 Architecture Atlas，以及 H5 ECharts focused candidate。

本轮确认并移除 5 类非主体内容：

| 页面 | 移除内容 | 保留的主体内容 |
|---|---|---|
| `/chat` | 英文 slogan、“从一个业务问题开始，让证据一路走到报告”、`可信运行时` 自我评价、英文氛围标签 | 新建数据任务、SQL 审批边界、查询/口径/偏好入口、当前任务与执行计划 |
| `/tasks` | `INTEGRATION SPIKE`、Pi→Forge 架构宣传、口号式标题 | 创建任务、跨渠道状态、SQL 审批一致性和 Runtime 状态 |
| Registry Studio | `CANONICAL SCHEMA CONTROL PLANE` eyebrow/hero 表达 | Canonical Schema 投影、Binding revision、Draft/发布风险 |
| 全局导航 | `AI SQL Agent` 产品描述 | Forge 产品名、功能导航和 AI 管理助手操作 |
| `/login` | `AI SQL Agent`、`私有化部署` 展示 | Forge、登录、错误和版本 |
| `/admin/architecture` | “不是单一 SQL 生成器”、 “使用越多，组织能力越强”等产品主张 | 用户角色、功能、平台能力、持久化资产和技术边界 |
| H5 focused candidate | `可信数据报告`、英文氛围标签、Renderer/版本/候选说明 | 报告标题、范围、质量、执行摘要、决策问题、图表、Evidence、限制和操作 |

其余 14 个模板未发现宣传 slogan 或营销 Hero。版本化生成的历史 Chart HTML 属不可变报告产物，本轮不原地改写；新 Renderer 必须遵守该规则。Dashboard、Metrics、Knowledge、Memory、Schema、Semantic、Settings、Audit、Import、Staging、Team 等页面均以资源、状态、表单、风险说明和操作为主体，因此没有为“去营销化”而删除必要帮助。

## 判定边界

以下文本保留：

- `Model Control Plane`、Binding、Revision、候选模型等管理员技术状态；
- “可信关系”这类 Registry 数据状态，而不是产品自我评价；
- SQL 不可编辑、Draft 不执行数据库 DDL 等直接降低操作风险的说明；
- 由当前数据支持的业务判断和建议。

它们描述当前对象、状态或风险，不属于宣传文案。

## 回归

`tests/test_web_product_content.py` 固定本轮明确拒绝的短语，并验证 Chat、Tasks、Registry 首屏改为功能性内容。测试不使用“增长”“价值”“可信”等宽泛关键词，避免误杀业务报告与治理术语。
