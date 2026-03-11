# Forge

An Agent-Native DSL that compiles deterministically to SQL.

## 核心定位

> 让 AI 操作「意图」而不是「执行细节」

Forge 是一个面向 AI Agent 的查询语言，作为自然语言与 SQL 之间的防抖层：

```
自然语言
    ↓  LLM 生成
  Forge DSL        ← AI 操作这一层
    ↓  Schema 校验 + 语义校验
    ↓  确定性编译
  SQL（目标方言）
    ↓  执行引擎
   查询结果
```

## 解决的问题

| Text-to-SQL 的痛点 | Forge 的应对 |
|---|---|
| 字段名/表名幻觉 | Schema 注册，只允许引用已知实体 |
| JOIN 类型推断错误 | 强制显式声明，无默认值 |
| 方言语法差异 | 编译器统一处理，DSL 无方言 |
| 错误在执行时才发现 | 解析阶段即可校验 |
| 业务语义歧义 | 预定义指标层，名称唯一映射 |

## 项目结构

```
forge/          # DSL 核心（语法、编译器）
tests/          # 测试用例
  text-to-sql-failures/   # AI 生成 SQL 的典型失败案例（设计靶心）
docs/           # 设计文档
```

## 状态

🌱 早期设计阶段
