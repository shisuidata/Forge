# Forge Website

Forge 官网使用 Astro + Starlight，承载项目介绍、快速开始、核心概念、基准测试和私有化 PoC 说明。

## Development

```bash
npm install
npm run dev
```

## Build

```bash
npm run build
```

## Content

- `src/content/docs/index.mdx`: 首页。
- `src/content/docs/guides/`: 安装、快速开始和 PoC 入门。
- `src/content/docs/concepts/`: 工作原理、Registry、设计哲学。
- `src/content/docs/reference/`: benchmark、DSL 和架构说明。

首页指标应与仓库根目录 `README.md` 和 `docs/test-report-2026-07-13.md` 保持一致。
