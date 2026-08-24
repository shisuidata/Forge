// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
	integrations: [
		starlight({
			title: 'Forge',
			logo: {
				src: './src/assets/logo.svg',
				alt: 'Forge Logo',
			},
			favicon: '/favicon.svg',
			description: '面向数据团队的 AI 查询 Agent，让弱模型也能生成可信 SQL。',
			defaultLocale: 'root',
			locales: { root: { label: '简体中文', lang: 'zh-CN' } },
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/shisuidata/Forge' }],
			customCss: ['./src/styles/custom.css'],
			head: [
				{
					tag: 'link',
					attrs: { rel: 'apple-touch-icon', sizes: '180x180', href: '/apple-touch-icon.png' },
				},
				{
					tag: 'link',
					attrs: { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/favicon-32x32.png' },
				},
				{
					tag: 'link',
					attrs: { rel: 'icon', type: 'image/png', sizes: '16x16', href: '/favicon-16x16.png' },
				},
				{
					tag: 'link',
					attrs: {
						rel: 'preconnect',
						href: 'https://fonts.googleapis.com',
					},
				},
				{
					tag: 'link',
					attrs: {
						rel: 'preconnect',
						href: 'https://fonts.gstatic.com',
						crossorigin: '',
					},
				},
				{
					tag: 'link',
					attrs: {
						rel: 'stylesheet',
						href: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Noto+Sans+SC:wght@300;400;500;600;700&display=swap',
					},
				},
			],
			sidebar: [
				{
					label: '架构课程',
					items: [
						{ label: '课程导读', slug: 'course' },
						{ label: '01 · 为什么问数不可信', slug: 'course/01-why-trust' },
						{ label: '02 · 总体架构', slug: 'course/02-overview' },
						{ label: '03 · 核心技术优势', slug: 'course/03-core-advantages' },
						{ label: '04 · 查询生命周期', slug: 'course/04-query-lifecycle' },
						{ label: '05 · DSL 与编译器', slug: 'course/05-dsl-compiler' },
						{ label: '06 · Registry', slug: 'course/06-registry' },
						{ label: '07 · Schema 检索', slug: 'course/07-retrieval-context' },
						{ label: '08 · Agent 与 Pipeline', slug: 'course/08-agent-memory-pipeline' },
						{ label: '09 · 安全与权限', slug: 'course/09-security' },
						{ label: '10 · 部署与运维', slug: 'course/10-deployment' },
						{ label: '11 · 基准与证据', slug: 'course/11-evidence' },
						{ label: '12 · 完整实战', slug: 'course/12-labs' },
						{ label: '13 · 演进路线', slug: 'course/13-roadmap' },
						{ label: '附录', slug: 'course/appendix' },
					],
				},
				{
					label: '快速开始',
					items: [
						{ label: '介绍', slug: 'guides/introduction' },
						{ label: '安装部署', slug: 'guides/installation' },
						{ label: '五分钟上手', slug: 'guides/quickstart' },
					],
				},
				{
					label: '核心概念',
					items: [
						{ label: '工作原理', slug: 'concepts/how-it-works' },
						{ label: '设计哲学', slug: 'concepts/philosophy' },
						{ label: 'Registry 语义层', slug: 'concepts/registry' },
					],
				},
				{
					label: '参考',
					items: [
						{ label: '基准测试', slug: 'reference/benchmarks' },
						{ label: 'DSL 语义', slug: 'reference/dsl-semantics' },
						{ label: '架构设计', slug: 'reference/architecture' },
					],
				},
				{
					label: '开发日志',
					items: [
						{ label: 'Day 0 · 为什么做这件事', slug: 'devlog/day0' },
						{ label: 'Day 6 · 从原型到产品', slug: 'devlog/day6' },
					],
				},
			],
		}),
	],
});
