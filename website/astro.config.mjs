// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

function rehypeKeyboardTables() {
	/** @param {import('hast').Root | import('hast').RootContent} node */
	function visit(node) {
		if (node.type === 'element' && node.tagName === 'table') node.properties.tabIndex = 0;
		if ('children' in node) node.children.forEach(visit);
	}
	return visit;
}

export default defineConfig({
	site: 'https://forge.ss-data.cc',
	markdown: { rehypePlugins: [rehypeKeyboardTables] },
	integrations: [
		starlight({
			title: 'Forge',
			logo: { src: './src/assets/forge-mark.svg', alt: 'Forge' },
			favicon: '/brand/forge-mark.svg',
			head: [
				{ tag: 'link', attrs: { rel: 'icon', href: '/favicon.ico', sizes: 'any' } },
				{ tag: 'link', attrs: { rel: 'apple-touch-icon', href: '/apple-touch-icon.png' } },
				{ tag: 'meta', attrs: { name: 'theme-color', content: '#0f1114' } },
			],
			description: 'Forge 是开源的 Data Agent 可信数据运行时：评估候选 SQL，按授权放行、等待审核或拒绝，并为每次执行留下可校验的证据。',
			defaultLocale: 'root',
			locales: { root: { label: '简体中文', lang: 'zh-CN' } },
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/shisuidata/Forge' }],
			customCss: ['./src/styles/custom.css'],

			sidebar: [
				{
					label: '快速开始',
					items: [
						{ label: '介绍 · Trust Runtime', slug: 'guides/introduction' },
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
					label: '参考与证据',
					items: [
						{ label: '基准测试与验证边界', slug: 'reference/benchmarks' },
						{ label: 'DSL 语义', slug: 'reference/dsl-semantics' },
						{ label: '架构设计', slug: 'reference/architecture' },
					],
				},
				{
					label: '历史架构课程',
					collapsed: true,
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
					label: '历史开发日志',
					collapsed: true,
					items: [
						{ label: 'Day 0 · 为什么做这件事', slug: 'devlog/day0' },
						{ label: 'Day 6 · 从原型到产品', slug: 'devlog/day6' },
					],
				},
			],
		}),
	],
});
