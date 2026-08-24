# Forge Web Product Shell Prototype

W3A 的隔离桌面交互骨架，用于在大范围修改生产 Jinja 前验证信息架构、页面关系、关键状态和核心人工旅程。

## 页面

- `#/workspace`：工作台
- `#/new`：新建任务
- `#/tasks`：任务列表
- `#/tasks/tr_sales_channel?tab=sql`：任务详情与 SQL 审批
- `#/reports`：报告库
- `#/reports/rp_category_h1`：报告详情
- `#/data`：数据资产
- `#/admin`：管理入口

## 边界

- 只使用版本化 fixture，所有页面固定显示“交互原型 / 演示数据”。
- 不请求生产 API，不连接数据库，不写 Task、Approval、Audit、Report 或 Registry Store。
- 原型中的审批、重试、同步和配置操作只切换浏览器内状态或显示说明。
- 本地固定依赖，无 CDN；桌面 1440×900 与 1600×1000 是当前门禁。
- 用户通过 W3A 之前不修改生产 Web Product Shell。

## 运行与验证

```bash
cd tools/web-product-shell-prototype
npm ci
npm test
npm run build
npm run preview
```

浏览器打开 `http://127.0.0.1:4176/`。

从仓库根目录运行 Playwright：

```bash
.venv/bin/python tools/web-product-shell-prototype/scripts/verify.py \
  --url http://127.0.0.1:4176/ \
  --output-dir /tmp/forge-web-shell
```
