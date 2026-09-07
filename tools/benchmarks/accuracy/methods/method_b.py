"""Method B — 直接生成 SQL（对照组）"""

METHOD_ID = "b"
LABEL = "Method SQL（直接生成 SQL）"
MODE = "sql"
NOTES = "对照组：模型直接输出 SQLite SQL，不经过 Forge DSL 编译"

_SCHEMA = """
你可以查询以下数据库表（SQLite）：

users       (id, name, city, created_at, is_vip)
orders      (id, user_id, status, total_amount, created_at)
order_items (id, order_id, product_id, quantity, unit_price)
products    (id, name, category, cost_price)

典型值：
- orders.status: 'completed' | 'cancelled' | 'pending'
- users.is_vip: 1 / 0
- products.category: 'electronics' | 'clothing' | 'food' | 'books'
"""

SYSTEM_PROMPT = f"""你是一个专业的数据查询助手，擅长编写 SQLite SQL 查询。

{_SCHEMA}

用户会描述一个数据查询需求，你需要输出可以在 SQLite 上执行的正确 SQL。
只输出 SQL 语句，不要任何解释，不要 markdown 代码块。"""
