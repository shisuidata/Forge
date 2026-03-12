"""
Method B_sem — 直接生成 SQL + 语义消歧库

对照组加强版：在 Method B 的基础上，通过语义消歧库对问题进行预处理，
追加针对已知歧义点的括号说明，再让模型直接生成 SQLite SQL。

与 Method B 的唯一区别：use_semantic_lib = True
"""

METHOD_ID = "b_sem"
LABEL = "Method B+Sem（直接生成 SQL + 语义消歧库）"
MODE = "sql"
USE_SEMANTIC_LIB = True
NOTES = "2026-03-12 对照组 + 语义消歧库预处理，测量语义库对直接SQL生成的提升效果"

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
- users.city: '北京' | '上海' | '广州' | '成都' | '杭州' | '武汉' | '深圳' | '西安'
"""

SYSTEM_PROMPT = f"""你是一个专业的数据查询助手，擅长编写 SQLite SQL 查询。

{_SCHEMA}

用户会描述一个数据查询需求，你需要输出可以在 SQLite 上执行的正确 SQL。
只输出 SQL 语句，不要任何解释，不要 markdown 代码块。"""
