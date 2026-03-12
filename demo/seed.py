"""
Demo data seeder — creates demo/forge_demo.db with realistic sample data.

Schema:
  users, orders, order_items, products

Covers all test cases in tests/text-to-sql-failures/:
  A1 — users with zero orders (LEFT JOIN demo)
  A2 — orders with no items (ANTI JOIN demo)
  B1 — VIP filter across cities
  D1 — repurchase rate ambiguity
  E2 — category revenue share
"""

import sqlite3
import random
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "forge_demo.db"

CITIES   = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安"]
STATUSES = ["completed", "pending", "cancelled"]

PRODUCTS = [
    (1, "iPhone 15",    "电子产品", 5200.00),
    (2, "MacBook Air",  "电子产品", 8800.00),
    (3, "AirPods Pro",  "电子产品",  980.00),
    (4, "优衣库 T恤",   "服装",       99.00),
    (5, "李宁运动鞋",   "服装",      499.00),
    (6, "无印良品外套", "服装",      599.00),
    (7, "有机坚果礼盒", "食品",      188.00),
    (8, "茅台飞天",     "食品",     1499.00),
    (9, "宜家书架",     "家居",      349.00),
    (10,"戴森吸尘器",   "家居",     3299.00),
]

USERS = [
    # (id, name, city, is_vip)
    (1,  "张伟",  "北京", 1),
    (2,  "李娜",  "上海", 1),
    (3,  "王芳",  "广州", 0),
    (4,  "刘强",  "深圳", 1),
    (5,  "陈明",  "杭州", 0),
    (6,  "赵雪",  "成都", 1),
    (7,  "孙磊",  "武汉", 0),
    (8,  "周婷",  "西安", 0),
    (9,  "吴浩",  "北京", 1),
    (10, "郑丽",  "上海", 0),
    (11, "冯俊",  "广州", 1),
    (12, "蒋欣",  "深圳", 0),
    # 以下用户没有订单 —— A1 LEFT JOIN 演示
    (13, "秦悦",  "北京", 0),
    (14, "韩冰",  "上海", 1),
    (15, "许晨",  "杭州", 0),
]


def random_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    days = random.randint(end_days_ago, start_days_ago)
    dt = datetime.now() - timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def seed():
    DB_PATH.unlink(missing_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ── schema ─────────────────────────────────────────────────────────────────
    cur.executescript("""
    CREATE TABLE users (
        id         INTEGER PRIMARY KEY,
        name       TEXT NOT NULL,
        city       TEXT NOT NULL,
        created_at TEXT NOT NULL,
        is_vip     INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE orders (
        id           INTEGER PRIMARY KEY,
        user_id      INTEGER NOT NULL,
        status       TEXT NOT NULL,
        total_amount REAL NOT NULL,
        created_at   TEXT NOT NULL,
        FOREIGN KEY (user_id) REFERENCES users(id)
    );

    CREATE TABLE order_items (
        id         INTEGER PRIMARY KEY,
        order_id   INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity   INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        FOREIGN KEY (order_id)   REFERENCES orders(id),
        FOREIGN KEY (product_id) REFERENCES products(id)
    );

    CREATE TABLE products (
        id          INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        category    TEXT NOT NULL,
        cost_price  REAL NOT NULL
    );
    """)

    # ── users ──────────────────────────────────────────────────────────────────
    cur.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?)",
        [(uid, name, city, random_date(365, 180), is_vip)
         for uid, name, city, is_vip in USERS],
    )

    # ── products ───────────────────────────────────────────────────────────────
    cur.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", PRODUCTS)

    # ── orders + items ─────────────────────────────────────────────────────────
    order_id = 1
    item_id  = 1
    orders, items = [], []

    # users 1–12 get orders; 13–15 get none (A1 demo)
    for uid in range(1, 13):
        # each user gets 1–6 orders
        n_orders = random.randint(1, 6)
        # user 4 & 6 are heavy buyers (D1 repurchase demo)
        if uid in (4, 6):
            n_orders = random.randint(4, 8)

        for _ in range(n_orders):
            status     = random.choices(STATUSES, weights=[6, 2, 2])[0]
            created_at = random_date(180)
            total      = round(random.uniform(100, 5000), 2)

            orders.append((order_id, uid, status, total, created_at))

            # order_id 5 has no items — A2 ANTI JOIN demo
            if order_id != 5:
                n_items = random.randint(1, 3)
                chosen  = random.sample(PRODUCTS, min(n_items, len(PRODUCTS)))
                for pid, _, _, cost in chosen:
                    qty   = random.randint(1, 4)
                    price = round(cost * random.uniform(1.1, 2.0), 2)
                    items.append((item_id, order_id, pid, qty, price))
                    item_id += 1

            order_id += 1

    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders)
    cur.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?)", items)

    conn.commit()
    conn.close()

    print(f"✅ Demo database created: {DB_PATH}")
    print(f"   users:       {len(USERS)} rows ({len(USERS)-3} with orders, 3 without)")
    print(f"   orders:      {len(orders)} rows (order #5 has no items)")
    print(f"   order_items: {len(items)} rows")
    print(f"   products:    {len(PRODUCTS)} rows")
    print()
    print("DATABASE_URL for .env:")
    print(f"  DATABASE_URL=sqlite:///{DB_PATH.resolve()}")


if __name__ == "__main__":
    seed()
