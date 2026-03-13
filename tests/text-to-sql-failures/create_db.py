"""创建本地测试题的 SQLite 测试数据库。"""
import sqlite3
from pathlib import Path

db_path = Path(__file__).parent / "test.db"

conn = sqlite3.connect(str(db_path))
conn.executescript("""
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS products;

CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT,
  city TEXT,
  created_at DATE,
  is_vip INTEGER
);

CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  name TEXT,
  category TEXT,
  cost_price REAL
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  status TEXT,
  total_amount REAL,
  created_at DATE
);

CREATE TABLE order_items (
  id INTEGER PRIMARY KEY,
  order_id INTEGER,
  product_id INTEGER,
  quantity INTEGER,
  unit_price REAL
);

INSERT INTO users VALUES
  (1,'Alice','Beijing','2024-01-10',1),
  (2,'Bob','Shanghai','2024-02-15',0),
  (3,'Charlie','Beijing','2024-03-01',1),
  (4,'Diana','Guangzhou','2024-03-20',0),
  (5,'Eve','Shanghai','2024-04-05',1);

INSERT INTO products VALUES
  (1,'Widget A','Electronics',50.0),
  (2,'Widget B','Electronics',80.0),
  (3,'Gadget X','Clothing',20.0),
  (4,'Gadget Y','Clothing',30.0),
  (5,'Book Z','Books',15.0);

INSERT INTO orders VALUES
  (1,1,'completed',200.0,'2025-01-05'),
  (2,1,'completed',350.0,'2025-02-10'),
  (3,2,'completed',150.0,'2025-01-20'),
  (4,3,'completed',500.0,'2025-01-15'),
  (5,3,'completed',100.0,'2025-02-28'),
  (6,3,'completed',250.0,'2025-03-10'),
  (7,1,'cancelled',180.0,'2025-03-01'),
  (8,2,'completed',90.0,'2025-03-15'),
  (9,4,'completed',320.0,'2025-03-20');

-- 订单 8 故意没有明细，用于 A2 反连接测试
INSERT INTO order_items VALUES
  (1, 1,1,2,100.0),
  (2, 1,3,1, 50.0),
  (3, 2,2,1,200.0),
  (4, 2,1,1,150.0),
  (5, 3,3,3, 50.0),
  (6, 4,1,3,100.0),
  (7, 4,2,1,200.0),
  (8, 5,5,2, 50.0),
  (9, 6,4,5, 50.0),
  (10,7,3,2, 90.0),
  (11,9,2,2, 80.0),
  (12,9,4,2, 80.0);
""")
conn.commit()

for table in ("users", "products", "orders", "order_items"):
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"  {table}: {count} rows")

conn.close()
print(f"\n✅ 测试数据库已创建: {db_path}")
