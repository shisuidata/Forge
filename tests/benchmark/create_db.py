#!/usr/bin/env python3
"""
创建 benchmark 数据库和 gold CSV 文件。

运行方式：
    python tests/benchmark/create_db.py

输出：
    tests/benchmark/benchmark.db          — SQLite 数据库（20用户/10商品/50订单/51明细）
    tests/benchmark/gold/C01.csv … C40.csv — 每个 case 的正确答案
"""
from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent.parent
BENCH_DIR = Path(__file__).parent
DB_PATH   = BENCH_DIR / "benchmark.db"
GOLD_DIR  = BENCH_DIR / "gold"
CASES_PATH = BENCH_DIR / "cases.json"


# ── DDL ──────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    city       TEXT    NOT NULL,
    is_vip     INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    id         INTEGER PRIMARY KEY,
    name       TEXT    NOT NULL,
    category   TEXT    NOT NULL,
    cost_price REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id           INTEGER PRIMARY KEY,
    user_id      INTEGER NOT NULL,
    status       TEXT    NOT NULL,
    total_amount REAL    NOT NULL,
    created_at   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS order_items (
    id         INTEGER PRIMARY KEY,
    order_id   INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity   INTEGER NOT NULL,
    unit_price REAL    NOT NULL
);
"""

# ── 数据 ──────────────────────────────────────────────────────────────────────
#
# 设计说明：
#   - 20 位用户：北京(1-5), 上海(6-10), 广州(11-14), 成都(15-17), 杭州(18-20)
#   - VIP 用户：1,2,3,6,7,11,15,18
#   - 10 种商品：电子产品(1-3), 服装(4-6), 家居(7-8), 食品(9-10)
#   - 50 笔订单：已取消 ids = 3,8,12,22,23,29,35,46
#   - 用户 19,20 无订单（ANTI JOIN 用例）
#   - order_items 与 orders.total_amount 金额完全对应

USERS = [
    # id, name, city, is_vip, created_at
    (1,  "张三", "北京", 1, "2022-03-15"),
    (2,  "李四", "北京", 1, "2022-05-20"),
    (3,  "王五", "北京", 1, "2022-07-10"),
    (4,  "赵六", "北京", 0, "2022-09-01"),
    (5,  "钱七", "北京", 0, "2022-11-15"),
    (6,  "孙八", "上海", 1, "2022-01-20"),
    (7,  "周九", "上海", 1, "2022-04-05"),
    (8,  "吴十", "上海", 0, "2022-06-30"),
    (9,  "郑一", "上海", 0, "2022-08-15"),
    (10, "陈二", "上海", 0, "2022-10-22"),
    (11, "冯三", "广州", 1, "2023-02-15"),
    (12, "褚四", "广州", 0, "2023-04-10"),
    (13, "卫五", "广州", 0, "2023-06-05"),
    (14, "蒋六", "广州", 0, "2023-08-20"),
    (15, "沈七", "成都", 1, "2023-10-01"),
    (16, "韩八", "成都", 0, "2023-10-25"),
    (17, "杨九", "成都", 0, "2023-11-15"),
    (18, "朱十", "杭州", 1, "2023-12-01"),
    (19, "秦一", "杭州", 0, "2022-12-20"),
    (20, "许二", "杭州", 0, "2023-01-10"),
]

PRODUCTS = [
    # id, name, category, cost_price
    (1,  "智能手机",   "电子产品", 800),
    (2,  "笔记本电脑", "电子产品", 1200),
    (3,  "平板电脑",   "电子产品", 500),
    (4,  "羽绒服",     "服装",     200),
    (5,  "连衣裙",     "服装",     80),
    (6,  "运动鞋",     "服装",     100),
    (7,  "沙发",       "家居",     600),
    (8,  "台灯",       "家居",     60),
    (9,  "咖啡豆",     "食品",     30),
    (10, "有机绿茶",   "食品",     20),
]

# 已取消 ids: 3,8,12,22,23,29,35,46
ORDERS = [
    # id, user_id, status, total_amount, created_at
    (1,  1,  "completed", 1500, "2023-01-15"),
    (2,  1,  "completed", 1200, "2023-07-01"),
    (3,  1,  "cancelled",  800, "2023-02-20"),
    (4,  1,  "completed",  900, "2024-01-08"),
    (5,  2,  "completed", 1200, "2023-02-10"),
    (6,  2,  "completed",  900, "2023-07-15"),
    (7,  2,  "completed",  800, "2024-01-15"),
    (8,  2,  "cancelled",  700, "2023-03-15"),
    (9,  3,  "completed", 2000, "2023-03-05"),
    (10, 3,  "completed", 1500, "2023-08-05"),
    (11, 3,  "completed", 1050, "2024-02-10"),
    (12, 3,  "cancelled",  600, "2023-04-28"),
    (13, 4,  "completed",  800, "2023-03-20"),
    (14, 4,  "completed",  900, "2023-08-18"),
    (15, 4,  "completed",  700, "2023-05-12"),
    (16, 5,  "completed", 1000, "2023-04-15"),
    (17, 5,  "completed",  900, "2023-09-12"),
    (18, 5,  "completed",  800, "2024-01-22"),
    (19, 6,  "completed", 2000, "2023-04-22"),
    (20, 6,  "completed", 1800, "2023-09-28"),
    (21, 6,  "completed", 1650, "2024-02-05"),
    (22, 6,  "cancelled",  900, "2023-07-20"),
    (23, 7,  "cancelled",  600, "2023-10-05"),
    (24, 7,  "completed", 1500, "2023-05-10"),
    (25, 7,  "completed", 1200, "2024-02-18"),
    (26, 8,  "completed",  600, "2023-05-28"),
    (27, 8,  "completed",  800, "2023-10-18"),
    (28, 9,  "completed",  900, "2023-06-08"),
    (29, 9,  "cancelled",  500, "2023-11-02"),
    (30, 9,  "completed",  700, "2024-01-28"),
    (31, 10, "completed",  700, "2023-06-20"),
    (32, 10, "completed",  900, "2023-11-15"),
    (33, 11, "completed",  500, "2023-06-25"),
    (34, 11, "completed",  800, "2023-11-20"),
    (35, 12, "cancelled",  600, "2023-11-28"),
    (36, 12, "completed", 1400, "2023-12-05"),
    (37, 13, "completed",  700, "2023-12-10"),
    (38, 13, "completed",  800, "2024-01-12"),
    (39, 14, "completed",  600, "2023-12-15"),
    (40, 14, "completed",  800, "2023-07-05"),
    (41, 15, "completed",  800, "2023-12-18"),
    (42, 15, "completed",  900, "2024-03-05"),
    (43, 16, "completed",  500, "2023-12-22"),
    (44, 16, "completed",  600, "2024-02-08"),
    (45, 18, "completed",  600, "2023-12-28"),
    (46, 17, "cancelled",  500, "2023-11-10"),
    (47, 18, "completed",  500, "2023-07-15"),
    (48, 13, "completed",  600, "2024-02-25"),
    (49, 14, "completed",  500, "2023-09-22"),
    (50, 15, "completed",  700, "2024-03-15"),
]

# 每行 sum(quantity * unit_price) 必须等于对应 order 的 total_amount
ORDER_ITEMS = [
    # id, order_id, product_id, quantity, unit_price
    (1,  1,  1, 1, 1500),   # o1=1500: 智能手机×1@1500
    (2,  2,  3, 2,  600),   # o2=1200: 平板电脑×2@600
    (3,  3,  4, 2,  400),   # o3=800(cancelled): 羽绒服×2@400
    (4,  4,  1, 1,  900),   # o4=900: 智能手机×1@900
    (5,  5,  2, 1, 1200),   # o5=1200: 笔记本电脑×1@1200
    (6,  6,  3, 1,  900),   # o6=900: 平板电脑×1@900
    (7,  7,  1, 1,  800),   # o7=800: 智能手机×1@800
    (8,  8,  5, 5,  140),   # o8=700(cancelled): 连衣裙×5@140
    (9,  9,  2, 1, 2000),   # o9=2000: 笔记本电脑×1@2000
    (10, 10, 1, 1, 1500),   # o10=1500: 智能手机×1@1500
    (11, 11, 3, 1, 1000),   # o11=1050: 平板电脑×1@1000
    (12, 11, 9, 1,   50),   #           咖啡豆×1@50
    (13, 12, 4, 1,  600),   # o12=600(cancelled): 羽绒服×1@600
    (14, 13, 7, 1,  800),   # o13=800: 沙发×1@800
    (15, 14, 7, 1,  900),   # o14=900: 沙发×1@900
    (16, 15, 5, 5,  140),   # o15=700: 连衣裙×5@140
    (17, 16, 3, 2,  500),   # o16=1000: 平板电脑×2@500
    (18, 17, 4, 2,  450),   # o17=900: 羽绒服×2@450
    (19, 18, 6, 4,  200),   # o18=800: 运动鞋×4@200
    (20, 19, 2, 1, 2000),   # o19=2000: 笔记本电脑×1@2000
    (21, 20, 1, 2,  900),   # o20=1800: 智能手机×2@900
    (22, 21, 2, 1, 1650),   # o21=1650: 笔记本电脑×1@1650
    (23, 22, 1, 1,  900),   # o22=900(cancelled): 智能手机×1@900
    (24, 23, 4, 2,  300),   # o23=600(cancelled): 羽绒服×2@300
    (25, 24, 1, 1, 1500),   # o24=1500: 智能手机×1@1500
    (26, 25, 2, 1, 1200),   # o25=1200: 笔记本电脑×1@1200
    (27, 26, 5, 4,  150),   # o26=600: 连衣裙×4@150
    (28, 27, 3, 1,  800),   # o27=800: 平板电脑×1@800
    (29, 28, 1, 1,  900),   # o28=900: 智能手机×1@900
    (30, 29, 5, 5,  100),   # o29=500(cancelled): 连衣裙×5@100
    (31, 30, 3, 1,  700),   # o30=700: 平板电脑×1@700
    (32, 31, 7, 1,  700),   # o31=700: 沙发×1@700
    (33, 32, 7, 1,  900),   # o32=900: 沙发×1@900
    (34, 33, 8, 5,  100),   # o33=500: 台灯×5@100
    (35, 34, 3, 1,  800),   # o34=800: 平板电脑×1@800
    (36, 35, 4, 2,  300),   # o35=600(cancelled): 羽绒服×2@300
    (37, 36, 2, 1, 1400),   # o36=1400: 笔记本电脑×1@1400
    (38, 37, 6, 4,  175),   # o37=700: 运动鞋×4@175
    (39, 38, 1, 1,  800),   # o38=800: 智能手机×1@800
    (40, 39, 5, 6,  100),   # o39=600: 连衣裙×6@100
    (41, 40, 4, 4,  200),   # o40=800: 羽绒服×4@200
    (42, 41, 3, 1,  800),   # o41=800: 平板电脑×1@800
    (43, 42, 7, 1,  900),   # o42=900: 沙发×1@900
    (44, 43, 9, 5,  100),   # o43=500: 咖啡豆×5@100
    (45, 44, 10, 6, 100),   # o44=600: 有机绿茶×6@100
    (46, 45, 8, 6,  100),   # o45=600: 台灯×6@100
    (47, 46, 9, 5,  100),   # o46=500(cancelled): 咖啡豆×5@100
    (48, 47, 6, 2,  250),   # o47=500: 运动鞋×2@250
    (49, 48, 5, 4,  150),   # o48=600: 连衣裙×4@150
    (50, 49, 4, 2,  250),   # o49=500: 羽绒服×2@250
    (51, 50, 3, 1,  700),   # o50=700: 平板电脑×1@700
]


def create_db(db_path: Path) -> sqlite3.Connection:
    db_path.unlink(missing_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.executescript(DDL)

    conn.executemany(
        "INSERT INTO users VALUES (?,?,?,?,?)", USERS)
    conn.executemany(
        "INSERT INTO products VALUES (?,?,?,?)", PRODUCTS)
    conn.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)", ORDERS)
    conn.executemany(
        "INSERT INTO order_items VALUES (?,?,?,?,?)", ORDER_ITEMS)
    conn.commit()

    # 验证 order_items 金额
    errs = []
    for oid, user_id, status, expected, _ in ORDERS:
        actual = conn.execute(
            "SELECT SUM(quantity*unit_price) FROM order_items WHERE order_id=?", (oid,)
        ).fetchone()[0] or 0
        if abs(actual - expected) > 0.01:
            errs.append(f"  ❌ order {oid}: expected {expected}, got {actual}")
    if errs:
        print("order_items 金额校验失败：")
        for e in errs:
            print(e)
        sys.exit(1)

    print(f"✅ 数据库创建成功：{db_path}")
    print(f"   用户: {len(USERS)}  商品: {len(PRODUCTS)}  订单: {len(ORDERS)}  明细: {len(ORDER_ITEMS)}")
    return conn


def generate_gold_csvs(conn: sqlite3.Connection, cases: list[dict]) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    ok, fail = 0, 0
    for case in cases:
        cid  = case["id"]
        sql  = case["reference_sql"]
        path = GOLD_DIR / f"{cid}.csv"
        try:
            cur  = conn.execute(sql)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            with path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            ok += 1
        except Exception as e:
            print(f"  ❌ {cid}: {e}  SQL: {sql[:80]}")
            fail += 1

    print(f"✅ Gold CSV 生成：{ok} 成功  {fail} 失败  →  {GOLD_DIR}")


def main() -> None:
    cases = json.loads(CASES_PATH.read_text(encoding="utf-8"))
    conn  = create_db(DB_PATH)
    generate_gold_csvs(conn, cases)
    conn.close()


if __name__ == "__main__":
    main()
