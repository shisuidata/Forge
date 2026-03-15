#!/usr/bin/env python3
"""Seed large_demo.db with 200-table e-commerce schema mock data."""
import json
import random
import shutil
import sqlite3
import string
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
ROOT = Path(__file__).parent.parent
REGISTRY_PATH = ROOT / "tests/fixtures/large_registry.json"
DB_PATH = Path(__file__).parent / "large_demo.db"

# ─── Reference data ──────────────────────────────────────────────────────────
NAMES = ["张伟", "王芳", "李娜", "赵磊", "陈静", "刘洋", "杨帆", "黄晓",
         "周雷", "吴婷", "徐明", "孙晨", "马力", "胡建", "林雪", "朱峰",
         "何军", "高燕", "郑超", "唐梅"]
CITIES = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安",
          "南京", "苏州", "重庆", "天津", "宁波", "郑州", "长沙"]
PRODUCTS = ["iPhone 15 Pro", "MacBook Air M2", "AirPods Pro 2", "华为 Mate 60",
            "小米 14", "耐克 Air Max", "阿迪达斯 NMD", "李宁 弜",
            "优衣库 T恤", "无印良品 收纳盒", "戴森 V12", "美的 电饭煲",
            "茅台 飞天", "五粮液 普五", "乐高 43219", "索尼 WH-1000XM5"]
CATEGORIES = ["电子产品", "服装鞋帽", "食品饮料", "家居家电", "美妆护肤",
              "运动户外", "图书文具", "玩具礼品", "汽车配件", "母婴用品"]
KEYWORDS = ["手机", "笔记本", "耳机", "运动鞋", "连衣裙", "护肤品",
            "零食", "家电", "玩具", "书包", "口红", "充电器"]
BRANDS_LIST = ["苹果", "华为", "小米", "耐克", "阿迪达斯", "李宁", "优衣库",
               "无印良品", "戴森", "美的", "茅台", "索尼", "三星", "联想", "海尔"]
COUNTRIES = ["中国", "美国", "日本", "德国", "韩国", "法国", "英国", "意大利"]
PROVINCES = ["北京", "上海", "广东", "浙江", "四川", "湖北", "陕西", "江苏",
             "重庆", "天津", "宁波", "河南", "湖南", "福建", "山东"]

START_DT = datetime(2024, 1, 1)
END_DT = datetime(2025, 12, 31)


# ─── Helpers ─────────────────────────────────────────────────────────────────
def rand_dt():
    delta = END_DT - START_DT
    return (START_DT + timedelta(seconds=random.randint(0, int(delta.total_seconds())))).strftime("%Y-%m-%d %H:%M:%S")


def rand_date():
    delta = END_DT - START_DT
    return (START_DT + timedelta(days=random.randint(0, delta.days))).strftime("%Y-%m-%d")


def rand_str(n=8):
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))


def rand_ip():
    return f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"


def rand_name():
    return random.choice(NAMES)


def rand_city():
    return random.choice(CITIES)


def fk(ids, table, fallback_max=20):
    lst = ids.get(table)
    if lst:
        return random.choice(lst)
    return random.randint(1, fallback_max)


# ─── Type inference ──────────────────────────────────────────────────────────
def col_type(col_name):
    n = col_name.lower()
    if n.endswith("_id") or n == "id":
        return "INTEGER"
    if any(n.endswith(s) for s in ("_amount", "_price", "_cost", "_rate",
                                    "_score", "_value", "_ratio", "_pct",
                                    "_margin", "_fee")):
        return "REAL"
    if any(pat in n for pat in ("exchange_rate",)):
        return "REAL"
    if n.startswith("weight"):
        return "REAL"
    if any(n.endswith(s) for s in ("_count", "_qty", "_seconds", "_minutes",
                                    "_days", "_ms")):
        return "INTEGER"
    if n in ("period_no", "periods", "position", "hour", "fan_count",
             "helper_count", "result_count", "latency_ms", "load_seconds",
             "sync_rows", "duration_ms", "impression_count", "rows_processed",
             "fail_count", "sample_qty", "defect_qty", "click_count",
             "like_count", "comment_count", "helpful_count", "order_count",
             "page_count", "foot_traffic", "max_online_users", "total_viewers",
             "total_orders", "rank_position", "prev_position",
             "suggest_position", "item_count", "promise_minutes",
             "actual_minutes", "stay_minutes", "watch_seconds", "speed_kmh"):
        return "INTEGER"
    if n.startswith("page"):
        return "INTEGER"
    if n.startswith("is_") or n.startswith("has_") or n.endswith("_flag"):
        return "INTEGER"
    if any(n.endswith(s) for s in ("_dt", "_date")) or n in ("date_id", "full_date"):
        return "TEXT"
    return "TEXT"


# ─── Schema loading ──────────────────────────────────────────────────────────
def load_registry():
    with open(REGISTRY_PATH) as f:
        return json.load(f)["tables"]


# ─── CREATE TABLE ────────────────────────────────────────────────────────────
def create_tables(conn, tables):
    cur = conn.cursor()
    for tbl, info in tables.items():
        cols = info["columns"]
        col_defs = []
        for col_name, col_info in cols.items():
            ct = col_type(col_name)
            col_defs.append(f"  {col_name} {ct}")
        ddl = f"CREATE TABLE {tbl} (\n" + ",\n".join(col_defs) + "\n);"
        cur.execute(ddl)
    conn.commit()


# ─── Value generator ─────────────────────────────────────────────────────────
def gen_val(col_name, col_info, ids, nullable_chance=0.2):
    nullable = col_info.get("nullable", False)
    if nullable and random.random() < nullable_chance:
        return None

    enum = col_info.get("enum", [])
    # filter out None from enum for picking
    clean_enum = [e for e in enum if e is not None]

    n = col_name.lower()
    ct = col_type(col_name)

    # FK resolution
    fk_map = {
        "user_id": "dim_user",
        "product_id": "dim_product",
        "order_id": "dwd_order_detail",
        "order_item_id": "dwd_order_item_detail",
        "category_id": "dim_category",
        "brand_id": "dim_brand",
        "supplier_id": "dim_supplier",
        "merchant_id": "dim_merchant",
        "channel_id": "dim_channel",
        "platform_id": "dim_platform",
        "device_id": "dim_device",
        "warehouse_id": "dim_warehouse",
        "region_id": "dim_region",
        "city_id": "dim_city",
        "province_id": "dim_province",
        "vip_level_id": "dim_vip_level",
        "risk_level_id": "dim_risk_level",
        "coupon_id": "dim_coupon",
        "promotion_id": "dim_promotion",
        "payment_method_id": "dim_payment_method",
        "logistics_id": "dim_logistics_company",
        "reason_id": "dim_return_reason",
        "after_sale_type_id": "dim_after_sale_type",
        "employee_id": "dim_employee",
        "operator_id": "dim_employee",
        "checker_id": "dim_employee",
        "auditor_id": "dim_employee",
        "rater_id": "dim_employee",
        "department_id": "dim_department",
        "cs_id": "dim_customer_service",
        "store_id": "dim_store",
        "anchor_id": "dim_anchor",
        "placement_id": "dim_ad_placement",
        "algo_id": "dim_recommendation_algo",
        "rec_algo_id": "dim_recommendation_algo",
        "intent_id": "dim_search_intent",
        "content_type_id": "dim_content_type",
        "template_id": "dim_message_template",
        "plan_id": "dim_subscription_plan",
        "rule_id": "dim_point_rule",
        "segment_id": "dim_user_segment",
        "label_id": "dim_user_label",
        "activity_id": "dim_activity",
        "experiment_id": "dim_ab_experiment",
        "live_id": "dwd_live_room_detail",
        "refund_id": "dwd_refund_detail",
        "after_sale_id": "dwd_after_sale_detail",
        "comment_id": "dwd_comment_detail",
        "search_id": "dwd_search_log",
        "impression_id": "dwd_ad_impression_log",
        "rec_id": "dwd_recommendation_expose_log",
        "push_id": "dwd_push_send_log",
    }

    if col_name in fk_map:
        target = fk_map[col_name]
        return fk(ids, target)

    if clean_enum:
        return random.choice(clean_enum)

    # type-based generation
    if ct == "INTEGER":
        if n.startswith("is_") or n.startswith("has_") or n.endswith("_flag"):
            return random.randint(0, 1)
        if "score" in n:
            return random.randint(1, 5)
        if "count" in n or "qty" in n or "quantity" in n:
            return random.randint(1, 100)
        if "seconds" in n:
            return random.randint(5, 3600)
        if "minutes" in n:
            return random.randint(1, 120)
        if "days" in n:
            return random.randint(1, 365)
        if "hour" == n:
            return random.randint(0, 23)
        if "position" in n or "rank" in n:
            return random.randint(1, 100)
        if "period_no" == n:
            return random.randint(1, 12)
        if "periods" == n:
            return random.choice([3, 6, 12])
        if "page" in n:
            return random.randint(1, 50)
        if "latency" in n or "duration" in n or "load" in n:
            return random.randint(50, 5000)
        if "rows" in n or "sync_rows" in n:
            return random.randint(100, 100000)
        return random.randint(1, 1000)

    if ct == "REAL":
        if "amount" in n or "price" in n or "cost" in n or "value" in n:
            if "total" in n or "order" in n:
                return round(random.uniform(50, 5000), 2)
            if "unit" in n:
                return round(random.uniform(10, 3000), 2)
            if "cost" in n:
                return round(random.uniform(5, 2000), 2)
            return round(random.uniform(1, 1000), 2)
        if "rate" in n or "ratio" in n or "pct" in n:
            return round(random.uniform(0.01, 1.0), 4)
        if "margin" in n:
            return round(random.uniform(0.05, 0.6), 4)
        if "score" in n:
            return round(random.uniform(1.0, 5.0), 2)
        if "exchange" in n:
            return round(random.uniform(0.1, 10.0), 4)
        if "weight" in n:
            return round(random.uniform(0.1, 50.0), 2)
        return round(random.uniform(0.0, 100.0), 2)

    # TEXT
    if n.endswith("_dt") or n == "start_dt" or n == "end_dt":
        return rand_dt()
    if n.endswith("_date") or n in ("date_id", "full_date", "record_date", "calc_date",
                                     "data_date", "rate_date", "alloc_date"):
        return rand_date()
    if n == "user_name" or n == "anchor_name" or n == "cs_name" or n == "employee_name":
        return rand_name()
    if n == "product_name":
        return random.choice(PRODUCTS)
    if n == "category_name":
        return random.choice(CATEGORIES)
    if n == "brand_name":
        return random.choice(BRANDS_LIST)
    if n in ("city_name",):
        return rand_city()
    if n in ("province_name",):
        return random.choice(PROVINCES)
    if n in ("region_name",):
        return random.choice(PROVINCES + CITIES)
    if n == "country":
        return random.choice(COUNTRIES)
    if n in ("keyword", "input_keyword", "selected_suggest"):
        return random.choice(KEYWORDS)
    if "tracking_no" in n or "pickup_code" in n:
        return rand_str(12)
    if "ip_address" in n:
        return rand_ip()
    if "batch_no" in n or "cert_no" in n or "card_no" in n:
        return rand_str(10)
    if "third_party_txn_id" in n:
        return rand_str(16)
    if "session_id" in n:
        return rand_str(16)
    if "video_id" in n or "content_id" in n or "banner_id" in n:
        return str(random.randint(1000, 9999))
    if "kol_id" in n:
        return str(random.randint(1, 50))
    if "inviter_id" in n or "invitee_id" in n or "asker_id" in n or "answerer_id" in n:
        return str(fk(ids, "dim_user"))
    if "enterprise_id" in n:
        return str(random.randint(1, 30))
    if "rider_id" in n or "courier_id" in n:
        return str(random.randint(1, 50))
    if "_name" in n:
        return rand_name() + rand_str(4)
    if "detail_address" in n:
        return f"{rand_city()}{rand_str(3)}路{random.randint(1,999)}号"
    if "settle_account" in n:
        return rand_str(16)
    if "binlog_file" in n:
        return f"mysql-bin.{random.randint(1000,9999)}"
    if "binlog_pos" in n:
        return str(random.randint(1000, 99999))
    if "pk_value" in n or "before_data" in n or "after_data" in n:
        return f'{{"id":{random.randint(1,10000)}}}'
    if "issue_desc" in n or "evidence" in n or "reply_content" in n or "feedback" in n:
        return "示例描述内容"
    if "benefits" in n or "strategy" in n:
        return "基础权益套餐"
    if "skill_tags" in n:
        return "售前,退换货"
    if "job_title" in n:
        return random.choice(["工程师", "产品经理", "运营专员", "数据分析师", "客服"])
    if "cost_center" in n:
        return f"CC{random.randint(100,999)}"
    if "logistics_zone" in n:
        return random.choice(["华北区", "华东区", "华南区", "西部区"])
    if "pipeline_name" in n:
        return random.choice(["ods_to_dwd", "dwd_to_dws", "dim_sync", "etl_main"])
    if "table_name" in n:
        return random.choice(["dim_user", "dwd_order_detail", "dwd_payment_detail"])
    if "column_name" in n:
        return random.choice(["user_id", "order_id", "amount", "status"])
    if "check_rule" in n:
        return random.choice(["not_null", "range_check", "enum_check", "pk_unique"])
    if "api_name" in n:
        return random.choice(["/api/order", "/api/user", "/api/product", "/api/payment"])
    if "caller_id" in n:
        return f"svc_{rand_str(4)}"
    if "source_table" in n:
        return random.choice(["order", "user", "product", "payment"])
    if "target_table" in n:
        return random.choice(["dwd_order_detail", "dim_user", "dwd_payment_detail"])
    if "report_name" in n:
        return random.choice(["日销售报表", "用户分析", "商品排行", "物流统计"])
    if "feature_name" in n:
        return random.choice(["new_cart_ui", "rec_v2", "checkout_v3", "search_v4"])
    if "resource_name" in n:
        return random.choice(["销售报表", "用户数据库", "订单系统", "财务审批"])
    if "node_name" in n:
        return random.choice(["部门主管审批", "财务审批", "总监审批"])
    if "metric_name" in n:
        return random.choice(["GMV", "转化率", "点击率", "DAU"])
    if "destination_country" in n:
        return random.choice(COUNTRIES)
    if "product_ids" in n:
        ids_sample = [str(fk(ids, "dim_product")) for _ in range(random.randint(2, 4))]
        return ",".join(ids_sample)
    if "r_score_range" in n or "f_score_range" in n or "m_score_range" in n:
        return f"{random.randint(1,3)}-{random.randint(4,5)}"
    if "error_msg" in n or "reject_reason" in n or "comment" in n:
        return None if random.random() < 0.5 else "备注信息"
    if "old_value" in n:
        return None if random.random() < 0.3 else str(random.randint(1, 1000))
    if "new_value" in n:
        return str(random.randint(1, 1000))
    return rand_str(8)


# ─── Seed a single table ─────────────────────────────────────────────────────
def seed_table(conn, table_name, col_defs, ids, n_rows):
    cols = list(col_defs.keys())
    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"

    rows = []
    pk_col = cols[0]  # first column is PK
    pk_vals = []

    for i in range(1, n_rows + 1):
        row = []
        for j, (col_name, col_info) in enumerate(col_defs.items()):
            if j == 0:
                # PK: always sequential integer
                row.append(i)
            else:
                row.append(gen_val(col_name, col_info, ids))
        rows.append(tuple(row))
        pk_vals.append(i)

    conn.executemany(sql, rows)
    conn.commit()
    ids[table_name] = pk_vals
    print(f"  Seeding {table_name}... {n_rows} rows")


# ─── Special table seeders ────────────────────────────────────────────────────
def seed_dim_date(conn, ids):
    """Generate actual calendar dates for 2024 and 2025."""
    rows = []
    date_names = {
        (1, 1): "元旦", (5, 1): "五一", (10, 1): "国庆",
        (11, 11): "双十一", (6, 18): "618", (12, 12): "双十二",
        (3, 8): "38女王节", (9, 9): "99大促",
    }
    dow_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    pk_vals = []
    i = 1
    for year in [2024, 2025]:
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31)
        cur = start
        while cur <= end:
            dow = cur.weekday()
            is_weekend = 1 if dow >= 5 else 0
            is_workday = 0 if is_weekend else 1
            holiday = date_names.get((cur.month, cur.day))
            is_holiday = 1 if holiday else is_weekend
            rows.append((
                i,
                cur.strftime("%Y-%m-%d"),
                cur.year,
                (cur.month - 1) // 3 + 1,
                cur.month,
                int(cur.strftime("%W")),
                dow_map[dow],
                is_holiday,
                is_workday,
                holiday,
            ))
            pk_vals.append(i)
            i += 1
            cur += timedelta(days=1)

    cols = "date_id,full_date,year,quarter,month,week,day_of_week,is_holiday,is_workday,holiday_name"
    conn.executemany(f"INSERT INTO dim_date ({cols}) VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()
    ids["dim_date"] = pk_vals
    print(f"  Seeding dim_date... {len(rows)} rows")


def seed_dim_time(conn, ids):
    """Generate 24-hour time dimension."""
    def period(h):
        if h < 6: return "凌晨"
        if h < 9: return "早晨"
        if h < 12: return "上午"
        if h < 14: return "午间"
        if h < 18: return "下午"
        if h < 20: return "傍晚"
        return "夜间"

    rows = [(i + 1, i, period(i), 1 if i in (10, 11, 20, 21, 22) else 0)
            for i in range(24)]
    pk_vals = [r[0] for r in rows]
    conn.executemany("INSERT INTO dim_time (time_id,hour,time_period,is_peak) VALUES (?,?,?,?)", rows)
    conn.commit()
    ids["dim_time"] = pk_vals
    print(f"  Seeding dim_time... 24 rows")


# ─── Determine row count ─────────────────────────────────────────────────────
CORE_DWD = {
    "dwd_order_detail", "dwd_order_item_detail", "dwd_payment_detail",
    "dwd_logistics_detail", "dwd_delivery_detail", "dwd_page_view_log",
    "dwd_product_view_log", "dwd_search_log", "dwd_click_log",
}
SMALL_DWD = {
    "dwd_etl_run_log", "dwd_api_call_log", "dwd_data_quality_check_log",
    "dwd_data_sync_log", "dwd_cdc_change_log", "dwd_report_access_log",
}


def row_count(table_name):
    if table_name.startswith("dim_"):
        return random.randint(20, 100)
    if table_name in CORE_DWD:
        return random.randint(2000, 5000)
    if table_name in SMALL_DWD:
        return random.randint(200, 500)
    return random.randint(500, 1000)


# ─── Seed order ──────────────────────────────────────────────────────────────
DIM_ORDER = [
    # geography
    "dim_province", "dim_region", "dim_city",
    # product
    "dim_category", "dim_brand", "dim_supplier",
    # user support
    "dim_vip_level", "dim_risk_level", "dim_user_segment",
    "dim_rfm_segment",
    # platform / channel / device
    "dim_platform", "dim_channel", "dim_device",
    # payment / logistics
    "dim_payment_method", "dim_logistics_company", "dim_logistics_route",
    # coupon / promotion
    "dim_coupon", "dim_promotion", "dim_activity",
    # order support
    "dim_return_reason", "dim_after_sale_type",
    # warehouse
    "dim_warehouse",
    # personnel
    "dim_department", "dim_employee", "dim_customer_service",
    # merchant / store
    "dim_merchant", "dim_store",
    # product extras
    "dim_tag", "dim_unit", "dim_review_tag", "dim_product_cert",
    # user
    "dim_user", "dim_user_label",
    # content / live
    "dim_content_type", "dim_content_label", "dim_anchor",
    "dim_live_category",
    # ad / rec
    "dim_ad_placement", "dim_recommendation_algo", "dim_search_intent",
    # special
    "dim_gift_card", "dim_subscription_plan", "dim_point_rule",
    "dim_ab_experiment", "dim_message_template", "dim_holiday",
    # time (handled specially below)
    "dim_delivery_time_slot", "dim_tax_rate", "dim_currency",
    "dim_product_spec",
]

DWD_ORDER = [
    # core transactions
    "dwd_order_detail", "dwd_order_item_detail",
    "dwd_payment_detail", "dwd_refund_detail",
    "dwd_return_goods_detail",
    "dwd_logistics_detail", "dwd_delivery_detail",
    # after sale
    "dwd_after_sale_detail", "dwd_comment_detail",
    # live
    "dwd_live_room_detail", "dwd_live_watch_log", "dwd_live_order_detail",
    "dwd_anchor_detail", "dwd_anchor_live_schedule",
    # cart / coupon
    "dwd_cart_detail", "dwd_coupon_use_detail", "dwd_coupon_issue_detail",
    # behavior logs
    "dwd_page_view_log", "dwd_product_view_log", "dwd_search_log",
    "dwd_click_log", "dwd_favorite_log", "dwd_share_log",
    # ad
    "dwd_ad_impression_log", "dwd_ad_click_log", "dwd_ad_convert_log",
    "dwd_ad_budget_detail", "dwd_ad_keyword_bid_log",
    # rec
    "dwd_recommendation_expose_log", "dwd_recommendation_click_log",
    # push
    "dwd_push_send_log", "dwd_push_click_log",
    # user events
    "dwd_user_register_log", "dwd_user_login_log",
    "dwd_user_profile_change_log", "dwd_address_detail",
    # points / vip / subscription
    "dwd_point_earn_detail", "dwd_point_use_detail",
    "dwd_vip_upgrade_log", "dwd_subscription_detail",
    "dwd_gift_card_use_detail", "dwd_auto_renew_log",
    "dwd_svip_benefit_use_log",
    # warehouse / supply
    "dwd_warehouse_inbound_detail", "dwd_warehouse_outbound_detail",
    "dwd_inventory_change_detail", "dwd_inventory_reserve_detail",
    "dwd_stock_alert_log",
    "dwd_purchase_order_detail", "dwd_supplier_settle_detail",
    "dwd_quality_check_detail", "dwd_supplier_quality_rating",
    # merchant
    "dwd_merchant_settle_detail", "dwd_merchant_login_log",
    "dwd_merchant_product_operate_log", "dwd_merchant_settle_apply",
    "dwd_merchant_penalty_log",
    # price / product ops
    "dwd_price_change_log", "dwd_product_audit_log",
    "dwd_product_label_assign_log", "dwd_product_rank_log",
    "dwd_sku_availability_log",
    # cs / complaints
    "dwd_consult_log", "dwd_complaint_detail", "dwd_after_sale_detail",
    "dwd_review_audit_log",
    # search extras
    "dwd_search_result_click_log", "dwd_search_no_result_log",
    "dwd_search_suggest_log", "dwd_filter_use_log",
    # promotions
    "dwd_promotion_apply_detail", "dwd_activity_expose_log",
    "dwd_flash_sale_order_detail", "dwd_group_buy_detail",
    "dwd_pre_sale_detail",
    "dwd_seckill_queue_log", "dwd_bargain_detail", "dwd_bargain_help_log",
    # o2o / store
    "dwd_store_visit_log", "dwd_o2o_order_detail",
    "dwd_store_inventory_detail", "dwd_store_revenue_detail",
    "dwd_store_staff_attendance", "dwd_store_task_log",
    "dwd_self_pickup_log",
    # special orders
    "dwd_flash_sale_order_detail", "dwd_double11_order_detail",
    "dwd_618_order_detail",
    "dwd_enterprise_info", "dwd_enterprise_order_detail",
    "dwd_enterprise_contract_detail",
    "dwd_instant_order_detail", "dwd_rider_track_log",
    "dwd_points_mall_order",
    # user behavior extras
    "dwd_product_compare_log", "dwd_follow_log",
    "dwd_invite_log", "dwd_brand_follow_log",
    "dwd_category_browse_log", "dwd_short_video_log",
    "dwd_ugc_content_detail", "dwd_content_report_log",
    "dwd_notification_detail", "dwd_cashier_session_log",
    "dwd_price_alert_log", "dwd_user_label_assign_log",
    "dwd_user_device_bind_log", "dwd_session_detail",
    # ab test
    "dwd_ab_test_exposure_log", "dwd_ab_test_convert_log",
    "dwd_feature_flag_log",
    # financial
    "dwd_invoice_detail", "dwd_cashback_detail",
    "dwd_revenue_recognition", "dwd_cost_allocation",
    "dwd_gross_profit_detail",
    "dwd_payment_installment_detail", "dwd_wallet_transaction_detail",
    "dwd_order_cancel_detail",
    # rating / review
    "dwd_rating_detail", "dwd_review_reply_detail",
    "dwd_bundle_order_detail",
    # rfm / ltv
    "dwd_rfm_score_detail", "dwd_user_lifetime_value",
    # risk / fraud
    "dwd_fraud_detect_log", "dwd_risk_event_log",
    "dwd_blacklist_log",
    # cross-border
    "dwd_cross_border_order_detail", "dwd_customs_clearance_log",
    "dwd_bonded_warehouse_detail", "dwd_fx_rate_log",
    # kol / qa
    "dwd_kol_cooperation_detail", "dwd_product_qa_detail",
    # internal ops
    "dwd_permission_change_log", "dwd_approval_flow_log",
    "dwd_channel_attribution_detail", "dwd_product_exposure_log",
    # logistics extras
    "dwd_logistics_exception_log", "dwd_logistics_cost_detail",
    # nps / warranty
    "dwd_nps_survey_detail", "dwd_warranty_claim_detail",
    # etl / data eng
    "dwd_etl_run_log", "dwd_data_quality_check_log",
    "dwd_data_sync_log", "dwd_cdc_change_log", "dwd_report_access_log",
    "dwd_api_call_log",
]


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    if DB_PATH.exists():
        DB_PATH.unlink()

    tables = load_registry()

    # Build the DB in a local temp file to avoid SMB/NFS locking issues,
    # then copy to final destination.
    tmp_dir = Path(tempfile.mkdtemp())
    tmp_db = tmp_dir / "large_demo.db"

    conn = sqlite3.connect(tmp_db)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-65536")

    print("Creating tables...")
    create_tables(conn, tables)

    ids = {}

    print("\nSeeding dim tables...")
    # Special dim_date and dim_time first
    seed_dim_date(conn, ids)
    seed_dim_time(conn, ids)

    # Seed dims in order
    seeded = {"dim_date", "dim_time"}
    for tbl in DIM_ORDER:
        if tbl in seeded:
            continue
        if tbl not in tables:
            continue
        n = row_count(tbl)
        seed_table(conn, tbl, tables[tbl]["columns"], ids, n)
        seeded.add(tbl)

    # Seed any remaining dim tables not in DIM_ORDER
    for tbl in tables:
        if tbl.startswith("dim_") and tbl not in seeded:
            n = row_count(tbl)
            seed_table(conn, tbl, tables[tbl]["columns"], ids, n)
            seeded.add(tbl)

    print("\nSeeding dwd tables...")
    seeded_dwd = set()
    for tbl in DWD_ORDER:
        if tbl in seeded_dwd:
            continue
        if tbl not in tables:
            continue
        n = row_count(tbl)
        seed_table(conn, tbl, tables[tbl]["columns"], ids, n)
        seeded_dwd.add(tbl)

    # Seed any remaining dwd tables not in DWD_ORDER
    for tbl in tables:
        if tbl.startswith("dwd_") and tbl not in seeded_dwd:
            n = row_count(tbl)
            seed_table(conn, tbl, tables[tbl]["columns"], ids, n)
            seeded_dwd.add(tbl)

    conn.close()

    # Copy from temp to final destination
    print(f"\nCopying to {DB_PATH}...")
    shutil.copy2(tmp_db, DB_PATH)
    shutil.rmtree(tmp_dir)

    total = sum(len(v) for v in ids.values())
    print(f"Done: {DB_PATH}")
    print(f"Total tables: {len(ids)}, Total rows: {total:,}")


if __name__ == "__main__":
    main()
