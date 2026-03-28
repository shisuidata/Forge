#!/usr/bin/env python3
"""
Forge Mock 数据填充脚本。

覆盖所有数据存储，生成贴近真实场景的测试数据：
    - audit_log:           100 条审计记录（各状态分布）
    - memory_ems:          多个会话的完整对话轮次
    - memory_smp:          语义记忆（org/team/user 三级）
    - tenant_teams:        5 个团队
    - tenant_users:        20 个用户
    - team_table_acl:      团队表权限
    - knowledge_candidates: 知识候选（各来源、各状态）
    - knowledge_sources:   知识源配置

用法：
    python scripts/seed_mock_data.py              # 填充到默认数据库
    python scripts/seed_mock_data.py --clean      # 清空后重新填充
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 确保项目根目录在 path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aiosqlite
import asyncio

from agent.db import get_engine, execute_ddl
from sqlalchemy import text

# ── 常量 ──────────────────────────────────────────────────────────────────────

NOW = datetime.utcnow()

TEAMS = [
    ("default",     "默认团队"),
    ("marketing",   "营销部"),
    ("analytics",   "数据分析组"),
    ("engineering", "研发部"),
    ("finance",     "财务部"),
]

USERS = [
    # (user_id, display_name, team_id, role)
    ("ou_admin001",   "张明（管理员）", "default",     "admin"),
    ("ou_lina001",    "李娜",          "marketing",   "member"),
    ("ou_wangfang",   "王芳",          "marketing",   "member"),
    ("ou_zhaowei",    "赵伟",          "analytics",   "admin"),
    ("ou_liuyang",    "刘洋",          "analytics",   "member"),
    ("ou_chenxi",     "陈曦",          "analytics",   "member"),
    ("ou_wujie",      "吴洁",          "analytics",   "member"),
    ("ou_sunlei",     "孙磊",          "engineering", "admin"),
    ("ou_zhoujing",   "周静",          "engineering", "member"),
    ("ou_xuyun",      "徐云",          "engineering", "member"),
    ("ou_huangli",    "黄力",          "finance",     "admin"),
    ("ou_zhenghao",   "郑浩",          "finance",     "member"),
    ("ou_mayu",       "马瑜",          "default",     "member"),
    ("ou_helin",      "何琳",          "default",     "member"),
    ("ou_guozhi",     "郭志",          "marketing",   "member"),
    ("ou_yangfei",    "杨菲",          "analytics",   "member"),
    ("ou_songyi",     "宋毅",          "engineering", "member"),
    ("ou_tanxin",     "谭欣",          "finance",     "member"),
    ("web_user_demo", "Web演示用户",   "analytics",   "member"),
    ("feishu_bot",    "飞书Bot",       "default",     "member"),
]

# 每个团队可访问的表
TEAM_ACL = {
    "default":     None,  # None = 不限制
    "marketing":   [
        "dim_user", "dim_region", "dim_campaign", "dwd_order_detail",
        "dwd_payment", "dws_user_daily", "ads_marketing_roi",
    ],
    "analytics":   None,  # 分析组可访问全部表
    "engineering": [
        "dim_user", "dwd_order_detail", "dwd_order_item",
        "ods_log_event", "dwd_api_call",
    ],
    "finance":     [
        "dim_user", "dwd_order_detail", "dwd_payment",
        "dwd_refund", "dws_finance_daily", "ads_revenue_report",
    ],
}

# ── 查询场景数据 ──────────────────────────────────────────────────────────────

QUERY_SCENARIOS = [
    # (user_message, sql, forge_json_snippet, status)
    # --- 成功执行 ---
    ("各城市的订单总额是多少",
     "SELECT u.city, SUM(o.total_amount) AS total FROM orders o JOIN users u ON o.user_id = u.id GROUP BY u.city ORDER BY total DESC",
     {"mode": "query", "from": "orders", "joins": [{"table": "users", "on": "orders.user_id = users.id"}]},
     "approved"),
    ("最近30天的VIP用户数量",
     "SELECT COUNT(DISTINCT id) AS vip_count FROM users WHERE is_vip = 1 AND created_at >= date('now','-30 days')",
     {"mode": "query", "from": "users", "filters": [{"field": "is_vip", "op": "=", "value": 1}]},
     "approved"),
    ("每个品类的退货率排名",
     "WITH orders_by_cat AS (SELECT p.category, COUNT(*) AS total, SUM(CASE WHEN o.status='refunded' THEN 1 ELSE 0 END) AS refunded FROM orders o JOIN order_items oi ON o.id=oi.order_id JOIN products p ON oi.product_id=p.id GROUP BY p.category) SELECT category, ROUND(1.0*refunded/total, 4) AS refund_rate FROM orders_by_cat ORDER BY refund_rate DESC",
     {"mode": "query", "from": "orders", "ctes": [{"name": "orders_by_cat"}]},
     "approved"),
    ("本月新增用户的首单转化率",
     "SELECT ROUND(1.0 * COUNT(DISTINCT o.user_id) / COUNT(DISTINCT u.id), 4) AS conversion_rate FROM users u LEFT JOIN orders o ON u.id = o.user_id AND o.created_at >= date('now','start of month') WHERE u.created_at >= date('now','start of month')",
     {"mode": "query", "from": "users"},
     "approved"),
    ("各渠道的客单价对比",
     "SELECT u.channel, ROUND(AVG(o.total_amount), 2) AS avg_order_value FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 'completed' GROUP BY u.channel ORDER BY avg_order_value DESC",
     {"mode": "query", "from": "orders"},
     "approved"),
    ("上周每天的订单量趋势",
     "SELECT DATE(created_at) AS dt, COUNT(*) AS order_count FROM orders WHERE created_at >= date('now','-7 days') GROUP BY DATE(created_at) ORDER BY dt",
     {"mode": "query", "from": "orders"},
     "approved"),
    ("复购用户占比",
     "SELECT ROUND(1.0 * SUM(CASE WHEN order_cnt >= 2 THEN 1 ELSE 0 END) / COUNT(*), 4) AS repurchase_rate FROM (SELECT user_id, COUNT(*) AS order_cnt FROM orders WHERE status = 'completed' GROUP BY user_id)",
     {"mode": "query", "from": "orders", "ctes": [{"name": "user_orders"}]},
     "approved"),
    ("GMV 环比增长率",
     "WITH monthly AS (SELECT strftime('%Y-%m', created_at) AS month, SUM(total_amount) AS gmv FROM orders WHERE status IN ('shipped','completed') GROUP BY month ORDER BY month DESC LIMIT 2) SELECT * FROM monthly",
     {"mode": "query", "from": "orders"},
     "approved"),

    # --- 待确认 ---
    ("哪些商品最近卖得好",
     "SELECT p.name, SUM(oi.quantity) AS total_qty FROM order_items oi JOIN products p ON oi.product_id = p.id GROUP BY p.name ORDER BY total_qty DESC LIMIT 20",
     {"mode": "query", "from": "order_items"},
     "pending"),
    ("用户留存率怎么样",
     "SELECT COUNT(DISTINCT CASE WHEN o2.user_id IS NOT NULL THEN o1.user_id END) * 1.0 / COUNT(DISTINCT o1.user_id) AS retention FROM orders o1 LEFT JOIN orders o2 ON o1.user_id = o2.user_id AND o2.created_at > o1.created_at",
     {"mode": "query", "from": "orders"},
     "pending"),

    # --- 已取消 ---
    ("帮我看看竞品数据",
     None, None, "cancelled"),
    ("删除所有过期订单",
     None, None, "cancelled"),

    # --- 错误 ---
    ("统计每个省份的人均消费",
     None, None, "error"),
    ("用户的 LTV 是多少",
     None, None, "error"),
    ("分析用户画像",
     None, None, "error"),
]

# ── 会话场景 ──────────────────────────────────────────────────────────────────

SESSION_SCENARIOS = [
    {
        "session_id": "s_demo_query_001",
        "user_id": "web_user_demo",
        "messages": [
            ("user", "各城市的订单总额是多少", None, None, None, None),
            ("assistant", "", "generate_forge_query",
             '{"mode":"query","from":"orders"}',
             "SELECT u.city, SUM(o.total_amount) AS total FROM orders o JOIN users u ON o.user_id = u.id GROUP BY u.city",
             "sql_review"),
            ("state", "", None, None, None, "state_set"),
            ("assistant", "SQL 已确认执行", None, None, None, "approved"),
        ],
    },
    {
        "session_id": "s_demo_metric_001",
        "user_id": "ou_zhaowei",
        "messages": [
            ("user", "帮我定义一个复购率指标", None, None, None, None),
            ("assistant", "📋 **指标定义提案**\n\n复购率 = 下过2次及以上订单的用户数 / 所有下单用户数\n\n如确认无误，请回复「确认」",
             "propose_metric_definition",
             '{"name":"repurchase_rate","metric_class":"derivative","numerator":"repeat_buyers","denominator":"total_buyers"}',
             None, "metric_clarification"),
            ("user", "确认", None, None, None, None),
            ("assistant", "✅ 指标「复购率」已保存到 Registry", None, None, None, "metric_saved"),
        ],
    },
    {
        "session_id": "s_demo_clarify_001",
        "user_id": "ou_lina001",
        "messages": [
            ("user", "这个月的销售额多少", None, None, None, None),
            ("assistant", "请确认：您说的「销售额」是指哪个口径？\n1. 订单金额（含未支付）\n2. 实际收款金额（已支付 + 已完成）",
             None, None, None, "clarification"),
            ("user", "实际收款金额", None, None, None, None),
            ("assistant", "", "generate_forge_query",
             '{"mode":"query","from":"orders","filters":[{"field":"status","op":"IN","value":["paid","completed"]}]}',
             "SELECT SUM(total_amount) AS revenue FROM orders WHERE status IN ('paid','completed') AND created_at >= date('now','start of month')",
             "sql_review"),
            ("state", "", None, None, None, "state_set"),
            ("assistant", "SQL 已确认执行", None, None, None, "approved"),
        ],
    },
    {
        "session_id": "s_demo_error_001",
        "user_id": "ou_liuyang",
        "messages": [
            ("user", "统计每个省份的人均消费", None, None, None, None),
            ("assistant", "⚠ 查询生成失败（已重试 2 次）：数据库中没有省份字段，只有城市(city)。请改用城市维度，或先在 Registry 中定义省份映射。",
             None, None, None, "error"),
        ],
    },
    {
        "session_id": "s_demo_pipeline_001",
        "user_id": "ou_chenxi",
        "messages": [
            ("user", "分析各城市订单趋势", None, None, None, None),
            ("assistant", "", "generate_forge_query",
             '{"mode":"query","from":"orders"}',
             "SELECT u.city, DATE(o.created_at) AS dt, COUNT(*) AS cnt FROM orders o JOIN users u ON o.user_id = u.id GROUP BY u.city, dt ORDER BY dt",
             "sql_review"),
            ("state", "", None, None, None, "state_set"),
            ("assistant", "SQL 已执行，正在生成分析报告...", None, None, None, "approved"),
            ("assistant", "📊 分析报告\n\n北京和上海占总订单量的 62%，近 7 天北京增速最快（+15%）。建议关注三线城市的增长潜力。",
             None, None, None, "message"),
        ],
    },
]

# ── SMP 记忆 ──────────────────────────────────────────────────────────────────

SMP_RECORDS = [
    # org 级
    ("org", "__org__", "confirmed_fact", "default_revenue_scope",
     '{"scope":"已完成订单","note":"销售额默认统计已完成状态的订单金额"}',
     "s_demo_clarify_001", 1.0),
    ("org", "__org__", "confirmed_fact", "refund_rate_formula",
     '{"formula":"退货订单数 / 总订单数","note":"退货率按订单维度计算，非金额维度"}',
     "s_demo_query_001", 1.0),
    ("org", "__org__", "threshold", "refund_rate_warning",
     '{"value":0.05,"severity":"warning","note":"退货率超过 5% 需告警"}',
     "", 0.95),
    ("org", "__org__", "calendar", "shopping_festivals_2026",
     '{"dates":["2026-01-01","2026-02-14","2026-06-18","2026-11-11","2026-12-12"],"note":"电商大促日期"}',
     "", 0.9),
    ("org", "__org__", "benchmark", "industry_avg_order_value",
     '{"value":185.0,"currency":"CNY","industry":"电商","year":2025}',
     "", 0.85),

    # team 级
    ("team", "__team__marketing", "rule", "campaign_attribution_window",
     '{"days":7,"note":"营销归因窗口默认 7 天"}',
     "", 1.0),
    ("team", "__team__analytics", "user_profile", "preferred_chart_type",
     '{"type":"line","note":"分析组偏好折线图展示趋势"}',
     "", 0.8),
    ("team", "__team__finance", "rule", "revenue_recognition",
     '{"condition":"订单状态为已完成且已过退货期","note":"收入确认规则"}',
     "", 1.0),

    # user 级
    ("user", "ou_zhaowei", "user_profile", "expertise",
     '{"domain":"数据分析","level":"senior","note":"熟悉 SQL 和数仓"}',
     "s_demo_metric_001", 0.9),
    ("user", "ou_lina001", "correction", "revenue_means_paid",
     '{"original":"销售额","corrected_to":"实收金额（已支付+已完成）","note":"用户明确过销售额口径偏好"}',
     "s_demo_clarify_001", 1.0),
    ("user", "ou_liuyang", "user_profile", "common_queries",
     '{"topics":["用户留存","转化漏斗","品类分析"],"note":"常查主题"}',
     "s_demo_error_001", 0.7),
    ("user", "web_user_demo", "user_profile", "usage_pattern",
     '{"frequency":"daily","peak_hour":10,"note":"每天上午 10 点左右使用"}',
     "s_demo_query_001", 0.6),
    ("user", "ou_chenxi", "confirmed_fact", "city_grouping",
     '{"tier1":["北京","上海","广州","深圳"],"tier2":["杭州","成都","武汉","南京"],"note":"用户确认的城市分级"}',
     "s_demo_pipeline_001", 1.0),
]

# ── 知识候选 ──────────────────────────────────────────────────────────────────

KNOWLEDGE_CANDIDATES = [
    ("web_ui", "", "threshold", "high_value_user_threshold",
     '{"amount":10000,"period":"year","note":"年消费超过 1 万为高价值用户"}',
     "human", 1.0, "org", "confirmed", "ou_admin001"),
    ("conversation", "session:s_demo_clarify_001", "rule", "revenue_default_scope",
     '{"scope":"paid+completed","note":"从对话中提取的销售额口径偏好"}',
     "llm", 0.9, "org", "confirmed", "ou_zhaowei"),
    ("document", "docs/business_rules.md", "rule", "order_completion_criteria",
     '{"status":["completed"],"cooling_period_days":7,"note":"订单完成判定规则"}',
     "llm", 0.85, "org", "confirmed", "ou_admin001"),
    ("web_ui", "", "calendar", "team_monthly_review",
     '{"day_of_month":5,"note":"每月 5 号团队数据复盘"}',
     "human", 1.0, "team:analytics", "confirmed", "ou_zhaowei"),
    ("conversation", "session:s_demo_error_001", "fact", "no_province_field",
     '{"note":"数据库中没有省份字段，只有城市(city)，需要额外映射"}',
     "llm", 0.95, "org", "pending", ""),
    ("web_ui", "", "benchmark", "conversion_rate_benchmark",
     '{"value":0.032,"industry":"电商","metric":"首单转化率","year":2025}',
     "human", 0.9, "org", "pending", ""),
    ("document", "docs/kpi_definitions.md", "threshold", "dau_alert",
     '{"min_dau":5000,"note":"DAU 低于 5000 时告警"}',
     "llm", 0.8, "org", "pending", ""),
    ("conversation", "session:s_demo_query_001", "fact", "top_cities_order_volume",
     '{"cities":["北京","上海","广州"],"note":"订单量 TOP3 城市"}',
     "llm", 0.7, "org", "rejected", "ou_zhaowei"),
    ("web_ui", "", "rule", "test_data_filter",
     '{"note":"统计时需排除 user_id 以 test_ 开头的测试账号"}',
     "human", 1.0, "org", "confirmed", "ou_admin001"),
]

KNOWLEDGE_SOURCES = [
    ("rss", "36氪数据行业动态", '{"url":"https://36kr.com/feed","keywords":["数据","BI","数仓"]}', True),
    ("rss", "InfoQ 数据工程", '{"url":"https://www.infoq.cn/feed","keywords":["data engineering","lakehouse"]}', True),
    ("web_search", "行业报告搜索", '{"keywords":["电商数据分析报告 2026"],"schedule":"weekly"}', True),
    ("url_fetch", "公司内部 Wiki", '{"url":"https://wiki.internal/data-dictionary","auth":"token:xxx"}', False),
    ("rss", "Hacker News 数据", '{"url":"https://hnrss.org/newest?q=data+warehouse","keywords":["warehouse"]}', False),
]


# ── 写入函数 ──────────────────────────────────────────────────────────────────

def _ts(days_ago: float = 0, hours_ago: float = 0) -> str:
    dt = NOW - timedelta(days=days_ago, hours=hours_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def seed_memory_db(clean: bool = False):
    """写入 memory.db（EMS/SMP/tenant/knowledge）。"""
    from agent.db import get_engine
    engine = get_engine()

    with engine.connect() as conn:
        # 建表
        for ddl_module in ["agent.memory.ems", "agent.memory.smp", "agent.tenant", "agent.knowledge"]:
            mod = __import__(ddl_module, fromlist=["_DDL"])
            for stmt in mod._DDL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    try:
                        conn.execute(text(stmt))
                    except Exception:
                        pass
            conn.commit()

        if clean:
            for tbl in ["memory_ems", "memory_smp", "tenant_users", "tenant_teams",
                         "team_table_acl", "knowledge_candidates", "knowledge_sources"]:
                conn.execute(text(f"DELETE FROM {tbl}"))
            conn.commit()
            print("  [clean] 已清空 memory 数据库")

        # ── 团队 ──
        for team_id, display_name in TEAMS:
            conn.execute(text(
                "INSERT OR IGNORE INTO tenant_teams (team_id, display_name, created_at) VALUES (:tid, :dn, :ca)"
            ), {"tid": team_id, "dn": display_name, "ca": _ts(days_ago=60)})

        # ── 用户 ──
        for uid, name, tid, role in USERS:
            days = random.randint(5, 60)
            conn.execute(text(
                "INSERT OR IGNORE INTO tenant_users (user_id, team_id, display_name, role, created_at, updated_at) "
                "VALUES (:uid, :tid, :name, :role, :ca, :ua)"
            ), {"uid": uid, "tid": tid, "name": name, "role": role,
                "ca": _ts(days_ago=days), "ua": _ts(days_ago=random.randint(0, days))})

        # ── ACL ──
        for team_id, tables in TEAM_ACL.items():
            if tables is None:
                continue
            for tbl in tables:
                conn.execute(text(
                    "INSERT OR IGNORE INTO team_table_acl (team_id, table_name) VALUES (:tid, :tn)"
                ), {"tid": team_id, "tn": tbl})

        # ── EMS 会话 ──
        for scenario in SESSION_SCENARIOS:
            sid = scenario["session_id"]
            uid = scenario["user_id"]
            for seq, (role, content, tool_name, tool_input, tool_output, action) in enumerate(scenario["messages"], 1):
                conn.execute(text(
                    "INSERT OR IGNORE INTO memory_ems "
                    "(session_id, user_id, seq, role, content, tool_name, tool_input, tool_output, action, created_at) "
                    "VALUES (:sid, :uid, :seq, :role, :content, :tn, :ti, :to, :action, :ca)"
                ), {"sid": sid, "uid": uid, "seq": seq, "role": role, "content": content or "",
                    "tn": tool_name, "ti": tool_input, "to": tool_output, "action": action,
                    "ca": _ts(days_ago=random.uniform(0, 7), hours_ago=random.uniform(0, 12))})

        # ── SMP ──
        for scope, uid, cat, key, value, sessions, conf in SMP_RECORDS:
            conn.execute(text(
                "INSERT OR IGNORE INTO memory_smp "
                "(scope, user_id, category, key, value, source_sessions, confidence, created_at, updated_at) "
                "VALUES (:scope, :uid, :cat, :key, :val, :sess, :conf, :ca, :ua)"
            ), {"scope": scope, "uid": uid, "cat": cat, "key": key, "val": value,
                "sess": sessions, "conf": conf,
                "ca": _ts(days_ago=random.randint(1, 30)),
                "ua": _ts(days_ago=random.uniform(0, 5))})

        # ── Knowledge candidates ──
        for src, url, cat, key, val, ext_by, conf, scope, status, reviewer in KNOWLEDGE_CANDIDATES:
            days = random.randint(1, 30)
            conn.execute(text(
                "INSERT INTO knowledge_candidates "
                "(source, source_url, category, key, value, extracted_by, confidence, scope, status, reviewed_by, created_at, reviewed_at) "
                "VALUES (:src, :url, :cat, :key, :val, :ext, :conf, :scope, :status, :rev, :ca, :ra)"
            ), {"src": src, "url": url, "cat": cat, "key": key, "val": val,
                "ext": ext_by, "conf": conf, "scope": scope, "status": status, "rev": reviewer,
                "ca": _ts(days_ago=days),
                "ra": _ts(days_ago=days - 1) if status != "pending" else ""})

        # ── Knowledge sources ──
        for ktype, name, config, enabled in KNOWLEDGE_SOURCES:
            conn.execute(text(
                "INSERT INTO knowledge_sources (type, name, config, enabled, last_run, created_at) "
                "VALUES (:type, :name, :config, :enabled, :lr, :ca)"
            ), {"type": ktype, "name": name, "config": config, "enabled": enabled,
                "lr": _ts(days_ago=random.randint(0, 3)) if enabled else "",
                "ca": _ts(days_ago=random.randint(10, 60))})

        conn.commit()
    print("  [memory] 团队/用户/EMS/SMP/知识 数据已写入")


async def seed_audit_db(clean: bool = False):
    """写入 audit_log（forge_audit.db）。"""
    from agent.audit import DB_PATH, _DDL

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_DDL)

        if clean:
            await db.execute("DELETE FROM audit_log")
            print("  [clean] 已清空 audit_log")

        # 从场景数据生成基础记录
        records = []
        for i, (msg, sql, fj, status) in enumerate(QUERY_SCENARIOS):
            uid = random.choice([u[0] for u in USERS[:12]])
            ts = _ts(days_ago=random.uniform(0, 14), hours_ago=random.uniform(0, 23))
            err = None
            if status == "error":
                err = random.choice([
                    "⚠ 查询生成失败（已重试 2 次）：表 province 不存在",
                    "⚠ LLM 返回格式错误，无法解析 Forge JSON",
                    "⚠ 编译失败：HAVING 子句中引用了未聚合的列",
                ])
            records.append((ts, uid, msg, json.dumps(fj, ensure_ascii=False) if fj else None,
                            sql, status, err))

        # ── 带真实 SQL 的额外查询 ──
        EXTRA_APPROVED = [
            ("今天有多少新用户注册",
             "SELECT COUNT(*) AS new_users FROM users WHERE DATE(created_at) = DATE('now')",
             {"mode": "query", "from": "users", "select": [{"agg": "count", "field": "*", "alias": "new_users"}],
              "filters": [{"field": "created_at", "op": ">=", "value": "today"}]}),
            ("最畅销的前10个商品",
             "SELECT p.name AS product_name, SUM(oi.quantity) AS total_sold\nFROM order_items oi\nJOIN products p ON oi.product_id = p.id\nGROUP BY p.name\nORDER BY total_sold DESC\nLIMIT 10",
             {"mode": "query", "from": "order_items", "joins": [{"table": "products", "on": "order_items.product_id = products.id"}],
              "limit": 10}),
            ("各支付方式的占比",
             "SELECT payment_method,\n       COUNT(*) AS cnt,\n       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct\nFROM orders\nWHERE status IN ('paid', 'completed')\nGROUP BY payment_method\nORDER BY cnt DESC",
             {"mode": "query", "from": "orders", "select": [{"field": "payment_method"}, {"agg": "count", "field": "*"}],
              "window": [{"function": "SUM", "over": {}}]}),
            ("活跃用户的订单频次分布",
             "SELECT order_count, COUNT(*) AS user_count\nFROM (\n  SELECT user_id, COUNT(*) AS order_count\n  FROM orders\n  WHERE created_at >= DATE('now', '-90 days')\n  GROUP BY user_id\n) t\nGROUP BY order_count\nORDER BY order_count",
             {"mode": "query", "from": "orders", "ctes": [{"name": "user_freq"}]}),
            ("各品类毛利率",
             "SELECT p.category,\n       ROUND(SUM(oi.unit_price * oi.quantity - p.cost_price * oi.quantity) / SUM(oi.unit_price * oi.quantity) * 100, 2) AS gross_margin_pct\nFROM order_items oi\nJOIN products p ON oi.product_id = p.id\nGROUP BY p.category\nORDER BY gross_margin_pct DESC",
             {"mode": "query", "from": "order_items", "joins": [{"table": "products"}]}),
            ("新客 vs 老客的客单价对比",
             "WITH user_first AS (\n  SELECT user_id, MIN(DATE(created_at)) AS first_order_date\n  FROM orders GROUP BY user_id\n)\nSELECT CASE WHEN o.created_at <= uf.first_order_date THEN '新客' ELSE '老客' END AS user_type,\n       ROUND(AVG(o.total_amount), 2) AS avg_order_value,\n       COUNT(*) AS order_count\nFROM orders o\nJOIN user_first uf ON o.user_id = uf.user_id\nGROUP BY user_type",
             {"mode": "query", "from": "orders", "ctes": [{"name": "user_first"}]}),
            ("周末 vs 工作日订单量",
             "SELECT CASE WHEN CAST(strftime('%w', created_at) AS INTEGER) IN (0, 6) THEN '周末' ELSE '工作日' END AS day_type,\n       COUNT(*) AS order_count,\n       ROUND(AVG(total_amount), 2) AS avg_amount\nFROM orders\nWHERE created_at >= DATE('now', '-30 days')\nGROUP BY day_type",
             {"mode": "query", "from": "orders"}),
            ("会员等级分布",
             "SELECT CASE\n         WHEN total_spent >= 10000 THEN '钻石'\n         WHEN total_spent >= 5000 THEN '金牌'\n         WHEN total_spent >= 1000 THEN '银牌'\n         ELSE '普通'\n       END AS tier,\n       COUNT(*) AS user_count\nFROM (\n  SELECT user_id, SUM(total_amount) AS total_spent\n  FROM orders WHERE status = 'completed'\n  GROUP BY user_id\n) t\nGROUP BY tier\nORDER BY user_count DESC",
             {"mode": "query", "from": "orders", "ctes": [{"name": "user_spending"}]}),
            ("退货原因分析",
             "SELECT refund_reason,\n       COUNT(*) AS cnt,\n       ROUND(SUM(refund_amount), 2) AS total_refund\nFROM refunds\nWHERE created_at >= DATE('now', '-30 days')\nGROUP BY refund_reason\nORDER BY cnt DESC",
             {"mode": "query", "from": "refunds"}),
            ("各仓库的发货量",
             "SELECT w.warehouse_name,\n       COUNT(s.id) AS shipment_count,\n       ROUND(AVG(JULIANDAY(s.delivered_at) - JULIANDAY(s.shipped_at)), 1) AS avg_delivery_days\nFROM shipments s\nJOIN warehouses w ON s.warehouse_id = w.id\nWHERE s.shipped_at >= DATE('now', '-30 days')\nGROUP BY w.warehouse_name\nORDER BY shipment_count DESC",
             {"mode": "query", "from": "shipments", "joins": [{"table": "warehouses"}]}),
            ("页面转化漏斗",
             "SELECT '浏览' AS stage, COUNT(DISTINCT user_id) AS users FROM page_views WHERE event = 'view'\nUNION ALL\nSELECT '加购', COUNT(DISTINCT user_id) FROM page_views WHERE event = 'add_cart'\nUNION ALL\nSELECT '下单', COUNT(DISTINCT user_id) FROM page_views WHERE event = 'checkout'\nUNION ALL\nSELECT '支付', COUNT(DISTINCT user_id) FROM page_views WHERE event = 'payment'",
             {"mode": "query", "from": "page_views"}),
            ("搜索关键词 TOP20",
             "SELECT keyword,\n       COUNT(*) AS search_count,\n       ROUND(SUM(CASE WHEN has_result = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS hit_rate_pct\nFROM search_logs\nWHERE created_at >= DATE('now', '-7 days')\nGROUP BY keyword\nORDER BY search_count DESC\nLIMIT 20",
             {"mode": "query", "from": "search_logs", "limit": 20}),
            ("各地区的物流时效",
             "SELECT u.city AS region,\n       COUNT(*) AS order_count,\n       ROUND(AVG(JULIANDAY(s.delivered_at) - JULIANDAY(o.created_at)), 1) AS avg_days\nFROM orders o\nJOIN users u ON o.user_id = u.id\nJOIN shipments s ON o.id = s.order_id\nWHERE s.delivered_at IS NOT NULL\nGROUP BY u.city\nORDER BY avg_days",
             {"mode": "query", "from": "orders", "joins": [{"table": "users"}, {"table": "shipments"}]}),
            ("促销活动 ROI",
             "SELECT c.campaign_name,\n       c.budget,\n       SUM(o.total_amount) AS revenue,\n       ROUND(SUM(o.total_amount) / c.budget, 2) AS roi\nFROM campaigns c\nJOIN orders o ON o.campaign_id = c.id AND o.status = 'completed'\nGROUP BY c.campaign_name, c.budget\nORDER BY roi DESC",
             {"mode": "query", "from": "campaigns", "joins": [{"table": "orders"}]}),
            ("用户分层（RFM）",
             "WITH rfm AS (\n  SELECT user_id,\n         JULIANDAY('now') - JULIANDAY(MAX(created_at)) AS recency,\n         COUNT(*) AS frequency,\n         SUM(total_amount) AS monetary\n  FROM orders WHERE status = 'completed'\n  GROUP BY user_id\n)\nSELECT\n  CASE WHEN recency <= 30 AND frequency >= 5 AND monetary >= 5000 THEN '高价值'\n       WHEN recency <= 60 AND frequency >= 2 THEN '成长型'\n       WHEN recency > 90 THEN '流失风险'\n       ELSE '普通' END AS segment,\n  COUNT(*) AS user_count\nFROM rfm\nGROUP BY segment",
             {"mode": "query", "from": "orders", "ctes": [{"name": "rfm"}]}),
            ("库存周转率",
             "SELECT p.category,\n       ROUND(SUM(oi.quantity) * 1.0 / AVG(inv.stock_qty), 2) AS turnover_rate\nFROM order_items oi\nJOIN products p ON oi.product_id = p.id\nJOIN inventory inv ON p.id = inv.product_id\nWHERE oi.created_at >= DATE('now', '-90 days')\nGROUP BY p.category\nORDER BY turnover_rate DESC",
             {"mode": "query", "from": "order_items", "joins": [{"table": "products"}, {"table": "inventory"}]}),
            ("客服满意度评分",
             "SELECT agent_name,\n       COUNT(*) AS ticket_count,\n       ROUND(AVG(satisfaction_score), 2) AS avg_score,\n       ROUND(SUM(CASE WHEN satisfaction_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS good_rate_pct\nFROM support_tickets\nWHERE resolved_at >= DATE('now', '-30 days')\nGROUP BY agent_name\nORDER BY avg_score DESC",
             {"mode": "query", "from": "support_tickets"}),
            ("退款处理时效",
             "SELECT DATE(created_at) AS dt,\n       COUNT(*) AS refund_count,\n       ROUND(AVG(JULIANDAY(processed_at) - JULIANDAY(created_at)) * 24, 1) AS avg_hours\nFROM refunds\nWHERE created_at >= DATE('now', '-14 days')\nGROUP BY DATE(created_at)\nORDER BY dt",
             {"mode": "query", "from": "refunds"}),
            ("平均配送时长",
             "SELECT ROUND(AVG(JULIANDAY(delivered_at) - JULIANDAY(shipped_at)), 2) AS avg_delivery_days,\n       ROUND(AVG(JULIANDAY(shipped_at) - JULIANDAY(o.created_at)), 2) AS avg_processing_days\nFROM shipments s\nJOIN orders o ON s.order_id = o.id\nWHERE s.delivered_at IS NOT NULL AND s.shipped_at >= DATE('now', '-30 days')",
             {"mode": "query", "from": "shipments", "joins": [{"table": "orders"}]}),
            ("用户性别分布",
             "SELECT gender,\n       COUNT(*) AS user_count,\n       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct\nFROM users\nWHERE gender IS NOT NULL\nGROUP BY gender\nORDER BY user_count DESC",
             {"mode": "query", "from": "users", "window": [{"function": "SUM"}]}),
        ]
        for msg, sql_text, fj in EXTRA_APPROVED:
            uid = random.choice([u[0] for u in USERS[:12]])
            ts = _ts(days_ago=random.uniform(0, 30), hours_ago=random.uniform(0, 23))
            records.append((ts, uid, msg, json.dumps(fj, ensure_ascii=False), sql_text, "approved", None))

        # ── 带真实 SQL 的 pending 查询 ──
        EXTRA_PENDING = [
            ("对比一下Q1和Q2的GMV",
             "WITH quarterly AS (\n  SELECT CASE\n           WHEN CAST(strftime('%m', created_at) AS INT) BETWEEN 1 AND 3 THEN 'Q1'\n           WHEN CAST(strftime('%m', created_at) AS INT) BETWEEN 4 AND 6 THEN 'Q2'\n         END AS quarter,\n         SUM(total_amount) AS gmv\n  FROM orders\n  WHERE status = 'completed'\n    AND strftime('%Y', created_at) = '2026'\n  GROUP BY quarter\n)\nSELECT * FROM quarterly WHERE quarter IS NOT NULL",
             {"mode": "query", "from": "orders", "ctes": [{"name": "quarterly"}]}),
            ("查一下上周各品类的销量",
             "SELECT p.category, SUM(oi.quantity) AS total_qty\nFROM order_items oi\nJOIN products p ON oi.product_id = p.id\nJOIN orders o ON oi.order_id = o.id\nWHERE o.created_at >= DATE('now', '-7 days')\nGROUP BY p.category\nORDER BY total_qty DESC",
             {"mode": "query", "from": "order_items", "joins": [{"table": "products"}, {"table": "orders"}]}),
            ("统计一下昨天的GMV",
             "SELECT SUM(total_amount) AS gmv\nFROM orders\nWHERE DATE(created_at) = DATE('now', '-1 day')\n  AND status IN ('paid', 'completed')",
             {"mode": "query", "from": "orders"}),
            ("哪个渠道效果最好",
             "SELECT channel,\n       COUNT(DISTINCT user_id) AS users,\n       SUM(total_amount) AS revenue,\n       ROUND(AVG(total_amount), 2) AS avg_order\nFROM orders o\nJOIN users u ON o.user_id = u.id\nWHERE o.created_at >= DATE('now', '-30 days')\nGROUP BY channel\nORDER BY revenue DESC",
             {"mode": "query", "from": "orders", "joins": [{"table": "users"}]}),
            ("用户增长怎么样",
             "SELECT DATE(created_at) AS dt,\n       COUNT(*) AS new_users,\n       SUM(COUNT(*)) OVER (ORDER BY DATE(created_at)) AS cumulative_users\nFROM users\nWHERE created_at >= DATE('now', '-30 days')\nGROUP BY DATE(created_at)\nORDER BY dt",
             {"mode": "query", "from": "users", "window": [{"function": "SUM", "over": {"order_by": "dt"}}]}),
        ]
        for msg, sql_text, fj in EXTRA_PENDING:
            uid = random.choice([u[0] for u in USERS])
            ts = _ts(days_ago=random.uniform(0, 5), hours_ago=random.uniform(0, 12))
            records.append((ts, uid, msg, json.dumps(fj, ensure_ascii=False), sql_text, "pending", None))

        # ── 已取消 ──
        EXTRA_CANCELLED = [
            ("帮我看看这个月的报表", "SELECT * FROM monthly_report WHERE month = '2026-03'", None),
            ("导出全部用户手机号", "SELECT name, phone FROM users", None),
            ("删除7天前的日志数据", None, None),
            ("把这个 SQL 帮我优化一下", None, None),
        ]
        for msg, sql_text, _ in EXTRA_CANCELLED:
            uid = random.choice([u[0] for u in USERS])
            ts = _ts(days_ago=random.uniform(0, 20), hours_ago=random.uniform(0, 23))
            fj_str = json.dumps({"mode": "query"}, ensure_ascii=False) if sql_text else None
            records.append((ts, uid, msg, fj_str, sql_text, "cancelled", None))

        # ── 错误场景（真实错误信息）──
        EXTRA_ERRORS = [
            ("统计每个省份的人均消费",
             "⚠ 查询生成失败（已重试 2 次）：表 users 中没有 province 字段。可用的地理字段为 city。建议使用城市维度或先在 Registry 中配置省份映射。"),
            ("用户的 LTV 是多少",
             "⚠ 查询生成失败：LTV（用户终身价值）需要多个复杂计算步骤（获客成本、留存曲线、ARPU），超出当前 DSL 的表达能力。建议先定义 LTV 的原子指标再组合。"),
            ("分析用户画像",
             "⚠ 「用户画像」过于宽泛，无法生成具体查询。请明确分析维度，例如：年龄分布、消费层级、地域分布、活跃度等。"),
            ("帮我跑个 A/B 测试分析",
             "⚠ 查询生成失败：数据库中没有实验分组表（experiment_assignments）。A/B 测试分析需要先接入实验平台数据。"),
            ("预测下个月的销量",
             "⚠ Forge 是查询工具，不支持预测建模。请使用专门的预测服务或 Python 脚本进行时间序列预测。"),
            ("把订单表和日志表做个 CROSS JOIN",
             "⚠ 编译失败（已重试 2 次）：CROSS JOIN orders × log_events 将产生约 5 亿行结果集，已超出安全阈值。请添加过滤条件缩小范围。"),
            ("用递归 CTE 算出组织树层级",
             "⚠ 编译失败：当前 DSL 不支持递归 CTE（WITH RECURSIVE）。这属于算法逻辑类查询，超出 Forge 能力边界。"),
            ("[手动编辑 SQL]",
             "⚠ 执行失败：(sqlite3.OperationalError) near \"SELEC\": syntax error — SQL 语法错误，请检查拼写。"),
        ]
        for msg, err_msg in EXTRA_ERRORS:
            uid = random.choice([u[0] for u in USERS])
            ts = _ts(days_ago=random.uniform(0, 30), hours_ago=random.uniform(0, 23))
            records.append((ts, uid, msg, None, None, "error", err_msg))

        # ── 手动编辑 SQL 场景 ──
        MANUAL_SQLS = [
            ("[手动编辑 SQL]",
             "SELECT u.city, COUNT(*) AS cnt, SUM(o.total_amount) AS rev\nFROM orders o\nJOIN users u ON o.user_id = u.id\nWHERE o.created_at >= '2026-01-01'\nGROUP BY u.city\nHAVING COUNT(*) >= 10\nORDER BY rev DESC"),
            ("[手动编辑 SQL]",
             "SELECT DATE(created_at) AS dt,\n       COUNT(*) FILTER (WHERE status = 'completed') AS completed,\n       COUNT(*) FILTER (WHERE status = 'refunded') AS refunded\nFROM orders\nWHERE created_at >= DATE('now', '-14 days')\nGROUP BY DATE(created_at)\nORDER BY dt"),
            ("[手动编辑 SQL]",
             "UPDATE orders SET status = 'completed' WHERE id = 12345 -- 测试直接执行 DML"),
        ]
        for msg, sql_text in MANUAL_SQLS:
            uid = random.choice(["web_user_demo", "ou_zhaowei", "ou_admin001"])
            ts = _ts(days_ago=random.uniform(0, 7), hours_ago=random.uniform(0, 12))
            records.append((ts, uid, msg, None, sql_text, "approved", None))

        for ts, uid, msg, fj, sql, status, err in records:
            await db.execute(
                "INSERT INTO audit_log (timestamp, user_id, user_message, forge_json, sql, status, error_message) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (ts, uid, msg, fj, sql, status, err),
            )
        await db.commit()
    print(f"  [audit] {len(records)} 条审计记录已写入")


# ── 主入口 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Forge Mock 数据填充")
    parser.add_argument("--clean", action="store_true", help="清空后重新填充")
    args = parser.parse_args()

    print("Forge Mock Data Seeder")
    print("=" * 40)

    seed_memory_db(clean=args.clean)
    asyncio.run(seed_audit_db(clean=args.clean))

    print("=" * 40)
    print("完成！刷新 Web 页面查看效果。")


if __name__ == "__main__":
    main()
