"""Method B_large — 直接生成 SQL（大 Schema 对照组）

与 Method K 使用同一套 14 张电商数仓表和 40 道测试题，
但不经过 Forge DSL 编译，直接让模型输出 SQLite SQL。
用于与 Method K 做公平对比。
"""

METHOD_ID = "b_large"
LABEL = "Method SQL-Large（大Schema直接生成 SQL，对照组）"
MODE = "sql"
RUNS = 3
NOTES = "2026-03-15 大Schema对照组，与Method K同题同表，直接输出SQL"

_SCHEMA = """
你可以查询以下电商数仓表（SQLite，large_demo.db）：

# 用户域
dim_user(user_id, user_name, gender, age_group, vip_level_id, region_id, channel_id, register_date, is_active)
  gender: 'male'|'female'|'unknown'
  age_group: '18-24'|'25-34'|'35-44'|'45-54'|'55+'
  is_active: 0|1

dim_vip_level(vip_level_id, level_name, min_points, discount_rate, free_shipping)
  level_name: '普通'|'银卡'|'金卡'|'铂金'|'钻石'

# 订单域
dwd_order_detail(order_id, user_id, merchant_id, channel_id, platform_id, promotion_id, order_status, total_amount, discount_amount, coupon_amount, freight_amount, pay_amount, order_dt)
  order_status: '待付款'|'待发货'|'待收货'|'已完成'|'已取消'

dwd_order_item_detail(order_item_id, order_id, product_id, user_id, quantity, unit_price, discount_rate, actual_amount, is_gift, order_dt)
  is_gift: 0|1

dwd_payment_detail(payment_id, order_id, user_id, payment_method_id, pay_amount, pay_status, pay_dt)
  pay_status: '成功'|'失败'|'超时'|'撤销'

dwd_refund_detail(refund_id, order_id, user_id, after_sale_type_id, refund_amount, refund_status, apply_dt, complete_dt, reason_id)
  refund_status: '申请中'|'审核通过'|'退款中'|'已退款'|'拒绝'

dwd_cart_detail(cart_id, user_id, product_id, action_type, quantity, action_dt, platform_id)
  action_type: 'add'|'remove'|'update_qty'|'checkout'

# 商品域
dim_product(product_id, product_name, category_id, brand_id, supplier_id, unit_price, cost_price, status, is_imported)
  status: 'on_sale'|'off_shelf'|'pre_sale'|'discontinued'
  is_imported: 0|1

dim_category(category_id, category_name, parent_id, level, is_leaf)

dim_brand(brand_id, brand_name, country, is_authorized, brand_level)
  brand_level: '国际'|'国内知名'|'新兴'|'白牌'

# 评价与售后
dwd_comment_detail(comment_id, order_item_id, user_id, product_id, rating, comment_type, has_image, has_video, comment_dt)
  rating: 1|2|3|4|5
  comment_type: '好评'|'中评'|'差评'

# 地域与渠道
dim_region(region_id, region_name, parent_id, level, tier)
  tier: '一线'|'新一线'|'二线'|'三线'|'其他'

dim_channel(channel_id, channel_name, channel_type, platform_id, cost_per_click)
  channel_type: 'organic'|'paid_search'|'social'|'email'|'affiliate'

关键 JOIN 路径：
dwd_order_detail.user_id → dim_user.user_id
dwd_order_detail.channel_id → dim_channel.channel_id
dim_user.vip_level_id → dim_vip_level.vip_level_id
dwd_order_item_detail.order_id → dwd_order_detail.order_id
dwd_order_item_detail.product_id → dim_product.product_id
dwd_order_item_detail.user_id → dim_user.user_id
dim_product.category_id → dim_category.category_id
dim_product.brand_id → dim_brand.brand_id
dwd_refund_detail.order_id → dwd_order_detail.order_id
dwd_cart_detail.user_id → dim_user.user_id
dwd_comment_detail.product_id → dim_product.product_id
"""

SYSTEM_PROMPT = f"""你是一个专业的数据查询助手，擅长编写 SQLite SQL 查询。

{_SCHEMA}

用户会描述一个数据查询需求，你需要输出可以在 SQLite 上执行的正确 SQL。
只输出 SQL 语句，不要任何解释，不要 markdown 代码块。"""
