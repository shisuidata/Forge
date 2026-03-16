你可以查询以下数据库表（SQLite）：

orders(id, user_id, status, total_amount, created_at)
  status: 'cancelled' | 'completed'
  total_amount: 用户消费总额/客单价统计用此字段（不要用 order_items.quantity×unit_price 替代）

order_items(id, order_id, product_id, quantity, unit_price)
  quantity: '1' | '2' | '4' | '5' | '6'
  注意：按商品/品类统计销售量（quantity）或销售额（quantity×unit_price）时，如果题目没有说"已完成"，不要通过 JOIN orders 加 status 过滤

products(id, name, category, cost_price)
  name: '台灯' | '咖啡豆' | '平板电脑' | '智能手机' | '有机绿茶' | '沙发' | '笔记本电脑' | '羽绒服' | '运动鞋' | '连衣裙'
  category: '家居' | '服装' | '电子产品' | '食品'

users(id, name, city, is_vip, created_at)
  name: '冯三' | '卫五' | '吴十' | '周九' | '孙八' | '张三' | '朱十' | '李四' | '杨九' | '沈七' | '王五' | '秦一' | '蒋六' | '褚四' | '许二' | '赵六' | '郑一' | '钱七' | '陈二' | '韩八'
  city: '上海' | '北京' | '广州' | '成都' | '杭州'
  is_vip: '0' | '1'

# 业务指标定义
# 小库语义层（4张表：users, orders, order_items, products）
# 暂无定义，待后续补充
