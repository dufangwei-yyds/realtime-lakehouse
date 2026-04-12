import mysql.connector
import random
from datetime import datetime, timedelta

"""
数据集成模块: 业务数据库数据-生成测试数据
注意: 需要提前执行mock_data.sql创建数据库以及相应表的脚本
"""

# 连接数据库
ecommerce_conn = mysql.connector.connect(host='localhost', user='root', password='Dfw920130Q520,', database='ecommerce_db')
member_conn = mysql.connector.connect(host='localhost', user='root', password='Dfw920130Q520,', database='member_db')
erp_conn = mysql.connector.connect(host='localhost', user='root', password='Dfw920130Q520,', database='erp_db')

# 服装行业常量
CATEGORIES = ['连衣裙', '牛仔裤', '上衣', '外套', '休闲裤', '短裤', '短裙']
STYLES = ['休闲', '通勤', '运动', '轻奢', '复古', '街头', '甜美']
SIZES = ['XS', 'S', 'M', 'L', 'XL', 'XXL']
COLORS = ['黑色', '白色', '红色', '蓝色', '灰色', '黄色', '绿色', '紫色']
MATERIALS = ['棉', '麻', '丝', '羊毛', '涤纶', '锦纶', '氨纶']
SEASONS = ['春', '夏', '秋', '冬']
WAREHOUSES = ['总仓', '华北仓', '华东仓', '华南仓', '西南仓']

# 生成随机日期
def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# 生成商品数据
def generate_goods_data(count=1000):
    cursor = erp_conn.cursor()

    # 检查是否存在外键依赖, 如果存在则先清理相关数据
    try:
        # 先检查是否有外键约束
        check_cursor = ecommerce_conn.cursor()
        check_cursor.execute("SELECT COUNT(*) FROM ecommerce_db.orders WHERE sku_id IN (SELECT sku_id FROM erp_db.goods)")
        order_count = check_cursor.fetchone()[0]

        if order_count > 0:
            # 如果存在依赖数据, 先清理订单数据
            print("检测到订单数据依赖，正在清理相关数据...")
            check_cursor.execute("DELETE FROM ecommerce_db.order_status_history")
            check_cursor.execute("DELETE FROM ecommerce_db.orders")
            ecommerce_conn.commit()

        check_cursor.close()

        # 清理库存数据
        cursor.execute("DELETE FROM erp_db.inventory")

        # 清理商品数据
        cursor.execute("DELETE FROM erp_db.goods")
        erp_conn.commit()

    except Exception as e:
        print(f"清理数据时出错: {e}")
        erp_conn.rollback()
        return

    for i in range(count):
        sku_id = f"SKU{i + 1:06d}"
        spu_id = f"SPU{i // 10:03d}"  # 每10个SKU属于同一个SPU
        category = random.choice(CATEGORIES)
        brand = f"品牌{random.randint(1, 20)}"
        material = random.choice(MATERIALS)
        season = random.choice(SEASONS)
        style = random.choice(STYLES)
        size_range = ','.join(random.sample(SIZES, random.randint(2, len(SIZES))))
        color_list = ','.join(random.sample(COLORS, random.randint(1, len(COLORS))))
        price = round(random.uniform(50, 1000), 2)
        cost_price = round(price * random.uniform(0.3, 0.7), 2)
        status = 1
        create_time = random_date(datetime(2024, 1, 1), datetime.now())
        update_time = create_time if random.random() > 0.3 else random_date(create_time, datetime.now())

        sql = """
              INSERT INTO goods (sku_id, spu_id, goods_name, category, brand, material, season, style,
                                 size_range, color_list, price, cost_price, status, create_time, update_time)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) \
              """
        goods_name = f"{brand}{category}{style}{season}款"
        cursor.execute(sql, (sku_id, spu_id, goods_name, category, brand, material, season, style,
                             size_range, color_list, price, cost_price, status, create_time, update_time))

    erp_conn.commit()
    print(f"生成{count}条商品数据完成")

# 生成库存数据
def generate_inventory_data():
    cursor = erp_conn.cursor()

    # 先获取所有SKU
    cursor.execute("SELECT sku_id, size_range FROM goods")
    goods_list = cursor.fetchall()

    # 先清理现有的仓库和库存数据
    cursor.execute("DELETE FROM inventory")
    cursor.execute("DELETE FROM warehouses")

    # 插入仓库数据
    for i, warehouse_name in enumerate(WAREHOUSES):
        sql = "INSERT INTO warehouses (warehouse_id, warehouse_name, region, type) VALUES (%s, %s, %s, %s)"
        cursor.execute(sql, (i + 1, warehouse_name, warehouse_name[:2], 1 if i == 0 else 2))

    # 为每个SKU生成库存
    for sku_id, size_range in goods_list:
        sizes = size_range.split(',')
        for size in sizes:
            for warehouse_id in range(1, len(WAREHOUSES) + 1):
                available_quantity = random.randint(0, 100)
                locked_quantity = random.randint(0, min(10, available_quantity))
                update_time = random_date(datetime(2025, 6, 1), datetime.now())

                sql = """
                      INSERT INTO inventory (sku_id, size, warehouse_id, available_quantity, locked_quantity, \
                                             update_time)
                      VALUES (%s, %s, %s, %s, %s, %s) \
                      """
                cursor.execute(sql, (sku_id, size, warehouse_id, available_quantity, locked_quantity, update_time))

    erp_conn.commit()
    print(f"生成库存数据完成")

# 生成用户数据
def generate_user_data(count=10000):
    cursor = member_conn.cursor()
    generated_phones = set()  # 用于跟踪已生成的手机号

    # 先清理现有用户数据
    cursor.execute("DELETE FROM users")
    member_conn.commit()

    for i in range(count):
        # 添加时间戳确保用户名唯一性
        timestamp = int(datetime.now().timestamp() * 1000000) % 1000000
        username = f"用户{timestamp:06d}{i + 1:03d}"[-10:]  # 保证不超过10位

        # 确保生成唯一的手机号
        while True:
            phone = f"13{random.randint(10000000, 99999999)}"
            # 检查数据库中是否已存在该手机号
            cursor.execute("SELECT COUNT(*) FROM users WHERE phone = %s", (phone,))
            if cursor.fetchone()[0] == 0 and phone not in generated_phones:
                generated_phones.add(phone)
                break

        gender = random.randint(0, 2)
        birth_date = random_date(datetime(1980, 1, 1), datetime(2005, 1, 1))
        register_time = random_date(datetime(2024, 1, 1), datetime.now())
        preferred_style = random.choice(STYLES)
        preferred_size = random.choice(SIZES)
        vip_level = min(5, int(random.gauss(2, 1)))  # 正态分布, 多数用户为2-3级
        is_active = 1 if random.random() > 0.1 else 0  # 90%用户活跃

        sql = """
              INSERT INTO users (username, phone, gender, birth_date, register_time,
                                 preferred_style, preferred_size, vip_level, is_active)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) \
              """
        cursor.execute(sql, (username, phone, gender, birth_date, register_time,
                             preferred_style, preferred_size, vip_level, is_active))

    member_conn.commit()
    print(f"生成{count}条用户数据完成")

# 生成订单数据
def generate_order_data(count=50000):
    cursor = ecommerce_conn.cursor()

    # 获取用户ID列表
    member_cursor = member_conn.cursor()
    member_cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in member_cursor.fetchall()]

    # 获取商品SKU列表
    erp_cursor = erp_conn.cursor()
    erp_cursor.execute("SELECT sku_id, price, size_range, color_list FROM goods WHERE status=1")
    goods_list = erp_cursor.fetchall()

    for i in range(count):
        # 选择用户
        user_id = random.choice(user_ids)

        # 选择单个商品 (避免sku_id字段问题)
        sku_id, price, size_range, color_list = random.choice(goods_list)
        size = random.choice(size_range.split(','))
        color = random.choice(color_list.split(','))
        quantity = random.randint(1, 3)
        total_amount = price * quantity

        # 状态判断和时间生成
        status_distribution = random.random()
        if status_distribution < 0.8:
            order_status = 4
            create_time = random_date(datetime(2025, 1, 1), datetime.now())
            update_time = create_time + timedelta(hours=random.randint(1, 72))
        elif status_distribution < 0.9:
            order_status = 1
            create_time = random_date(datetime(2025, 6, 1), datetime.now())
            update_time = create_time
        else:
            order_status = 5
            create_time = random_date(datetime(2025, 5, 1), datetime.now())
            update_time = create_time + timedelta(minutes=random.randint(1, 30))

        # 一次性插入完整订单数据, 包含所有必需字段
        order_no = f"ORD{datetime.now().strftime('%Y%m%d')}{i:06d}"
        sql = """
              INSERT INTO orders (order_no, user_id, sku_id, size, color, quantity, 
                                 order_amount, order_status, create_time, update_time)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
              """
        cursor.execute(sql, (
            order_no, user_id, sku_id, size, color, quantity,
            total_amount, order_status, create_time, update_time
        ))
        order_id = cursor.lastrowid

        # 记录订单状态变更
        if order_status != 1:
            status_history_sql = """
                                 INSERT INTO order_status_history (order_id, old_status, new_status, change_time)
                                 VALUES (%s, %s, %s, %s)
                                 """
            if order_status == 4:
                cursor.execute(status_history_sql, (order_id, 1, 2, create_time))
                cursor.execute(status_history_sql, (order_id, 2, 3, create_time + timedelta(hours=12)))
                cursor.execute(status_history_sql, (order_id, 3, 4, update_time))
            elif order_status == 5:
                cursor.execute(status_history_sql, (order_id, 1, 5, update_time))

    ecommerce_conn.commit()
    print(f"生成{count}条订单数据完成")

# 运行数据生成
if __name__ == "__main__":
    generate_goods_data(5000)  # 生成5000个商品
    generate_inventory_data()  # 生成库存数据
    generate_user_data(10000)  # 生成10000个用户
    generate_order_data(50000)  # 生成50000个订单