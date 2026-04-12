# generate_live_data.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker("zh_CN")
producer = KafkaProducer(
    bootstrap_servers=['192.168.63.128:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# 预定义主播 ID（用于关联 anchor_dim）
anchor_ids = [f"anchor_{i}" for i in range(1, 4)]  # anchor_1, anchor_2, anchor_3

# 商品 ID 列表（需与 simulate_cdc.py 中 goods_dim 一致）
goods_ids = ["g1", "g2", "g3"]

# 直播间类目
categories = ["美妆", "服饰", "食品", "数码", "家居"]

print("🚀 开始生成直播间开播事件（作为 live_room_dim 维表源）...")
# 模拟 5 个直播间开播（每个 room_id 唯一，携带维度属性）
for i in range(5):
    room_id = f"room_{int(time.time())}_{random.randint(1001, 1005)}"
    anchor_id = random.choice(anchor_ids)
    event = {
        "room_id": room_id,
        "anchor_id": anchor_id,
        "title": fake.sentence(nb_words=6)[:20].rstrip("."),
        "category": random.choice(categories),
        "is_brand_event": random.choice([True, False]),
        "start_time": datetime.now().isoformat(),
        "event_time": int(time.time() * 1000)  # Unix ms
    }
    producer.send("live_room_start", event)
    print(f"  📺 发送直播间开播事件: {event}")
    time.sleep(1)

print("\n🛒 开始生成订单与支付事件...")
# 模拟 100 笔订单 + 支付（支付可能延迟）
for i in range(100):
    # 随机选择一个已开播的 room_id（简化：从最近生成的 room_id 中选）
    # 实际中应从活跃直播间列表获取，此处用时间戳模拟
    room_id = f"room_{int(time.time() - random.randint(0, 300))}_{random.randint(1001, 1005)}"
    anchor_id = random.choice(anchor_ids)
    goods_id = random.choice(goods_ids)
    user_id = f"user_{random.randint(10000, 99999)}"
    order_id = str(fake.uuid4())

    # 下单事件
    order_event = {
        "order_id": order_id,
        "room_id": room_id,
        "user_id": user_id,
        "goods_id": goods_id,
        "create_time": datetime.now().isoformat(),
        "event_time": int(time.time() * 1000)
    }
    producer.send("order_create", order_event)
    print(f"  🛍️ 发送订单事件: order_id={order_id}, room={room_id}")

    # 支付事件：70% 立即支付，30% 延迟 5~15 秒（模拟真实场景）
    if random.random() < 0.3:
        delay_sec = random.randint(5, 15)
        print(f"     ⏳ 订单 {order_id} 将在 {delay_sec} 秒后支付...")
        time.sleep(delay_sec)

    pay_event = {
        "order_id": order_id,
        "room_id": room_id,
        "pay_time": datetime.now().isoformat(),
        "status": "success",
        "event_time": int(time.time() * 1000)
    }
    producer.send("payment_success", pay_event)
    print(f"  💳 发送支付成功事件: {order_id}")

    # 控制发送节奏
    time.sleep(0.3)

producer.flush()
print("\n✅ 所有测试数据已发送至 Kafka！")
print("\n📌 提示：")
print("  - 商品维表 (goods_dim) 和主播维表 (anchor_dim) 请运行 simulate_cdc.py 初始化")
print("  - 用户维表 (user_dim) 可按需扩展")
