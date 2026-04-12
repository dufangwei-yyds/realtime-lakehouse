import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_order"
TOTAL_COUNT = 25000  # 订单量低于行为日志，高于礼物日志
BATCH_SIZE = 800
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
USER_IDS = [f"u_{100000 + i}" for i in range(1, 5001)]
PRODUCT_IDS = [f"p_50000{i}" for i in range(1, 1001)]
PAY_STATUSES = ["unpaid", "paid", "refunded"]


def generate_order_log(seq):
    order_id = f"o_70000{seq}"
    user_id = random.choice(USER_IDS)
    room_id = random.choice(ROOM_IDS)
    product_id = random.choice(PRODUCT_IDS)
    product_count = random.randint(1, 10)
    pay_status = random.choices(PAY_STATUSES, weights=[0.2, 0.75, 0.05], k=1)[0]  # 75%已支付

    # 模拟商品单价（从商品表逻辑复用：5-5000元）
    product_price = round(5 + random.random() * 4995, 2)
    # product_price = round(5 + RANDOM() * 4995, 2)
    total_amount = round(product_price * product_count, 2)

    # 时序关系：下单时间 < 支付时间（未支付则无支付时间）
    create_time = 1735603200000 + seq
    pay_time = create_time + random.randint(1000, 60000) if pay_status == "paid" else None

    # 电商直播间订单量更高（强制已支付）
    if room_id in [f"r_20000{i}" for i in range(31, 60)]:
        pay_status = "paid"
        pay_time = create_time + random.randint(1000, 60000)

    # 2%概率生成异常金额（负数）
    if random.random() < 0.02:
        total_amount = -round(product_price * product_count, 2)

    return {
        "order_id": order_id,
        "user_id": user_id,
        "room_id": room_id,
        "product_id": product_id,
        "product_count": product_count,
        "total_amount": total_amount,
        "pay_status": pay_status,
        "pay_time": pay_time,
        "create_time": create_time
    }


def send_to_kafka(producer, logs):
    for log in logs:
        try:
            producer.send(
                topic=TOPIC,
                key=log["room_id"].encode("utf-8"),
                value=json.dumps(log).encode("utf-8")
            )
        except KafkaError as e:
            print(f"发送失败: {e}, 日志: {log}")
    producer.flush()


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        linger_ms=500,
        compression_type="snappy"
    )

    start_time = time.time()
    for i in range(0, TOTAL_COUNT, BATCH_SIZE):
        batch_logs = [generate_order_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()