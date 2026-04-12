import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_product_shelf"
TOTAL_COUNT = 15000  # 电商直播间上架频率高
BATCH_SIZE = 600
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
PRODUCT_IDS = [f"p_50000{i}" for i in range(1, 1001)]  # 1000个商品
SHELF_STATUSES = ["up", "down"]  # 上架/下架
ANCHOR_REMARKS = [
    "今日主推，限时优惠", "库存有限，先到先得", "新品上架，福利价", "", "专属折扣，仅限直播间"
]


def generate_product_shelf_log(seq):
    room_id = random.choice(ROOM_IDS)
    product_id = random.choice(PRODUCT_IDS)
    shelf_status = random.choices(SHELF_STATUSES, weights=[0.8, 0.2], k=1)[0]  # 80%上架
    timestamp = 1735603200000 + seq * 60000  # 每1分钟1条（60000ms）
    anchor_remark = random.choice(ANCHOR_REMARKS)
    sort_num = random.randint(1, 10)  # 排序1-10

    # 电商分类直播间上架频率更高（强制上架）
    if room_id in [f"r_20000{i}" for i in range(31, 60)]:  # 30个电商直播间
        shelf_status = "up"
        anchor_remark = random.choice(ANCHOR_REMARKS[:-1])  # 非空备注

    # 1.5%概率生成无效商品ID
    if random.random() < 0.015:
        product_id = "p_invalid"

    return {
        "room_id": room_id,
        "product_id": product_id,
        "shelf_status": shelf_status,
        "shelf_time": timestamp,
        "anchor_remark": anchor_remark,
        "sort_num": sort_num
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
        batch_logs = [generate_product_shelf_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()