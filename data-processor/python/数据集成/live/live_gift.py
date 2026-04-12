import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]  # 替换为你的Kafka地址
TOPIC = "live_gift"
TOTAL_COUNT = 50000  # 生成5万条日志（礼物行为量低于普通行为）
BATCH_SIZE = 500  # 每批发送500条
GIFT_IDS = [f"g_30000{i}" for i in range(1, 11)]  # 10种礼物ID（含1个无效礼物）
PAY_TYPES = ["douyin_coin", "wechat", "alipay"]
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 与客户端日志复用直播间ID


def generate_gift_log(seq):
    """生成单条礼物日志（含高价值礼物场景）"""
    user_id = f"u_{100000 + seq}"
    room_id = random.choice(ROOM_IDS)
    gift_id = random.choice(GIFT_IDS)

    # 高价值礼物（如跑车、火箭）赠送次数少（1-2次），普通礼物次数多（1-10次）
    if gift_id in ["g_300003", "g_300004", "g_300005"]:  # 豪华/特效礼物
        gift_count = random.randint(1, 2)
    else:
        gift_count = random.randint(1, 10)

    timestamp = 1735603200000 + seq  # 时间戳递增
    pay_type = random.choice(PAY_TYPES)

    # 2%概率生成无效礼物ID（测试过滤逻辑）
    if random.random() < 0.02:
        gift_id = "g_invalid"

    return {
        "user_id": user_id,
        "room_id": room_id,
        "gift_id": gift_id,
        "gift_count": gift_count,
        "timestamp": timestamp,
        "pay_type": pay_type
    }


def send_to_kafka(producer, logs):
    """批量发送日志到Kafka"""
    for log in logs:
        try:
            producer.send(
                topic=TOPIC,
                key=log["room_id"].encode("utf-8"),  # 按room_id分区
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
        batch_logs = [generate_gift_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()