import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]  # 替换为你的Kafka地址
TOPIC = "live_recommend_expose"
TOTAL_COUNT = 3000000  # 生成3万条日志
BATCH_SIZE = 50000  # 每批发送500条
RECOMMEND_CHANNELS = ["homepage", "search", "follow", "category"]  # 推荐渠道
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 复用直播间ID
EXPOSE_INTERVAL = 60000  # 曝光日志时间间隔（60秒，即1分钟1条/直播间）


def generate_expose_log(seq):
    """生成单条推荐曝光日志（按分钟级聚合）"""
    room_id = random.choice(ROOM_IDS)
    channel = random.choice(RECOMMEND_CHANNELS)

    # 曝光人数：头部直播间（S级主播）曝光量更高
    if room_id in [f"r_20000{i}" for i in range(1, 11)]:  # 前10个直播间为头部
        expose_cnt = random.randint(800, 1500)
    else:
        expose_cnt = random.randint(100, 500)

    # 时间戳按分钟递增（模拟每分钟1条曝光日志）
    timestamp = 1735603200000 + (seq // len(ROOM_IDS)) * EXPOSE_INTERVAL

    # 3%概率生成异常曝光量（负数，测试清洗逻辑）
    if random.random() < 0.03:
        expose_cnt = -random.randint(100, 500)

    return {
        "room_id": room_id,
        "recommend_channel": channel,
        "expose_user_cnt": expose_cnt,
        "expose_time": timestamp
    }


def send_to_kafka(producer, logs):
    """批量发送日志到Kafka"""
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
        batch_logs = [generate_expose_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()