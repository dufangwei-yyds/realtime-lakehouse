import json
import random
import secrets
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]  # 替换为你的Kafka地址
TOPIC = "live_behavior"
TOTAL_COUNT = 100000  # 生成10万条日志
BATCH_SIZE = 1000  # 每批发送1000条
BEHAVIOR_TYPES = ["enter", "exit", "like", "comment", "share", "follow"]
USER_SOURCES = ["recommend", "search", "follow", "homepage"]
SHARE_CHANNELS = ["", "wechat", "qq", "moments"]  # 非share行为为空
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 100个直播间ID
COMMENT_CONTENTS = ["", "主播好棒！", "666", "太精彩了！", "这个怎么买？", "垃圾内容"]  # 包含空评论


def generate_behavior_log(seq):
    """生成单条客户端行为日志（含正常/异常数据）"""
    # 随机基础字段
    user_id = f"u_{100000 + seq}" if random.random() > 0.01 else ""  # 1%概率为空user_id（异常）
    room_id = random.choice(ROOM_IDS)
    behavior_type = random.choice(BEHAVIOR_TYPES)
    timestamp = 1735603200000 + seq  # 时间戳递增（每条间隔1ms）
    device_id = f"d_{secrets.token_hex(3)}" # 随机设备ID
    user_source = random.choice(USER_SOURCES)

    # 行为相关字段
    share_channel = random.choice(SHARE_CHANNELS) if behavior_type == "share" else ""
    comment_content = random.choice(COMMENT_CONTENTS) if behavior_type == "comment" else ""

    # 1%概率生成异常时间戳（过去的时间）
    if random.random() < 0.01:
        timestamp = 1600000000000  # 2020年的时间戳

    return {
        "user_id": user_id,
        "room_id": room_id,
        "behavior_type": behavior_type,
        "timestamp": timestamp,
        "device_id": device_id,
        "user_source": user_source,
        "share_channel": share_channel,
        "comment_content": comment_content
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
    producer.flush()  # 批量提交


def main():
    # 初始化Kafka生产者
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        linger_ms=500,  # 批量发送延迟（毫秒）
        compression_type="snappy"  # 启用Snappy压缩
    )

    start_time = time.time()
    for i in range(0, TOTAL_COUNT, BATCH_SIZE):
        # 生成一批日志
        batch_logs = [generate_behavior_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        # 发送到Kafka
        send_to_kafka(producer, batch_logs)
        # 打印进度
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    # 关闭生产者
    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()