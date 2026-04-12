import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数（保留原核心配置，无冗余）
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_recommend_expose_v5"
TOTAL_COUNT = 30000  # 生成3万条日志
BATCH_SIZE = 500  # 每批发送500条
RECOMMEND_CHANNELS = ["homepage", "search", "follow", "category"]
EXPOSE_INTERVAL = 60000  # 分钟级时间戳（适配窗口聚合）

# 核心1：room_id与MySQL完全关联（MySQL范围：r_200001~r_210000）
ROOM_IDS = [f"r_20000{i}" for i in range(1, 10001)]  # 10000个ID，全覆盖MySQL

# 核心2：仅预生成2025年11月每天的分钟级时间戳（无2022年数据）
TARGET_TIMESTAMPS = []

def init_target_timestamps():
    """仅初始化2025年11月1日~30日的分钟级时间戳（每天00:00~23:59）"""
    start_2025_11 = datetime(2025, 11, 1)  # 2025年11月1日
    for day in range(30):  # 覆盖11月整月30天
        current_date = start_2025_11 + timedelta(days=day)
        day_start_ts = int(current_date.timestamp() * 1000)  # 当天00:00毫秒级时间戳
        # 每天生成1440个时间戳（0~1439分钟，覆盖全天24小时）
        for minute in range(1440):
            TARGET_TIMESTAMPS.append(day_start_ts + minute * EXPOSE_INTERVAL)


# 初始化2025年11月时间戳（程序启动时执行）
init_target_timestamps()


def generate_expose_log(seq):
    """生成单条日志（仅2025年11月数据+MySQL关联room_id）"""
    room_id = random.choice(ROOM_IDS)  # 确保能关联MySQL
    channel = random.choice(RECOMMEND_CHANNELS)

    # 保留原曝光人数逻辑（头部直播间曝光量更高）
    if room_id in [f"r_20000{i}" for i in range(1, 11)]:
        expose_cnt = random.randint(800, 1500)
    else:
        expose_cnt = random.randint(100, 500)

    # 仅从2025年11月的时间戳中循环选择（无2022年数据）
    timestamp = TARGET_TIMESTAMPS[seq % len(TARGET_TIMESTAMPS)]

    # 保留3%异常负数曝光量（测试清洗逻辑）
    if random.random() < 0.03:
        expose_cnt = -random.randint(100, 500)

    return {
        "room_id": room_id,
        "recommend_channel": channel,
        "expose_user_cnt": expose_cnt,
        "expose_time": timestamp
    }


def send_to_kafka(producer, logs):
    """批量发送（保留原逻辑）"""
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