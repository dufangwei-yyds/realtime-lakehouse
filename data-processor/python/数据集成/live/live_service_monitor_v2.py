import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_service_monitor_v2"
TOTAL_COUNT = 10000  # 总生成日志数（可按需调整）
BATCH_SIZE = 500
SERVICES = [
    {"name": "push_service", "methods": ["start_push", "stop_push", "reconnect_push"], "base_call": 500},
    {"name": "play_service", "methods": ["play_stream", "pause_stream", "stop_stream"], "base_call": 2000},
    {"name": "gift_service", "methods": ["send_gift", "query_gift", "refund_gift"], "base_call": 800},
    {"name": "order_service", "methods": ["create_order", "pay_order", "refund_order"], "base_call": 600},
    {"name": "compliance_service", "methods": ["detect_content", "review_content", "punish_anchor"], "base_call": 300}
]

# ---------------------- 核心配置：2025年11月时间范围 ----------------------
# 2025年11月01日 00:00:00 的毫秒级时间戳
NOV_1_2025_TIMESTAMP = int(datetime(2025, 11, 1, 0, 0, 0).timestamp() * 1000)
# 2025年11月30日 23:59:59 的毫秒级时间戳
NOV_30_2025_TIMESTAMP = int(datetime(2025, 11, 30, 23, 59, 59).timestamp() * 1000)
# 11月20日（含）之后的日期列表（确保每天必含数据）
REQUIRED_DATES = [datetime(2025, 11, day) for day in range(20, 31)]  # 11-20 到 11-30
# 11月01日-19日的日期列表（补充数据）
OPTIONAL_DATES = [datetime(2025, 11, day) for day in range(1, 20)]  # 11-01 到 11-19
ALL_DATES = OPTIONAL_DATES + REQUIRED_DATES


def get_random_timestamp_in_nov_2025(seq):
    """
    生成2025年11月的随机毫秒级时间戳，确保11月20日后每天都有数据
    :param seq: 日志序号（用于分配必选日期数据）
    :return: 毫秒级时间戳
    """
    # 逻辑：前 len(REQUIRED_DATES)*BATCH_SIZE 条日志优先分配给11月20日后的每天（确保每天必含）
    required_data_count = len(REQUIRED_DATES) * BATCH_SIZE
    if seq < required_data_count:
        # 分配给必选日期（11-20 到 11-30），每条日期分配 BATCH_SIZE 条数据
        date = REQUIRED_DATES[seq // BATCH_SIZE]
    else:
        # 剩余数据随机分配给11月所有日期
        date = random.choice(ALL_DATES)

    # 在选中的日期内随机生成时间（小时、分钟、秒）
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)

    # 构造完整datetime并转换为毫秒级时间戳
    random_datetime = date.replace(hour=random_hour, minute=random_minute, second=random_second)
    return int(random_datetime.timestamp() * 1000)


def generate_service_log(seq):
    service = random.choice(SERVICES)
    service_name = service["name"]
    method_name = random.choice(service["methods"])

    # 核心修改：使用11月的随机时间戳
    timestamp = get_random_timestamp_in_nov_2025(seq)

    # 保留原高峰时段逻辑（19-22点调用量翻倍）
    hour = time.localtime(timestamp / 1000).tm_hour
    if 19 <= hour <= 22:
        call_count = int(service["base_call"] * random.uniform(1.8, 2.2))
        avg_delay = random.randint(100, 200)
    else:
        call_count = int(service["base_call"] * random.uniform(0.8, 1.2))
        avg_delay = random.randint(50, 100)

    max_delay = avg_delay + random.randint(50, 150)
    error_count = int(call_count * random.uniform(0, 0.01))  # 错误率<1%
    error_rate = round(error_count / call_count * 100, 4) if call_count > 0 else 0

    # 3%概率生成异常错误率(>1%)
    if random.random() < 0.03:
        error_count = int(call_count * random.uniform(0.01, 0.05))
        error_rate = round(error_count / call_count * 100, 4)

    return {
        "service_name": service_name,
        "method_name": method_name,
        "call_count": call_count,
        "avg_delay": avg_delay,
        "max_delay": max_delay,
        "error_count": error_count,
        "error_rate": error_rate,
        "timestamp": timestamp
    }


def send_to_kafka(producer, logs):
    for log in logs:
        try:
            producer.send(
                topic=TOPIC,
                key=log["service_name"].encode("utf-8"),  # 按服务名分区
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
        # 传递seq参数（确保必选日期数据分配）
        batch_logs = [generate_service_log(seq=seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()