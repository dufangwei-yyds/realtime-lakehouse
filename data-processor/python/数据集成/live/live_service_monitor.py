import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_service_monitor"
TOTAL_COUNT = 10000  # 服务监控日志按分钟生成
BATCH_SIZE = 500
SERVICES = [
    {"name": "push_service", "methods": ["start_push", "stop_push", "reconnect_push"], "base_call": 500},
    {"name": "play_service", "methods": ["play_stream", "pause_stream", "stop_stream"], "base_call": 2000},
    {"name": "gift_service", "methods": ["send_gift", "query_gift", "refund_gift"], "base_call": 800},
    {"name": "order_service", "methods": ["create_order", "pay_order", "refund_order"], "base_call": 600},
    {"name": "compliance_service", "methods": ["detect_content", "review_content", "punish_anchor"], "base_call": 300}
]


def generate_service_log(seq):
    service = random.choice(SERVICES)
    service_name = service["name"]
    method_name = random.choice(service["methods"])
    timestamp = 1735603200000 + (seq // len(SERVICES)) * 60000  # 每1分钟1条/服务

    # 高峰时段调用量翻倍
    hour = time.localtime(timestamp / 1000).tm_hour
    if 19 <= hour <= 22:
        # call_count = service["base_call"] * random.randint(1.8, 2.2)
        call_count = service["base_call"] * random.uniform(1.8, 2.2)
        avg_delay = random.randint(100, 200)
    else:
        # call_count = service["base_call"] * random.randint(0.8, 1.2)
        call_count = service["base_call"] * random.uniform(0.8, 1.2)
        avg_delay = random.randint(50, 100)

    call_count = int(call_count)
    max_delay = avg_delay + random.randint(50, 150)
    error_count = int(call_count * random.uniform(0, 0.01))  # 错误率<1%
    error_rate = round(error_count / call_count * 100, 4) if call_count > 0 else 0

    # 3%概率生成异常错误率（>1%）
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
        batch_logs = [generate_service_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()