import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_cdn"
TOTAL_COUNT = 30000  # CDN日志按节点+时间生成
BATCH_SIZE = 1000
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
CDN_NODE_IDS = [f"cdn_6000{i}" for i in range(1, 21)]  # 20个CDN节点
REGION_CODES = ['110000', '120000', '130000', '140000', '150000', '210000', '220000', '230000']  # 8个核心地域


def generate_cdn_log(seq):
    cdn_node_id = random.choice(CDN_NODE_IDS)
    room_id = random.choice(ROOM_IDS)
    region_code = random.choice(REGION_CODES)
    timestamp = 1735603200000 + seq * 10000  # 每10秒1条（10000ms）

    # 高峰时段（19:00-22:00）负载升高
    hour = time.localtime(timestamp / 1000).tm_hour
    if 19 <= hour <= 22:
        cpu_usage = round(random.uniform(70, 90), 2)
        bandwidth_usage = round(random.uniform(80, 95), 2)
        delay = random.randint(40, 100)
    else:
        cpu_usage = round(random.uniform(50, 70), 2)
        bandwidth_usage = round(random.uniform(60, 80), 2)
        delay = random.randint(20, 40)

    packet_loss_rate = round(random.uniform(0, 2), 2)
    cache_hit_rate = round(random.uniform(90, 99), 2)

    # 3%概率生成异常延迟（>200ms，测试告警逻辑）
    if random.random() < 0.03:
        delay = random.randint(200, 500)

    return {
        "cdn_node_id": cdn_node_id,
        "room_id": room_id,
        "region_code": region_code,
        "cpu_usage": cpu_usage,
        "bandwidth_usage": bandwidth_usage,
        "delay": delay,
        "packet_loss_rate": packet_loss_rate,
        "cache_hit_rate": cache_hit_rate,
        "timestamp": timestamp
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
        batch_logs = [generate_cdn_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()