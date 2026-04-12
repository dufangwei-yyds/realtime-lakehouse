import json
import random
import secrets
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_play_quality"
TOTAL_COUNT = 80000  # 播放质量日志量接近行为日志
BATCH_SIZE = 1000
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
USER_IDS = [f"u_{100000 + i}" for i in range(1, 5001)]  # 5000个观众
DEVICE_IDS = [f"d_{secrets.token_hex(3)}" for _ in range(100)]  # 100个设备ID


def generate_play_quality_log(seq):
    user_id = random.choice(USER_IDS)
    room_id = random.choice(ROOM_IDS)
    device_id = random.choice(DEVICE_IDS)
    timestamp = 1735603200000 + seq

    # 网络类型决定播放体验（复用推流网络类型逻辑）
    network_type = random.choice(["wifi", "5g", "4g"])
    if network_type == "wifi":
        first_frame = random.randint(1000, 2000)
        stall_rate = round(random.uniform(0.1, 1.0), 2)
        buffer_count = random.randint(0, 2)
    elif network_type == "5g":
        first_frame = random.randint(1500, 2500)
        stall_rate = round(random.uniform(0.5, 1.5), 2)
        buffer_count = random.randint(0, 3)
    else:  # 4g
        first_frame = random.randint(2500, 5000)
        stall_rate = round(random.uniform(1.0, 3.0), 2)
        buffer_count = random.randint(1, 5)

    buffer_duration = buffer_count * random.randint(500, 2000)
    play_bitrate = random.randint(800, 2000)
    watch_duration = random.randint(10000, 360000)  # 10秒-1小时
    play_error_code = 0 if random.random() > 0.05 else random.choice([404, 500])

    # 2%概率生成异常卡顿率（>5%）
    if random.random() < 0.02:
        stall_rate = round(random.uniform(5, 10), 2)

    return {
        "user_id": user_id,
        "room_id": room_id,
        "device_id": device_id,
        "first_frame_time": first_frame,
        "buffer_count": buffer_count,
        "buffer_duration": buffer_duration,
        "play_stall_rate": stall_rate,
        "play_bitrate": play_bitrate,
        "play_error_code": play_error_code,
        "watch_duration": watch_duration,
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
        batch_logs = [generate_play_quality_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()