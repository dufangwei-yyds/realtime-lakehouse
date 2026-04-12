import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数(核心优化room_id生成逻辑，其余不变)
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_push_stream_v5"
TOTAL_COUNT = 40000  # 推流日志量保持不变
BATCH_SIZE = 1000

# 核心优化：room_id与MySQL t_room完全对齐（严格复刻MySQL生成规则）
# MySQL规则：CONCAT('r_20000', i)，i从1到100 → room_id范围：r_200001 ~ r_20000100
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # i=1→r_200001，i=100→r_20000100，与MySQL完全一致

# 主播ID范围对齐MySQL：MySQL主播ID是a_400001~a_400050（可选优化，不影响room_id关联）
ANCHOR_IDS = [f"a_40000{i}" for i in range(1, 51)]  # 仅保留MySQL中存在的主播ID（a_400001~a_400050）

PUSH_STATUSES = ["normal", "interrupt", "reconnect"]  # 推流状态不变
NETWORK_TYPES = ["wifi", "5g", "wired"]  # 网络类型不变

# 保留原时间戳逻辑（2025年11月全月，无其他月份）
NOV_2025_DATES = [datetime(2025, 11, day) for day in range(1, 31)]


def generate_push_stream_log():
    """生成推流日志（核心确保room_id与MySQL关联，其余逻辑不变）"""
    # 1. room_id：从与MySQL完全匹配的列表中随机选择（100%关联成功）
    room_id = random.choice(ROOM_IDS)
    # 2. anchor_id：从MySQL存在的主播ID中选择（可选优化，让主播ID也能关联）
    anchor_id = random.choice(ANCHOR_IDS)

    push_status = random.choices(PUSH_STATUSES, weights=[0.9, 0.05, 0.05], k=1)[0]

    # 时间戳逻辑不变（100%锁定2025年11月）
    random_date = random.choice(NOV_2025_DATES)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    push_datetime = random_date.replace(
        hour=random_hour,
        minute=random_minute,
        second=random_second,
        microsecond=0
    )
    push_timestamp = int(push_datetime.timestamp() * 1000)

    # 原业务逻辑（码率、帧率、异常指标等）完全保留
    network_type = random.choice(NETWORK_TYPES)
    if network_type == "wired":
        video_bitrate = random.randint(2000, 3000)
        frame_rate = random.randint(25, 30)
    elif network_type == "5g":
        video_bitrate = random.randint(1500, 2500)
        frame_rate = random.randint(22, 28)
    else:  # wifi
        video_bitrate = random.randint(1000, 2000)
        frame_rate = random.randint(20, 25)

    if push_status in ["interrupt", "reconnect"]:
        frame_loss_rate = round(random.uniform(2, 5), 2)
        video_mosaic_rate = round(random.uniform(1, 2), 2)
        audio_noise = random.randint(50, 60)
    else:
        frame_loss_rate = round(random.uniform(0, 0.5), 2)
        video_mosaic_rate = round(random.uniform(0, 0.2), 2)
        audio_noise = random.randint(30, 45)

    if random.random() < 0.01:
        video_bitrate = -random.randint(1000, 2000)

    return {
        "room_id": room_id,  # 确保与MySQL关联
        "anchor_id": anchor_id,  # 可选优化：与MySQL主播ID关联
        "push_status": push_status,
        "push_timestamp": push_timestamp,
        "video_bitrate": video_bitrate,
        "audio_bitrate": random.randint(64, 192),
        "frame_rate": frame_rate,
        "frame_loss_rate": frame_loss_rate,
        "video_mosaic_rate": video_mosaic_rate,
        "audio_noise": audio_noise,
        "network_type": network_type
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
        batch_logs = [generate_push_stream_log() for _ in range(min(BATCH_SIZE, TOTAL_COUNT - i))]
        send_to_kafka(producer, batch_logs)

        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()