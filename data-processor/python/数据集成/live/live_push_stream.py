import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数（对齐现有脚本）
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_push_stream"
TOTAL_COUNT = 40000  # 推流日志量与直播场次匹配
BATCH_SIZE = 1000
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 复用100个直播间
ANCHOR_IDS = [f"a_40000{i}" for i in range(1, 1001)]  # 复用1000个主播
PUSH_STATUSES = ["normal", "interrupt", "reconnect"]  # 推流状态
NETWORK_TYPES = ["wifi", "5g", "wired"]  # 主播网络类型


def generate_push_stream_log(seq):
    """生成推流质量日志（含状态波动逻辑）"""
    room_id = random.choice(ROOM_IDS)
    anchor_id = random.choice(ANCHOR_IDS)
    push_status = random.choices(PUSH_STATUSES, weights=[0.9, 0.05, 0.05], k=1)[0]  # 90%正常
    timestamp = 1735603200000 + seq

    # 网络类型决定码率/帧率（有线>5G>wifi）
    network_type = random.choice(NETWORK_TYPES)
    if network_type == "wired":
        video_bitrate = random.randint(2000, 3000)  # 有线码率最高
        frame_rate = random.randint(25, 30)
    elif network_type == "5g":
        video_bitrate = random.randint(1500, 2500)
        frame_rate = random.randint(22, 28)
    else:  # wifi
        video_bitrate = random.randint(1000, 2000)
        frame_rate = random.randint(20, 25)

    # 异常状态下指标恶化
    if push_status in ["interrupt", "reconnect"]:
        frame_loss_rate = round(random.uniform(2, 5), 2)
        video_mosaic_rate = round(random.uniform(1, 2), 2)
        audio_noise = random.randint(50, 60)
    else:
        frame_loss_rate = round(random.uniform(0, 0.5), 2)
        video_mosaic_rate = round(random.uniform(0, 0.2), 2)
        audio_noise = random.randint(30, 45)

    # 1%概率生成异常码率（负数，测试清洗逻辑）
    if random.random() < 0.01:
        video_bitrate = -random.randint(1000, 2000)

    return {
        "room_id": room_id,
        "anchor_id": anchor_id,
        "push_status": push_status,
        "push_timestamp": timestamp,
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
        batch_logs = [generate_push_stream_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()