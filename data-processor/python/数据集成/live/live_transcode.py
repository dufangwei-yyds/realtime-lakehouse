import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_transcode"
TOTAL_COUNT = 20000  # 每个直播间对应3种分辨率，日志量适中
BATCH_SIZE = 800
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
TARGET_RESOLUTIONS = ["1080p", "720p", "480p"]  # 三种目标分辨率


def generate_transcode_log(seq):
    transcode_task_id = f"tc_40000{seq}"
    room_id = random.choice(ROOM_IDS)
    target_resolution = random.choice(TARGET_RESOLUTIONS)
    timestamp = 1735603200000 + seq * 15000  # 每15秒1条（15000ms）

    # 源码率（对应推流码率范围）
    source_bitrate = random.randint(1000, 3000)

    # 分辨率对应目标码率（1080p>720p>480p）
    if target_resolution == "1080p":
        target_bitrate = random.randint(1500, 2000)
    elif target_resolution == "720p":
        target_bitrate = random.randint(1000, 1500)
    else:  # 480p
        target_bitrate = random.randint(800, 1000)

    # 正常转码成功率>99%，高峰时段略降
    hour = time.localtime(timestamp / 1000).tm_hour
    if 19 <= hour <= 22:
        success_rate = round(random.uniform(98.5, 99.5), 2)
        transcode_delay = random.randint(1000, 1500)
    else:
        success_rate = round(random.uniform(99.5, 100), 2)
        transcode_delay = random.randint(500, 1000)

    frame_loss = round(random.uniform(0, 1), 2)

    # 1.5%概率生成转码失败（成功率<90%）
    if random.random() < 0.015:
        success_rate = round(random.uniform(80, 89), 2)

    return {
        "transcode_task_id": transcode_task_id,
        "room_id": room_id,
        "source_bitrate": source_bitrate,
        "target_resolution": target_resolution,
        "target_bitrate": target_bitrate,
        "success_rate": success_rate,
        "transcode_delay": transcode_delay,
        "frame_loss": frame_loss,
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
        batch_logs = [generate_transcode_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()