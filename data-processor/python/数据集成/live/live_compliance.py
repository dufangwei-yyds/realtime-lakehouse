import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_compliance"
TOTAL_COUNT = 10000  # 合规日志量较少（每直播间周期生成）
BATCH_SIZE = 500
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
ILLEGAL_TYPES = ["ad", "porn", "violence", "none"]  # 95%无违规
ILLEGAL_LEVELS = [0, 1, 2, 3]  # 0=无，1=警告，2=限流，3=封号
REVIEW_STATUSES = ["auto", "manual"]
REVIEW_RESULTS = ["confirm", "reject"]


def generate_compliance_log(seq):
    room_id = random.choice(ROOM_IDS)
    timestamp = 1735603200000 + seq * 300000  # 每5分钟1条（300000ms）
    illegal_type = random.choices(ILLEGAL_TYPES, weights=[0.03, 0.01, 0.01, 0.95], k=1)[0]

    # 无违规时默认值
    if illegal_type == "none":
        illegal_level = 0
        illegal_content = ""
        review_status = "auto"
        review_result = "confirm"
    else:
        # 违规等级：广告多为警告，色情/暴力可能限流
        illegal_level = 1 if illegal_type == "ad" else random.choice([1, 2])
        illegal_content = random.choice([
            "提及第三方平台链接", "暴露敏感部位", "言语暴力", "虚假宣传"
        ])
        review_status = random.choices(REVIEW_STATUSES, weights=[0.9, 0.1], k=1)[0]
        review_result = random.choices(REVIEW_RESULTS, weights=[0.8, 0.2], k=1)[0]

    # 2%概率生成异常违规等级（无违规却有等级）
    if random.random() < 0.02 and illegal_type == "none":
        illegal_level = random.choice([1, 2])

    return {
        "room_id": room_id,
        "detect_time": timestamp,
        "illegal_type": illegal_type,
        "illegal_level": illegal_level,
        "illegal_content": illegal_content,
        "review_status": review_status,
        "review_result": review_result
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
        batch_logs = [generate_compliance_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()