import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_compliance_v2"
TOTAL_COUNT = 10000  # 合规日志量保持不变（每直播间周期生成）
BATCH_SIZE = 500

# 核心1：room_id与MySQL t_room完全关联（严格复刻MySQL生成规则）
# MySQL规则：CONCAT('r_20000', i)，i从1到100 → room_id范围：r_200001 ~ r_20000100
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 与MySQL 100%匹配，关联成功率100%

# 核心2：预生成2025年11月每天的时间戳（每5分钟1条，确保仅11月数据）
NOV_2025_TIMESTAMPS = []


def init_2025_nov_timestamps():
    """预生成2025年11月1日~30日的时间戳（每5分钟1条，覆盖每天00:00~23:55）"""
    start_date = datetime(2025, 11, 1)
    for day in range(30):  # 覆盖11月整月30天
        current_date = start_date + timedelta(days=day)
        # 每天生成288条（00:00、00:05、...、23:55）
        for minute in range(0, 1440, 5):  # 步长5分钟
            # 构造当前时间（精确到分钟）
            current_time = current_date.replace(
                hour=minute // 60,
                minute=minute % 60,
                second=0,
                microsecond=0
            )
            # 转换为毫秒级时间戳
            timestamp = int(current_time.timestamp() * 1000)
            NOV_2025_TIMESTAMPS.append(timestamp)


# 初始化2025年11月时间戳（程序启动时执行）
init_2025_nov_timestamps()

# 原业务配置不变
ILLEGAL_TYPES = ["ad", "porn", "violence", "none"]  # 95%无违规
ILLEGAL_LEVELS = [0, 1, 2, 3]  # 0=无, 1=警告, 2=限流, 3=封号
REVIEW_STATUSES = ["auto", "manual"]
REVIEW_RESULTS = ["confirm", "reject"]


def generate_compliance_log(seq):
    room_id = random.choice(ROOM_IDS)  # 确保与MySQL关联

    # 核心优化：detect_time仅使用2025年11月预生成的时间戳（循环使用，确保每5分钟1条）
    # 用seq取模，避免索引越界，同时保持周期特性
    detect_time = NOV_2025_TIMESTAMPS[seq % len(NOV_2025_TIMESTAMPS)]

    # 原业务逻辑完全保留
    illegal_type = random.choices(ILLEGAL_TYPES, weights=[0.03, 0.01, 0.01, 0.95], k=1)[0]
    if illegal_type == "none":
        illegal_level = 0
        illegal_content = ""
        review_status = "auto"
        review_result = "confirm"
    else:
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
        "room_id": room_id,  # 与MySQL关联
        "detect_time": detect_time,  # 仅2025年11月数据
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