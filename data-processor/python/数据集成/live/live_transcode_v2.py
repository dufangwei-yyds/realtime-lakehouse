import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_transcode_v2"
TOTAL_COUNT = 20000  # 转码日志量保持不变
BATCH_SIZE = 800

# 核心1：room_id与MySQL t_room完全关联（严格复刻MySQL生成规则）
# MySQL规则：CONCAT('r_20000', i)，i从1到100 → room_id范围：r_200001 ~ r_200100
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]  # 100%关联MySQL，无匹配失败
TARGET_RESOLUTIONS = ["1080p", "720p", "480p"]  # 三种目标分辨率不变

# 核心2：拆分11月日期为「20日之前」和「20日之后」，给20日之后更高权重（确保每天必现）
NOV_DATES = []
NOV_DATES_AFTER_20 = []  # 11月20日-30日（重点保障，每天必须有数据）
NOV_DATES_BEFORE_20 = []  # 11月1日-19日（确保全量覆盖）


def init_nov_dates():
    """初始化11月日期列表，设置权重确保20日之后必现"""
    start_date = datetime(2025, 11, 1)
    for day in range(30):  # 覆盖11月1-30日所有日期
        current_date = start_date + timedelta(days=day)
        if current_date.day >= 20:
            NOV_DATES_AFTER_20.append(current_date)
        else:
            NOV_DATES_BEFORE_20.append(current_date)
    # 日期权重分配：20日之后权重放大3倍，确保被选中概率更高
    NOV_DATES.extend(NOV_DATES_BEFORE_20)
    NOV_DATES.extend(NOV_DATES_AFTER_20 * 3)


# 初始化11月日期（程序启动时执行）
init_nov_dates()


def generate_15s_interval_timestamp(date):
    """为指定日期生成随机的15秒间隔时间戳（00:00:00~23:59:45）"""
    # 每天15秒间隔共5760个时间点（0~86385秒，步长15）
    random_second = random.randint(0, 86385)
    # 确保秒数是15的倍数（符合15秒间隔规则）
    random_second = (random_second // 15) * 15
    # 构造完整时间（精确到秒）
    current_time = date.replace(
        hour=random_second // 3600,
        minute=(random_second % 3600) // 60,
        second=random_second % 60,
        microsecond=0
    )
    # 转换为毫秒级时间戳
    return int(current_time.timestamp() * 1000)


def generate_transcode_log(seq):
    """生成转码日志（核心优化：日期随机选择，确保全月覆盖+20日之后必现）"""
    transcode_task_id = f"tc_40000{seq}"  # 保留原任务ID生成规则
    room_id = random.choice(ROOM_IDS)  # 与MySQL t_room关联
    target_resolution = random.choice(TARGET_RESOLUTIONS)  # 保留三种分辨率随机

    # 核心优化1：随机选择11月日期（20日之后权重更高，必现）
    selected_date = random.choice(NOV_DATES)
    # 核心优化2：生成该日期内的15秒间隔时间戳（仅2025年11月数据）
    timestamp = generate_15s_interval_timestamp(selected_date)

    # 原业务逻辑完全保留
    # 源码率（1000-3000）
    source_bitrate = random.randint(1000, 3000)
    # 分辨率对应目标码率（1080p>720p>480p）
    if target_resolution == "1080p":
        target_bitrate = random.randint(1500, 2000)
    elif target_resolution == "720p":
        target_bitrate = random.randint(1000, 1500)
    else:  # 480p
        target_bitrate = random.randint(800, 1000)

    # 高峰时段（19:00-22:00）转码性能调整（直接从选中日期取小时，更高效）
    hour = selected_date.hour
    if 19 <= hour <= 22:
        success_rate = round(random.uniform(98.5, 99.5), 2)
        transcode_delay = random.randint(1000, 1500)
    else:
        success_rate = round(random.uniform(99.5, 100), 2)
        transcode_delay = random.randint(500, 1000)

    frame_loss = round(random.uniform(0, 1), 2)

    # 1.5%概率生成转码失败（成功率80-89%）
    if random.random() < 0.015:
        success_rate = round(random.uniform(80, 89), 2)

    return {
        "transcode_task_id": transcode_task_id,
        "room_id": room_id,  # 与MySQL关联
        "source_bitrate": source_bitrate,
        "target_resolution": target_resolution,
        "target_bitrate": target_bitrate,
        "success_rate": success_rate,
        "transcode_delay": transcode_delay,
        "frame_loss": frame_loss,
        "timestamp": timestamp  # 仅2025年11月1-30日数据
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
        # 生成当前批次日志（seq保持递增，确保task_id唯一）
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