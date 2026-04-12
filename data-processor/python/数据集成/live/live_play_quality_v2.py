import json
import random
import secrets
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_play_quality_v2"
TOTAL_COUNT = 80000  # 播放质量日志量保持不变
BATCH_SIZE = 1000

# 核心1：确保room_id/user_id与MySQL完全关联（严格复刻MySQL生成规则）
# MySQL t_room：room_id范围 r_200001 ~ r_200100（100个直播间）
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
# MySQL t_user：user_id范围 u_100000 ~ u_199999（10万用户），脚本取前5000个（完全覆盖）
USER_IDS = [f"u_{100000 + i}" for i in range(1, 5001)]  # u_100001 ~ u_105001，均在MySQL用户池中
DEVICE_IDS = [f"d_{secrets.token_hex(3)}" for _ in range(100)]  # 100个设备ID（保留原逻辑）

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


def generate_random_timestamp(date):
    """为指定日期生成随机毫秒级时间戳（00:00:00~23:59:59）"""
    # 生成当天0点的时间戳（毫秒）
    day_start = int(date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    # 生成当天23:59:59的时间戳（毫秒）
    day_end = int(date.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    # 在当天范围内随机取毫秒级时间戳
    return random.randint(day_start, day_end)


def generate_play_quality_log(seq):
    """生成播放质量日志（核心优化：日期随机选择，确保全月覆盖+20日之后必现）"""
    user_id = random.choice(USER_IDS)  # 与MySQL t_user关联
    room_id = random.choice(ROOM_IDS)  # 与MySQL t_room关联
    device_id = random.choice(DEVICE_IDS)  # 保留原设备ID逻辑

    # 核心优化1：随机选择11月日期（20日之后权重更高，必现）
    selected_date = random.choice(NOV_DATES)
    # 核心优化2：生成该日期内的随机时间戳（仅2025年11月数据）
    timestamp = generate_random_timestamp(selected_date)

    # 原业务逻辑完全保留（网络类型决定播放体验）
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
    watch_duration = random.randint(10000, 360000)  # 10秒-1小时（保留原范围）

    # 错误码过滤：仅保留有效错误码（0/1001/1002/1003，与Flink SQL过滤条件一致）
    play_error_code = 0 if random.random() > 0.05 else random.choice([1001, 1002, 1003])

    # 2%概率生成异常卡顿率(>5%)（保留原异常逻辑）
    if random.random() < 0.02:
        stall_rate = round(random.uniform(5, 10), 2)

    return {
        "user_id": user_id,  # 与MySQL t_user关联
        "room_id": room_id,  # 与MySQL t_room关联
        "device_id": device_id,
        "first_frame_time": first_frame,
        "buffer_count": buffer_count,
        "buffer_duration": buffer_duration,
        "play_stall_rate": stall_rate,
        "play_bitrate": play_bitrate,
        "play_error_code": play_error_code,  # 仅有效错误码
        "watch_duration": watch_duration,
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
        # 生成当前批次日志（seq仅用于区分日志，不影响时间戳）
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