import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_cdn_v3"
TOTAL_COUNT = 30000  # CDN日志量保持不变
BATCH_SIZE = 1000

# 基础配置不变
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
CDN_NODE_IDS = [f"cdn_6000{i}" for i in range(1, 21)]  # 20个CDN节点
# region_code与MySQL t_region_dict完全关联（全量编码）
REGION_CODES = [
    '110000', '310000', '440000', '330000', '420000',  # 省级
    '110100', '310100', '440100', '440300', '330100', '420100',  # 市级
    '110105', '310104', '440106', '440305', '330106', '420106'  # 区级
]

# 核心1：拆分11月日期为「20日之前」和「20日之后」，给20日之后更高权重
NOV_DATES = []
NOV_DATES_AFTER_20 = []  # 11月20日-30日（必须每天有数据）
NOV_DATES_BEFORE_20 = []  # 11月1日-19日（确保覆盖）


def init_nov_dates():
    """初始化11月日期列表，拆分20日前后并设置权重"""
    start_date = datetime(2025, 11, 1)
    for day in range(30):
        current_date = start_date + timedelta(days=day)
        if current_date.day >= 20:
            NOV_DATES_AFTER_20.append(current_date)
        else:
            NOV_DATES_BEFORE_20.append(current_date)
    # 合并日期列表，20日之后的日期权重为3，之前为1（确保20日后每天必现）
    NOV_DATES.extend(NOV_DATES_BEFORE_20)
    NOV_DATES.extend(NOV_DATES_AFTER_20 * 3)  # 权重放大3倍


# 初始化11月日期（程序启动时执行）
init_nov_dates()


def generate_10s_interval_timestamp(date):
    """为指定日期生成随机的10秒间隔时间戳（00:00:00~23:59:50）"""
    # 每天10秒间隔共8640个时间点（0~86390秒，步长10）
    random_second = random.randint(0, 86390)  # 随机选一个10秒间隔的秒数
    # 确保秒数是10的倍数（符合10秒间隔）
    random_second = (random_second // 10) * 10
    # 构造完整时间
    current_time = date.replace(
        hour=random_second // 3600,
        minute=(random_second % 3600) // 60,
        second=random_second % 60,
        microsecond=0
    )
    return int(current_time.timestamp() * 1000)


def generate_cdn_log(_):
    """生成CDN日志（核心优化：随机选日期，确保全月覆盖）"""
    cdn_node_id = random.choice(CDN_NODE_IDS)
    room_id = random.choice(ROOM_IDS)
    region_code = random.choice(REGION_CODES)  # 与MySQL关联

    # 核心优化2：随机选择11月日期（20日之后权重更高，必现）
    selected_date = random.choice(NOV_DATES)
    # 生成该日期内的10秒间隔时间戳
    timestamp = generate_10s_interval_timestamp(selected_date)

    # 原业务逻辑完全保留（高峰时段负载调整）
    hour = selected_date.hour  # 直接从选中日期取小时，无需转换时间戳
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
        "timestamp": timestamp  # 覆盖11月1-30日，20日之后必现
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
        # 无需seq，直接生成随机日期日志
        batch_logs = [generate_cdn_log(_) for _ in range(min(BATCH_SIZE, TOTAL_COUNT - i))]
        send_to_kafka(producer, batch_logs)

        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()