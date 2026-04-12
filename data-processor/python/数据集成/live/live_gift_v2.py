import json
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

# 配置参数
BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]  # 替换为实际Kafka地址
TOPIC = "live_gift_v2"
TOTAL_COUNT = 50000  # 生成5万条日志(礼物行为量低于普通行为)
BATCH_SIZE = 500  # 每批发送500条
# 礼物ID：与t_gift表完全关联（g_300001至g_300010），含1个无效状态礼物
GIFT_IDS = [f"g_30000{i}" for i in range(1, 11)]
PAY_TYPES = ["douyin_coin", "wechat", "alipay"]
# 直播间ID：与t_room表完全关联（r_200001至r_200100）
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]


# 时间范围定义（覆盖2022年全年及2025年11月每天）
def get_random_timestamp():
    """随机生成2022年任意一天 或 2025年11月任意一天的时间戳(毫秒)"""
    # 2022年1月1日 00:00:00 至 2022年12月31日 23:59:59（365天）
    y2022_start = 1640995200000  # 2022-01-01 00:00:00
    y2022_end = 1672531199000  # 2022-12-31 23:59:59
    # 2025年11月1日 00:00:00 至 2025年11月30日 23:59:59（30天）
    y2025_11_start = 1761888000000  # 2025-11-01 00:00:00
    y2025_11_end = 1764480000000  # 2025-11-30 23:59:59

    # 50%概率选择2022年，50%选择2025年11月
    if random.random() < 0.5:
        return random.randint(y2022_start, y2022_end)
    else:
        return random.randint(y2025_11_start, y2025_11_end)


def generate_gift_log(seq):
    """生成单条礼物日志(确保与MySQL表关联，时间戳覆盖指定范围)"""
    # 用户ID：与t_user表完全关联（u_100000至u_149999，共5万条）
    user_id = f"u_{100000 + seq}"
    # 直播间ID：与t_room表完全关联
    room_id = random.choice(ROOM_IDS)
    # 礼物ID：与t_gift表完全关联（含1个无效状态礼物g_300010）
    gift_id = random.choice(GIFT_IDS)

    # 高价值礼物(豪华/特效)赠送次数少，普通礼物次数多
    if gift_id in ["g_300003", "g_300004", "g_300005", "g_300008", "g_300009"]:  # 豪华/特效礼物
        gift_count = random.randint(1, 2)
    else:  # 普通礼物
        gift_count = random.randint(1, 10)

    # 时间戳：覆盖2022年及2025年11月每天
    timestamp = get_random_timestamp()
    pay_type = random.choice(PAY_TYPES)

    # 2%概率生成无效礼物ID(测试过滤逻辑，与t_gift无关联)
    if random.random() < 0.02:
        gift_id = "g_invalid"

    return {
        "user_id": user_id,
        "room_id": room_id,
        "gift_id": gift_id,
        "gift_count": gift_count,
        "timestamp": timestamp,
        "pay_type": pay_type
    }


def send_to_kafka(producer, logs):
    """批量发送日志到Kafka"""
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
        batch_logs = [generate_gift_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()