import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_product_shelf_v2"
TOTAL_COUNT = 15000  # 保持原日志量不变
BATCH_SIZE = 600

# 核心1：确保room_id/product_id与MySQL完全关联（严格复刻表结构规则）
# MySQL t_room：room_id范围 r_200001 ~ r_200100（100个直播间），完全匹配
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
# MySQL t_product：product_id范围 p_500001 ~ p_5010000（1万条商品），脚本取前1000个（完全覆盖）
PRODUCT_IDS = [f"p_50000{i}" for i in range(1, 1001)]  # p_500001 ~ p_501000，均在MySQL商品池中
SHELF_STATUSES = ["up", "down"]  # 上架/下架（保留原规则）
ANCHOR_REMARKS = [
    "今日主推, 限时优惠", "库存有限, 先到先得", "新品上架, 福利价", "", "专属折扣, 仅限直播间"
]

# 核心2：初始化2025年11月日期（拆分前后段，给20日后更高权重）
NOV_DATES = []
NOV_DATES_AFTER_20 = []  # 11月20日-30日（重点保障，每天必现）
NOV_DATES_BEFORE_20 = []  # 11月1日-19日（全量覆盖）


def init_nov_dates():
    """初始化11月日期列表，20日后权重放大3倍（确保必现）"""
    start_date = datetime(2025, 11, 1)
    for day in range(30):  # 覆盖11月1-30日所有日期
        current_date = start_date + timedelta(days=day)
        if current_date.day >= 20:
            NOV_DATES_AFTER_20.append(current_date)
        else:
            NOV_DATES_BEFORE_20.append(current_date)
    # 日期权重分配：20日后占比更高（19 + 11*3 = 52份，20日后占33份）
    NOV_DATES.extend(NOV_DATES_BEFORE_20)
    NOV_DATES.extend(NOV_DATES_AFTER_20 * 3)


# 初始化11月日期（程序启动时执行）
init_nov_dates()


def generate_random_timestamp(date):
    """生成指定日期内的随机毫秒级时间戳（00:00:00 ~ 23:59:59）"""
    # 当天0点时间戳（毫秒）
    day_start = int(date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    # 当天23:59:59时间戳（毫秒）
    day_end = int(date.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    return random.randint(day_start, day_end)


def generate_product_shelf_log(seq):
    """生成商品上架日志（核心优化：关联有效性+时间戳范围）"""
    # 1. 关联MySQL：room_id/product_id均来自有效池（无无效ID）
    room_id = random.choice(ROOM_IDS)  # 与t_room关联100%成功
    product_id = random.choice(PRODUCT_IDS)  # 与t_product关联100%成功（删除无效ID逻辑）

    # 2. 时间戳：仅2025年11月随机日期（20日后权重更高）
    selected_date = random.choice(NOV_DATES)
    shelf_time = generate_random_timestamp(selected_date)  # 替换原固定步长时间戳

    # 3. 保留原业务逻辑：上架状态权重、电商直播间强制上架
    shelf_status = random.choices(SHELF_STATUSES, weights=[0.8, 0.2], k=1)[0]  # 80%上架
    anchor_remark = random.choice(ANCHOR_REMARKS)
    sort_num = random.randint(1, 10)  # 排序1-10

    # 电商分类直播间（30个）强制上架+非空备注（保留原逻辑）
    if room_id in [f"r_20000{i}" for i in range(31, 60)]:
        shelf_status = "up"
        anchor_remark = random.choice(ANCHOR_REMARKS[:-1])  # 排除空备注

    return {
        "room_id": room_id,  # 关联t_room
        "product_id": product_id,  # 关联t_product
        "shelf_status": shelf_status,
        "shelf_time": shelf_time,  # 仅2025年11月数据
        "anchor_remark": anchor_remark,
        "sort_num": sort_num
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
        batch_logs = [generate_product_shelf_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()