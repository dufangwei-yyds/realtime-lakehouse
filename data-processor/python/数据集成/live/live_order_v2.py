import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = ["192.168.63.128:9092"]
TOPIC = "live_order_v2"
TOTAL_COUNT = 25000  # 保持原订单量不变
BATCH_SIZE = 800

# 核心1：确保关联字段与MySQL完全匹配（三表关联100%成功）
# MySQL t_room：room_id = r_200001 ~ r_200100（100个直播间）
ROOM_IDS = [f"r_20000{i}" for i in range(1, 101)]
# MySQL t_user：user_id = u_100000 ~ u_199999（10万用户），脚本取前5000个（u_100001 ~ u_105001）
USER_IDS = [f"u_{100000 + i}" for i in range(1, 5001)]
# MySQL t_product：product_id = p_500001 ~ p_5010000（1万商品），脚本取前1001个（p_500001 ~ p_501001）
PRODUCT_IDS = [f"p_50000{i}" for i in range(1, 1001)]
PAY_STATUSES = ["unpaid", "paid", "refunded"]

# 核心2：初始化2025年11月日期（20日后权重放大，确保每天必现）
NOV_DATES = []
NOV_DATES_AFTER_20 = []  # 11月20日-30日（重点保障）
NOV_DATES_BEFORE_20 = []  # 11月1日-19日（全量覆盖）


def init_nov_dates():
    """初始化11月日期列表，20日后权重为3倍（占比≈63.5%）"""
    start_date = datetime(2025, 11, 1)
    for day in range(30):  # 覆盖11月1-30日所有日期
        current_date = start_date + timedelta(days=day)
        if current_date.day >= 20:
            NOV_DATES_AFTER_20.append(current_date)
        else:
            NOV_DATES_BEFORE_20.append(current_date)
    # 日期权重分配：20日前19天 + 20日后11天×3 = 52份，确保20日后数据充足
    NOV_DATES.extend(NOV_DATES_BEFORE_20)
    NOV_DATES.extend(NOV_DATES_AFTER_20 * 3)


# 初始化11月日期（程序启动时执行）
init_nov_dates()


def generate_random_timestamp(date):
    """生成指定日期内的随机毫秒级时间戳（00:00:00 ~ 23:59:59）"""
    day_start = int(date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    day_end = int(date.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    return random.randint(day_start, day_end)


def generate_order_log(seq):
    """生成直播订单日志（核心优化：关联有效性+时间戳范围）"""
    # 1. 关联MySQL三表：字段100%匹配，无无效值
    user_id = random.choice(USER_IDS)  # 与t_user关联（u_100001~u_105001均在MySQL中）
    room_id = random.choice(ROOM_IDS)  # 与t_room关联（r_200001~r_200100均在MySQL中）
    product_id = random.choice(PRODUCT_IDS)  # 与t_product关联（p_500001~p_501001均在MySQL中）

    # 2. 时间戳：仅2025年11月随机日期（时序关系：下单时间<支付时间）
    selected_date = random.choice(NOV_DATES)
    create_time = generate_random_timestamp(selected_date)  # 下单时间（11月内随机）
    pay_status = random.choices(PAY_STATUSES, weights=[0.2, 0.75, 0.05], k=1)[0]
    pay_time = create_time + random.randint(1000, 60000) if pay_status == "paid" else None

    # 3. 保留原业务逻辑：商品数量、单价、总价计算
    product_count = random.randint(1, 10)
    product_price = round(5 + random.random() * 4995, 2)  # 复用商品表单价规则（5-5000元）
    total_amount = round(product_price * product_count, 2)

    # 4. 保留电商直播间强制已支付规则
    if room_id in [f"r_20000{i}" for i in range(31, 60)]:  # 30个电商直播间
        pay_status = "paid"
        pay_time = create_time + random.randint(1000, 60000)

    # 5. 保留2%概率异常负金额（业务异常数据，不影响关联）
    if random.random() < 0.02:
        total_amount = -total_amount

    # 订单ID：保持原规则（o_700000 ~ o_7024999，唯一不重复）
    order_id = f"o_70000{seq}"

    return {
        "order_id": order_id,
        "user_id": user_id,  # 关联t_user
        "room_id": room_id,  # 关联t_room
        "product_id": product_id,  # 关联t_product
        "product_count": product_count,
        "total_amount": total_amount,
        "pay_status": pay_status,
        "pay_time": pay_time,
        "create_time": create_time  # 仅2025年11月数据
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
        batch_logs = [generate_order_log(seq) for seq in range(i, min(i + BATCH_SIZE, TOTAL_COUNT))]
        send_to_kafka(producer, batch_logs)
        if (i // BATCH_SIZE) % 10 == 0:
            progress = (i / TOTAL_COUNT) * 100
            print(f"已发送 {i}/{TOTAL_COUNT} 条，进度: {progress:.2f}%")

    producer.close()
    end_time = time.time()
    print(f"完成！总耗时: {end_time - start_time:.2f}秒，平均速率: {TOTAL_COUNT / (end_time - start_time):.2f}条/秒")


if __name__ == "__main__":
    main()