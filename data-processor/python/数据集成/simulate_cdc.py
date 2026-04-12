import json
import time
from datetime import datetime, timedelta
import random
from kafka import KafkaProducer

# 初始化 Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['192.168.63.128:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 商品维表数据
goods_list = [
    {"goods_id": "g1", "name": "口红", "category": "美妆", "price": 199.0},
    {"goods_id": "g2", "name": "面膜", "category": "美妆", "price": 89.0},
    {"goods_id": "g3", "name": "精华液", "category": "美妆", "price": 299.0},
]

# 主播维表数据
anchor_list = [
    {"anchor_id": "anchor_1001", "level": "S", "contract_type": "exclusive"},
    {"anchor_id": "anchor_1002", "level": "A", "contract_type": "standard"},
    {"anchor_id": "anchor_1003", "level": "B", "contract_type": "standard"},
]

def iso_now():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')

def send_cdc_message(topic, payload):
    producer.send(topic, payload)
    print(f"Sent to {topic}: {payload}")

# 当前时间
now_iso = iso_now()
now_ts_ms = int(time.time() * 1000)

# --- 发送商品维表初始数据 (op='c') ---
for goods in goods_list:
    # 添加 create_time 和 update_time（初始时相同）
    record = {
        "goods_id": goods["goods_id"],
        "name": goods["name"],
        "category": goods["category"],
        "price": goods["price"],
        "create_time": now_iso,
        "update_time": now_iso,
        "event_time": now_ts_ms
    }
    message = {
        "payload": {
            "before": None,
            "after": record,
            "op": "c",  # create
            "ts_ms": now_ts_ms
        }
    }
    send_cdc_message("mysql.goods_dim", message)

# --- 发送主播维表初始数据 (op='c') ---
for anchor in anchor_list:
    record = {
        "anchor_id": anchor["anchor_id"],
        "level": anchor["level"],
        "contract_type": anchor["contract_type"],
        "create_time": now_iso,
        "update_time": now_iso,
        "event_time": now_ts_ms
    }
    message = {
        "payload": {
            "before": None,
            "after": record,
            "op": "c",
            "ts_ms": now_ts_ms
        }
    }
    send_cdc_message("mysql.anchor_dim", message)

print("\n✅ 模拟 CDC 维表数据已发送（含 create_time / update_time / event_time）")
producer.flush()
producer.close()