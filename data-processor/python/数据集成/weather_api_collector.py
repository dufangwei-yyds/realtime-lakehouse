import requests
from kafka import KafkaProducer
from hdfs import InsecureClient
import json

"""
数据集成模块: 第三方API数据-彩云天气API案例
"""

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = '192.168.63.128:9092'
KAFKA_TOPIC_REALTIME = 'realtime_weather'

# HDFS 配置
HDFS_NAMENODE_URL = 'http://192.168.63.128:9870'
HDFS_WEATHER_PATH = '/user/weather/realtime.json'

# 彩云天气API URL
API_URL = "https://api.caiyunapp.com/v2.6/1zmAFVBfCuPpdI8W/101.6656,39.2072/weather?dailysteps=3&hourlysteps=48"

# 初始化 Kafka 生产者
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 初始化 HDFS 客户端
hdfs_client = InsecureClient(HDFS_NAMENODE_URL)

def fetch_weather_data():
    """调用天气API获取数据"""
    response = requests.get(API_URL)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from {API_URL}, status code: {response.status_code}")


def extract_realtime_data(data):
    """提取实时天气数据部分"""
    realtime = data['result']['realtime']
    return {
        'temperature': realtime['temperature'],
        'humidity': realtime['humidity'],
        'skycon': realtime['skycon'],
        'wind_speed': realtime['wind']['speed'],
        'visibility': realtime['visibility'],
        'pm25': realtime['air_quality']['pm25'],
        'aqi': realtime['air_quality']['aqi']['chn']
    }


def process_and_send_to_kafka(realtime_data):
    """将实时天气数据发送到 Kafka"""
    producer.send(KAFKA_TOPIC_REALTIME, value=realtime_data)
    print("✅ 实时天气数据已发送至 Kafka")


def save_other_data_to_hdfs(all_data):
    """将非实时数据写入 HDFS"""
    with hdfs_client.write(HDFS_WEATHER_PATH, overwrite=True) as writer:
        writer.write(json.dumps(all_data, ensure_ascii=False).encode('utf-8'))
    print(f"✅ 全量天气数据已写入 HDFS: {HDFS_WEATHER_PATH}")


def main():
    # 获取原始天气数据
    raw_data = fetch_weather_data()

    # 提取实时数据
    realtime_data = extract_realtime_data(raw_data)

    # 发送到 Kafka
    process_and_send_to_kafka(realtime_data)

    # 写入 HDFS (全量数据)
    save_other_data_to_hdfs(raw_data)


if __name__ == '__main__':
    main()
