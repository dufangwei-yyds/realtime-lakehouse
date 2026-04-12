import time
import re
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer

"""
数据集成模块: Nginx服务器日志数据-实时处理案例
"""

# 配置参数
LOG_FILE_PATH = "D:/software/nginx-1.28.0/nginx-1.28.0/logs/access.log"  # Nginx日志文件路径
KAFKA_BROKER = "192.168.63.128:9092"  # Kafka broker地址
KAFKA_TOPIC = "nginx-access-logs-00"  # 目标Kafka主题

# Nginx日志格式正则（匹配之前配置的main格式）
LOG_PATTERN = r'^(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)" "(.*?)"$'
# 分组对应：ip, time_local, request, status, body_bytes, referer, user_agent, x_forwarded_for


class NginxLogKafkaHandler(FileSystemEventHandler):
    def __init__(self):
        # 初始化Kafka生产者
        self.kafka_producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        # 打开日志文件并移动到末尾（从新内容开始监听）
        self.log_file = open(LOG_FILE_PATH, "r", encoding="utf-8")
        self.log_file.seek(0, 2)  # 定位到文件末尾
        print(f"开始监听日志文件：{LOG_FILE_PATH}")

    def on_modified(self, event):
        """文件被修改时触发（新日志写入）"""
        print("on_modified 触发")  # 看是否监听到修改事件
        if event.src_path == LOG_FILE_PATH:
            self.read_and_send_new_logs()

    def read_and_send_new_logs(self):
        """读取新增日志并发送到Kafka"""
        print("开始读取新日志...")
        while True:
            line = self.log_file.readline()
            if not line:  # 无新内容时退出
                print("无新日志内容")
                break
            print("读取到一行日志：" + line.strip())
            self.process_and_send(line.strip())

    def process_and_send(self, log_line):
        """解析日志并发送到Kafka"""
        # 匹配日志格式
        match = re.match(LOG_PATTERN, log_line)
        if not match:
            print(f"跳过格式不匹配的日志：{log_line}")
            return

        # 提取字段（结构化）
        log_data = {
            "ip": match.group(1),
            "time_local": match.group(2),
            "request": match.group(3),
            "status": match.group(4),
            "body_bytes": match.group(5),
            "referer": match.group(6),
            "user_agent": match.group(7),
            "x_forwarded_for": match.group(8),
            "kafka_send_time": time.time()  # 发送时间戳
        }

        # 发送到Kafka（JSON格式）
        try:
            self.kafka_producer.produce(
                KAFKA_TOPIC,
                key=log_data["ip"],  # 按IP分区，确保同一用户日志有序
                value=str(log_data),  # 实际生产环境可用json.dumps()
                on_delivery=self.delivery_report
            )
            self.kafka_producer.poll(0)  # 触发发送
        except BufferError:
            print("Kafka缓冲区满，等待重试...")
            self.kafka_producer.poll(1)  # 等待1秒后重试
            self.kafka_producer.produce(KAFKA_TOPIC, value=str(log_data))

    def delivery_report(self, err, msg):
        """Kafka消息发送结果回调"""
        if err:
            print(f"消息发送失败：{err}")
        else:
            print(f"消息发送成功：topic={msg.topic()}, 分区={msg.partition()}")


if __name__ == "__main__":
    # 启动日志监听
    event_handler = NginxLogKafkaHandler()
    observer = Observer()
    # 监听日志所在目录（处理日志切割）
    log_dir = os.path.dirname(LOG_FILE_PATH)
    observer.schedule(event_handler, path=log_dir, recursive=False)
    observer.start()

    # 保持运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        event_handler.log_file.close()  # 关闭文件
    observer.join()
