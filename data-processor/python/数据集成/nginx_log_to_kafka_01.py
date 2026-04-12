import time
import os
from confluent_kafka import Producer
from hdfs import InsecureClient

"""
数据集成模块: Nginx服务器日志数据-实时处理案例
"""

# HDFS配置
HDFS_NAMENODE = "http://192.168.63.128:9870"
HDFS_LOG_ROOT = "/user/logs/nginx"
LOCAL_CHECKPOINT = "D:/software/nginx-1.28.0/nginx-1.28.0/logs/hdfs_checkpoint.txt"  # 本地检查点文件

# KAFKA配置
KAFKA_BROKER = "192.168.63.128:9092"  # Kafka broker地址
KAFKA_TOPIC = "nginx-access-logs"  # 目标Kafka主题

# Nginx日志格式正则（匹配之前配置的main格式）
LOG_PATTERN = r'^(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)" "(.*?)"$'
# 分组对应：ip, time_local, request, status, body_bytes, referer, user_agent, x_forwarded_for


class HDFSLogKafkaHandler:
    def __init__(self):
        self.kafka_producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        self.hdfs_client = InsecureClient(HDFS_NAMENODE)
        self.last_processed_dt = self.read_checkpoint()  # 读取最后处理的时间

    def read_checkpoint(self):
        """读取检查点，确定上次处理到哪个dt和hour"""
        if os.path.exists(LOCAL_CHECKPOINT):
            with open(LOCAL_CHECKPOINT, 'r') as f:
                return f.read().strip()
        return None

    def write_checkpoint(self, dt_hour):
        """更新检查点"""
        with open(LOCAL_CHECKPOINT, 'w') as f:
            f.write(dt_hour)


    def check_and_process_hdfs_logs(self):
        """检查HDFS上新的日志文件并处理"""
        # 获取当前时间和前几个小时的时间分区
        now = datetime.datetime.now()
        for i in range(3):  # 检查最近3个小时的数据
            check_time = now - datetime.timedelta(hours=i)
            dt = check_time.strftime("%Y%m%d")
            hour = check_time.strftime("%H")
            dt_hour = f"{dt}_{hour}"

            # 如果已经处理过则跳过
            if self.last_processed_dt and dt_hour <= self.last_processed_dt:
                continue

            hdfs_path = f"{HDFS_LOG_ROOT}/dt={dt}/hour={hour}/access.log"
            if self.hdfs_client.status(hdfs_path, strict=False):
                self.process_hdfs_log_file(hdfs_path)
                self.write_checkpoint(dt_hour)


if __name__ == "__main__":
    handler = HDFSLogKafkaHandler()

    # 使用schedule定期检查HDFS新数据
    import schedule

    schedule.every(10).minutes.do(handler.check_and_process_hdfs_logs)

    print("开始监听HDFS日志目录...")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        handler.kafka_producer.flush()




