from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import StructType, StringType, LongType, DoubleType

# mkdir -p /data/tracking_data /data/checkpoints
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 process_data.py
# # 等待30秒后，查看生成的JSON文件
# ls /data/tracking_data/event_type=add_to_cart/date=2025-07-12/
# cat /data/tracking_data/event_type=add_to_cart/date=2025-07-12/*.json

#PYSPARK_PYTHON=python3.10 spark-submit \
#--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.apache.kafka:kafka-clients:3.6.0 \
#  process_data.py

# 用户行为触发→前端埋点采集→后端接收→Kafka 缓冲->基于存储的数据进行分析（如计算各商品的加购转化率、用户行为路径分析等）

# 初始化Spark
spark = SparkSession.builder \
    .appName("TrackingDataProcessing") \
    .getOrCreate()

# 定义Kafka消息的JSON结构（与前端埋点字段对应）
schema = StructType() \
    .add("event_type", StringType()) \
    .add("timestamp", LongType()) \
    .add("user_id", StringType()) \
    .add("session_id", StringType()) \
    .add("page_id", StringType()) \
    .add("product_id", StringType()) \
    .add("category", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", LongType()) \
    .add("received_at", LongType()) \
    .add("ip_address", StringType())

# 从Kafka读取数据
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tracking_events") \
    .load()

# 解析JSON数据
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 数据清洗（示例：过滤无效价格和数量）
cleaned_df = parsed_df \
    .filter(col("price") > 0) \
    .filter(col("quantity") > 0) \
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) \
    .withColumn("event_date", to_date(col("event_time")))  # 添加日期列

# 写入本地文件系统（按天分区）
# 本地路径  按事件类型和日期分区 每30秒处理一次
query = cleaned_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "hdfs://localhost:8020/tracking_data/events/") \
    .option("checkpointLocation", "hdfs://localhost:8020/tracking_data/checkpoints/") \
    .partitionBy("event_type", "event_date") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()

