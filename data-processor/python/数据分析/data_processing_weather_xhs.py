from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, regexp_extract, to_date

# 初始化Spark
spark = SparkSession.builder \
    .appName("SocialWeatherDataProcessing") \
    .getOrCreate()

# 1. 读取Kafka中的社交媒体热门款式数据（实时→离线存储）
social_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social_hot_styles") \
    .load() \
    .select(col("value").cast("string").alias("raw_data"))

# 解析JSON数据（提取核心字段）
social_parsed_df = social_df \
    .withColumn("style_name", regexp_extract("raw_data", '"style_name":"(.*?)"', 1)) \
    .withColumn("hot_score", regexp_extract("raw_data", '"hot_score":"(.*?)"', 1).cast("int")) \
    .withColumn("publish_date", to_date(regexp_extract("raw_data", '"publish_time":"(.*?)"', 1))) \
    .filter(col("style_name").isNotNull())  # 过滤无效数据

# 2. 读取HDFS中的天气数据
weather_df = spark.read \
    .format("json") \
    .load("hdfs://localhost:8020/user/data/weather/dt=2025-07-15/")  # 按日期读取

# 3. 关联业务数据 (假设已有线下客流数据, 存储在HDFS)
customer_flow_df = spark.read \
    .format("parquet") \
    .load("hdfs://localhost:8020/user/data/store/customer_flow/")  # 客流数据 (date, store_id, flow_count)

# 4. 数据关联 (天气→客流；热门款式→库存建议)
# 4.1 天气与客流关联
weather_customer_df = weather_df \
    .join(customer_flow_df, on="date", how="inner") \
    .select(
        col("date"),
        col("temp_max"),
        col("rain_prob"),
        col("flow_count"),
        col("store_id")
    )

# 4.2 热门款式TOP10 (按热度排序)
hot_style_top10 = social_parsed_df \
    .groupBy("style_name") \
    .sum("hot_score").alias("total_hot") \
    .orderBy(col("total_hot").desc()) \
    .limit(10)

# 5. 存储处理后的数据 (供分析用)
# 5.1 天气-客流关联结果→HDFS
weather_customer_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("hdfs://localhost:8020/user/processed/weather_customer/")

# 5.2 热门款式TOP10→HDFS
hot_style_top10.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("hdfs://localhost:8020/user/processed/hot_style_top10/")

spark.stop()
