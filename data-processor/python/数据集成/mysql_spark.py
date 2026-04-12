from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from pyspark import SparkConf

"""
数据集成模块: 业务数据库数据-Spark离线数据处理案例
未验证
"""

def main():
    # 初始化SparkSession
    conf = SparkConf().set("spark.driver.extraJavaOptions",
                           "--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED")

    spark = SparkSession.builder \
        .master("local") \
        .appName("Fashion Offline Sync")   \
        .config(conf=conf) \
        .config("spark.jars", "D:/workspace/data-processor/mysql-connector-java-8.0.20.jar") \
        .enableHiveSupport() \
        .getOrCreate()

    # MySQL连接配置
    mysql_conf = {
        "url": "jdbc:mysql://192.168.63.128:3306",
        "user": "root",
        "password": "Dfw920130Q520",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    spark.sql("create database fashion_dw")

    # 1. 全量同步商品数据
    goods_df = spark.read \
        .format("jdbc") \
        .option("url", f"{mysql_conf['url']}/erp_db") \
        .option("dbtable", "erp_db.goods") \
        .option("user", mysql_conf["user"]) \
        .option("password", mysql_conf["password"]) \
        .option("driver", mysql_conf["driver"]) \
        .load()

    goods_df.show(5)

    # 数据清洗：拆分尺码/颜色列表，添加日期分区
    goods_cleaned = goods_df \
        .withColumn("size_list", split(col("size_range"), ",")) \
        .withColumn("color_list", split(col("color_list"), ",")) \
        .withColumn("sync_date", current_date())  # 同步日期分区

    goods_cleaned.show(5)

    # 写入Hive数据仓库（全量覆盖）
    goods_cleaned.write \
        .mode("overwrite") \
        .partitionBy("sync_date", "category") \
        .saveAsTable("fashion_dw.dim_goods_full")

    # 2. 全量同步用户数据
    users_df = spark.read \
        .format("jdbc") \
        .option("url", f"{mysql_conf['url']}/member_db") \
        .option("dbtable", "member_db.users") \
        .option("user", mysql_conf["user"]) \
        .option("password", mysql_conf["password"]) \
        .option("driver", mysql_conf["driver"]) \
        .load()
    users_df.show(5)

    # 数据清洗：计算年龄，添加会员等级描述
    users_cleaned = users_df \
        .withColumn("age", year(current_date()) - year(col("birth_date"))) \
        .withColumn("vip_level_desc",
                    when(col("vip_level") == 1, "普通会员")
                    .when(col("vip_level") == 2, "银卡会员")
                    .when(col("vip_level") == 3, "金卡会员")
                    .otherwise("钻石会员")) \
        .withColumn("sync_date", current_date())
    users_cleaned.show(5)

    # 写入Hive数据仓库
    users_cleaned.write \
        .mode("overwrite") \
        .partitionBy("sync_date") \
        .saveAsTable("fashion_dw.dim_users_full")

    print(f"全量同步完成：{datetime.now()}")
    spark.stop()

if __name__ == "__main__":
    main()