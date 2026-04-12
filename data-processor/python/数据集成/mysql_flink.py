from pyflink.datastream import StreamExecutionEnvironment
from flink.table import StreamTableEnvironment, EnvironmentSettings

"""
数据集成模块: 业务数据库数据-Flink实时数据处理案例
未验证
"""

def main():
    # 1. 初始化执行环境
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # 测试阶段并行度设为1
    # 配置JAR包路径（MySQL CDC连接器）
    # env.add_jars("file:///home/dufangwei/workspace/data-processor/flink-connector-mysql-cdc-2.4.1.jar")
    env.add_jars("D:/workspace/data-processor/flink-connector-mysql-cdc-2.4.1.jar")

    # 初始化表环境
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2. 配置MySQL CDC连接（订单表）
    t_env.execute_sql("""
                      CREATE TABLE orders_cdc
                      (
                          order_id     BIGINT,
                          order_no     STRING,
                          user_id      BIGINT,
                          sku_id       STRING,
                          size         STRING,
                          color        STRING,
                          quantity     INT,
                          order_amount DECIMAL(10, 2),
                          order_status TINYINT,
                          create_time  TIMESTAMP,
                          update_time  TIMESTAMP,
                          PRIMARY KEY (order_id) NOT ENFORCED
                      ) WITH (
                            'connector' = 'mysql-cdc',
                            'hostname' = '192.168.63.128',
                            'port' = '3306',
                            'username' = 'root',
                            'password' = 'Dfw920130Q520',
                            'database-name' = 'ecommerce_db',
                            'table-name' = 'orders',
                            'scan.startup.mode' = 'latest-offset' 
                            )
                      """)

    # 3. 配置MySQL CDC连接（库存表）
    t_env.execute_sql("""
                      CREATE TABLE inventory_cdc
                      (
                          sku_id             STRING,
                          size               STRING,
                          warehouse_id       INT,
                          available_quantity INT,
                          locked_quantity    INT,
                          update_time        TIMESTAMP,
                          PRIMARY KEY (sku_id, size, warehouse_id) NOT ENFORCED
                      ) WITH (
                            'connector' = 'mysql-cdc',
                            'hostname' = '192.168.63.128',
                            'port' = '3306',
                            'username' = 'root',
                            'password' = 'Dfw920130Q520',
                            'database-name' = 'erp_db',
                            'table-name' = 'inventory',
                            'scan.startup.mode' = 'latest-offset'
                            )
                      """)

    # 4. 配置Kafka输出（订单数据）
    t_env.execute_sql("""
                      CREATE TABLE orders_kafka
                      (
                          order_id     BIGINT,
                          order_no     STRING,
                          user_id      BIGINT,
                          sku_id       STRING,
                          size         STRING,
                          color        STRING,
                          quantity     INT,
                          order_amount DECIMAL(10, 2),
                          order_status TINYINT,
                          create_time  TIMESTAMP,
                          update_time  TIMESTAMP
                      ) WITH (
                            'connector' = 'kafka',
                            'topic' = 'fashion_orders',
                            'properties.bootstrap.servers' = '192.168.63.128:9092',
                            'properties.group.id' = 'orders_group',
                            'format' = 'json'
                            )
                      """)

    # 5. 配置Kafka输出（库存数据）
    t_env.execute_sql("""
                      CREATE TABLE inventory_kafka
                      (
                          sku_id             STRING,
                          size               STRING,
                          warehouse_id       INT,
                          available_quantity INT,
                          locked_quantity    INT,
                          total_quantity     INT,
                          update_time TIMESTAMP
                      ) WITH (
                            'connector' = 'kafka',
                            'topic' = 'fashion_inventory',
                            'properties.bootstrap.servers' = '192.168.63.128:9092',
                            'properties.group.id' = 'inventory_group',
                            'format' = 'json'
                            )
                      """)

    # 6. 处理订单数据并写入Kafka
    t_env.execute_sql("""
                      INSERT INTO orders_kafka
                      SELECT order_id,
                             order_no,
                             user_id,
                             sku_id, size, color, quantity, order_amount, order_status, create_time, update_time
                      FROM orders_cdc
                      """)

    # 7. 处理库存数据（计算总库存）并写入Kafka
    t_env.execute_sql("""
                      INSERT INTO inventory_kafka
                      SELECT sku_id, size, warehouse_id, available_quantity, locked_quantity, available_quantity + locked_quantity AS total_quantity, # 计算总库存
                          update_time
                      FROM inventory_cdc
                      """)

    # 执行作业
    env.execute("Fashion Real-time Sync Job")


if __name__ == "__main__":
    main()