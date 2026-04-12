#!/usr/bin/env python3
# monitor_verify_ods.py

import subprocess
import json
import time
import sys
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
import argparse
import concurrent.futures

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ods_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ODSMonitor:
    """Paimon ODS层监控与验证类"""

    def __init__(self, config_path: str = None):
        self.config = self.load_config(config_path)

        # HDFS配置
        self.hdfs_namenode = self.config.get('hdfs_namenode', '192.168.63.128:8020')
        self.paimon_warehouse = self.config.get('paimon_warehouse', '/paimon/live_dw_03')
        self.ods_database = self.config.get('ods_database', 'ods.db')

        # MySQL配置
        self.mysql_host = self.config.get('mysql_host', '192.168.0.102')
        self.mysql_port = self.config.get('mysql_port', '3306')
        self.mysql_user = self.config.get('mysql_user', 'root')
        self.mysql_password = self.config.get('mysql_password', 'Dfw920130Q520,')
        self.mysql_database = self.config.get('mysql_database', 'live')

        # Kafka配置
        self.kafka_bootstrap = self.config.get('kafka_bootstrap', '192.168.63.128:9092')

        # 监控间隔
        self.monitor_interval = self.config.get('monitor_interval', 30)

        # 报警阈值
        self.thresholds = self.config.get('thresholds', {
            'cdc_lag_seconds': 60,
            'data_consistency_error': 0.01,
            'file_count_growth': 100,
            'snapshot_age_hours': 2
        })

        logger.info("ODS监控器初始化完成")

    def load_config(self, config_path: str) -> Dict:
        """加载配置文件"""
        default_config = {
            'hdfs_namenode': '192.168.63.128:8020',
            'paimon_warehouse': '/paimon/live_dw_03',
            'ods_database': 'ods.db',
            'mysql_host': '192.168.0.102',
            'mysql_port': '3306',
            'mysql_user': 'root',
            'mysql_password': 'Dfw920130Q520,',
            'mysql_database': 'live',
            'kafka_bootstrap': '192.168.63.128:9092',
            'monitor_interval': 30,
            'thresholds': {
                'cdc_lag_seconds': 60,
                'data_consistency_error': 0.01,
                'file_count_growth': 100,
                'snapshot_age_hours': 2
            }
        }

        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    default_config.update(user_config)
                    logger.info(f"从 {config_path} 加载配置")
            except Exception as e:
                logger.error(f"加载配置文件失败: {e}")

        return default_config

    def run_hdfs_command(self, command: str) -> Tuple[bool, str]:
        """执行HDFS命令"""
        try:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            return result.returncode == 0, result.stdout.strip()
        except subprocess.TimeoutExpired:
            return False, "命令执行超时"
        except Exception as e:
            return False, f"执行失败: {str(e)}"

    def run_mysql_query(self, query: str) -> Tuple[bool, List[Dict]]:
        """执行MySQL查询 - 修复反引号问题"""
        try:
            # 使用参数列表而不是字符串，避免shell注入和引号问题
            # 注意：这里使用 --execute 而不是 -e，以避免shell解析问题
            cmd = [
                'mysql',
                f'-h{self.mysql_host}',
                f'-P{self.mysql_port}',
                f'-u{self.mysql_user}',
                f'-p{self.mysql_password}',
                f'-D{self.mysql_database}',
                '--execute',
                query,
                '--silent',
                '--skip-column-names'
            ]

            # 创建环境变量，避免密码警告
            env = os.environ.copy()
            env['MYSQL_PWD'] = self.mysql_password

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30,
                env=env  # 使用自定义环境变量
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip()
                # 过滤掉可能的警告信息
                if "Using a password on the command line interface can be insecure" in error_msg:
                    lines = error_msg.split('\n')
                    error_msg = '\n'.join([line for line in lines if "Using a password" not in line])
                return False, [{"error": error_msg}]

            # 解析结果
            lines = result.stdout.strip().split('\n')
            if not lines or lines[0] == '':
                return True, []

            # 尝试解析为JSON数组
            if lines[0].startswith('['):
                try:
                    return True, json.loads(result.stdout)
                except json.JSONDecodeError:
                    # 如果不是有效的JSON，回退到表格格式
                    pass

            # 简单表格格式
            data = []
            for line in lines:
                if line.strip():
                    parts = line.split('\t')
                    if len(parts) > 1:
                        row_dict = {}
                        for i, part in enumerate(parts):
                            row_dict[f'col_{i}'] = part
                        data.append(row_dict)
                    else:
                        data.append({"row": line})
            return True, data

        except subprocess.TimeoutExpired:
            return False, [{"error": "查询超时"}]
        except Exception as e:
            return False, [{"error": str(e)}]

    def test_mysql_connection(self):
        """专门测试MySQL连接的方法"""
        print("测试MySQL连接...")

        # 先测试简单连接
        print("\n1. 测试基本连接...")
        test_query = "SELECT 1 as test_value, 'connection_ok' as status"
        success, result = self.run_mysql_query(test_query)
        print(f"  基本连接: {'成功' if success else '失败'}")

        if not success:
            print(f"  连接错误: {result}")
            return False

        # 测试各个表
        print("\n2. 测试表查询...")
        tables_to_test = [
            ("user", "用户表"),
            ("product", "商品表"),
            ("live_room", "直播间表"),
            ("anchor", "主播表"),
            ("order_item", "订单明细表"),
            ("product_sku", "商品SKU表")
        ]

        all_success = True
        for table_name, table_desc in tables_to_test:
            query = f"SELECT COUNT(*) as row_count FROM {self.mysql_database}.{table_name}"
            success, result = self.run_mysql_query(query)

            if success:
                row_count = "N/A"
                if result and len(result) > 0:
                    if isinstance(result[0], dict):
                        # 获取第一个值
                        row_count = list(result[0].values())[0]
                    elif hasattr(result[0], 'values'):
                        row_count = list(result[0].values())[0]
                print(f"  {table_desc}: 成功, 行数: {row_count}")
            else:
                print(f"  {table_desc}: 失败")
                if result and len(result) > 0:
                    error_msg = result[0].get('error', '未知错误')
                    print(f"    错误: {error_msg}")
                all_success = False

        # 测试表更新时间 - 修复版本
        print("\n3. 测试表更新时间...")
        for table_name, table_desc in tables_to_test:
            # 先检查表是否有updated_time字段
            check_query = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_SCHEMA = '{self.mysql_database}' 
            AND TABLE_NAME = '{table_name}' 
            AND COLUMN_NAME = 'updated_time'
            """
            success, check_result = self.run_mysql_query(check_query)

            if success and check_result and len(check_result) > 0:
                # 有updated_time字段
                query = f"SELECT MAX(updated_time) as latest_update FROM {self.mysql_database}.{table_name}"
                success, result = self.run_mysql_query(query)

                if success and result and len(result) > 0:
                    # 调试：打印原始结果
                    # print(f"DEBUG: {table_desc} 查询结果: {result}")

                    latest_update = "N/A"
                    if isinstance(result[0], dict):
                        # 尝试从不同可能的字段名获取值
                        possible_keys = ['latest_update', 'col_0', 'row']
                        for key in possible_keys:
                            if key in result[0] and result[0][key] is not None:
                                latest_update = result[0][key]
                                break

                        # 如果还没有找到，尝试获取第一个值
                        if latest_update == "N/A" and result[0]:
                            latest_update = list(result[0].values())[0]
                    else:
                        # 如果是其他格式，尝试获取第一个值
                        latest_update = str(result[0])[:50]  # 限制长度

                    print(f"  {table_desc}: {latest_update}")
                else:
                    print(f"  {table_desc}: 无法获取更新时间")
                    if not success and result and len(result) > 0:
                        error_msg = result[0].get('error', '')
                        if error_msg:
                            print(f"    错误: {error_msg}")
            else:
                print(f"  {table_desc}: 无updated_time字段")

        # 单独测试order表（需要特殊处理）
        print("\n4. 单独测试订单表...")
        order_queries = [
            # 测试行数
            f"SELECT COUNT(*) as cnt FROM `{self.mysql_database}`.`order`",
            # 测试更新时间
            f"SELECT MAX(updated_time) as latest_update FROM `{self.mysql_database}`.`order`"
        ]

        order_desc = "订单表"

        for i, query in enumerate(order_queries):
            if i == 0:
                print(f"  {order_desc}行数查询...")
            else:
                print(f"  {order_desc}更新时间查询...")

            success, result = self.run_mysql_query(query)

            if success and result and len(result) > 0:
                if isinstance(result[0], dict):
                    # 获取第一个值
                    value = list(result[0].values())[0]
                    if i == 0:
                        print(f"    {order_desc}行数: {value}")
                    else:
                        print(f"    {order_desc}最新更新时间: {value}")
                else:
                    print(f"    {order_desc}: 结果格式异常")
            else:
                print(f"    {order_desc}: 查询失败")
                if result and len(result) > 0:
                    error_msg = result[0].get('error', '未知错误')
                    print(f"      错误: {error_msg}")

        return all_success

    def debug_cdc_issue(self):
        """调试CDC问题"""
        print("\n=== CDC问题调试 ===")

        # 测试MySQL连接
        print("\n1. 测试MySQL连接...")
        self.test_mysql_connection()

        # 测试HDFS连接
        print("\n2. 测试HDFS连接...")
        cmd = f"hdfs dfs -ls hdfs://{self.hdfs_namenode}{self.paimon_warehouse}"
        success, output = self.run_hdfs_command(cmd)
        print(f"  HDFS连接状态: {'成功' if success else '失败'}")
        if success:
            print(f"  Paimon目录内容:")
            for line in output.split('\n')[:5]:
                print(f"    {line}")

        # 检查具体表
        print("\n3. 检查具体表状态...")
        test_table = "ods_dim_user"
        table_path = f"{self.paimon_warehouse}/{self.ods_database}/{test_table}"
        hdfs_path = f"hdfs://{self.hdfs_namenode}{table_path}"

        print(f"  检查表: {test_table}")
        print(f"  HDFS路径: {hdfs_path}")

        # 检查目录
        cmd = f"hdfs dfs -ls {hdfs_path}"
        success, output = self.run_hdfs_command(cmd)
        if success:
            print(f"  表目录内容:")
            for line in output.split('\n'):
                if line.strip():
                    print(f"    {line}")
        else:
            print(f"  无法访问表目录")

        # 检查snapshot
        snapshot_path = f"{hdfs_path}/snapshot"
        cmd = f"hdfs dfs -ls {snapshot_path} 2>/dev/null"
        success, output = self.run_hdfs_command(cmd)
        if success:
            print(f"  Snapshot文件:")
            snapshots = []
            for line in output.split('\n'):
                if line.strip() and 'snapshot-' in line:
                    parts = line.split()
                    if len(parts) >= 8:
                        filename = parts[-1]
                        snapshots.append(filename)
                        print(f"    {filename}")

            if snapshots:
                # 查看最新snapshot
                latest_snapshot = snapshots[-1]
                cmd = f"hdfs dfs -cat {snapshot_path}/{latest_snapshot}"
                success, content = self.run_hdfs_command(cmd)
                if success:
                    print(f"\n  最新snapshot内容预览:")
                    print(f"    {content[:500]}...")  # 只显示前500字符
                    try:
                        data = json.loads(content)
                        print(f"\n  Snapshot JSON结构:")
                        for key in list(data.keys())[:10]:  # 只显示前10个字段
                            value = data[key]
                            print(f"    {key}: {type(value).__name__} = {str(value)[:50]}")
                    except json.JSONDecodeError as e:
                        print(f"    JSON解析错误: {e}")
        else:
            print(f"  无法访问snapshot目录")

        print("\n=== 调试完成 ===")

    def check_paimon_tables_status(self) -> Dict:
        """检查Paimon表状态 - 使用并行处理优化性能"""
        logger.info("检查Paimon表状态...")
        results = {}

        # 定义要检查的表
        tables = [
            # 维度表
            ("ods_dim_user", "用户表"),
            ("ods_dim_anchor", "主播表"),
            ("ods_dim_live_room", "直播间表"),
            ("ods_dim_product", "商品表"),
            ("ods_dim_product_sku", "商品SKU表"),
            ("ods_dim_order", "订单表"),
            ("ods_dim_order_item", "订单明细表"),

            # 事实表
            ("ods_fact_client_log", "客户端日志事实表"),
            ("ods_fact_server_biz", "服务端业务事实表"),

            # 外部数据表
            ("ods_ext_advertisement", "广告数据表"),
            ("ods_ext_supply_chain", "供应链数据表")
        ]

        def check_table(table_name, table_desc):
            """检查单个表的状态"""
            try:
                table_path = f"{self.paimon_warehouse}/{self.ods_database}/{table_name}"
                hdfs_path = f"hdfs://{self.hdfs_namenode}{table_path}"

                # 检查表目录是否存在
                cmd = f"hdfs dfs -test -d {hdfs_path} && echo 'exists'"
                success, output = self.run_hdfs_command(cmd)

                if not success or 'exists' not in output:
                    return table_name, {
                        "status": "NOT_EXIST",
                        "description": table_desc,
                        "message": "表目录不存在"
                    }

                # 检查snapshot文件
                snapshot_path = f"{hdfs_path}/snapshot"
                cmd = f"hdfs dfs -ls {snapshot_path} 2>/dev/null | grep -c '^-'"
                success, output = self.run_hdfs_command(cmd)

                snapshot_count = int(output.strip()) if success and output.strip().isdigit() else 0

                # 获取最新snapshot信息（可选，因为耗时）
                latest_snapshot = None
                if snapshot_count > 0:
                    cmd = f"hdfs dfs -ls {snapshot_path}/snapshot-* 2>/dev/null | tail -1"
                    success, snapshot_file = self.run_hdfs_command(cmd)
                    if success and snapshot_file:
                        # 只记录文件名，不读取内容
                        parts = snapshot_file.split()
                        if len(parts) >= 8:
                            latest_snapshot = {"file": parts[-1]}

                result = {
                    "status": "HEALTHY",
                    "description": table_desc,
                    "snapshot_count": snapshot_count,
                    "latest_snapshot": latest_snapshot,
                    "hdfs_path": hdfs_path
                }

                return table_name, result

            except Exception as e:
                return table_name, {
                    "status": "ERROR",
                    "description": table_desc,
                    "message": f"检查异常: {str(e)}"
                }

        # 使用线程池并行检查
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_table = {executor.submit(check_table, name, desc): name for name, desc in tables}

            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    name, result = future.result()
                    results[name] = result
                    logger.info(
                        f"  {result['description']}: {result.get('snapshot_count', 0)}个snapshot, 状态: {result['status']}")
                except Exception as e:
                    logger.error(f"检查表 {table_name} 异常: {e}")
                    results[table_name] = {
                        "status": "ERROR",
                        "description": table_name,
                        "message": f"检查异常: {str(e)}"
                    }

        return results

    def check_data_consistency(self) -> Dict:
        """检查数据一致性"""
        logger.info("检查数据一致性...")
        checks = {}

        # 1. 订单-明细金额一致性检查
        query = f"""
        SELECT 
            COUNT(*) as error_count,
            SUM(ABS(oi_sum - o.payment_amount)) as total_error_amount
        FROM (
            SELECT 
                oi.order_id,
                SUM(oi.final_amount) as oi_sum
            FROM {self.mysql_database}.order_item oi
            GROUP BY oi.order_id
        ) t
        JOIN {self.mysql_database}.`order` o ON t.order_id = o.order_id
        WHERE ABS(t.oi_sum - o.payment_amount) > {self.thresholds['data_consistency_error']}
        """

        success, result = self.run_mysql_query(query)
        if success and result:
            error_count = result[0].get('error_count', 0) if isinstance(result[0], dict) else result[0].get('col_0', 0)
            checks['order_amount_consistency'] = {
                "status": "PASS" if int(error_count) == 0 else "FAIL",
                "error_count": error_count,
                "description": "订单与明细金额一致性"
            }

        # 2. 用户-主播关联检查
        query = f"""
        SELECT 
            COUNT(*) as error_count
        FROM {self.mysql_database}.user u
        LEFT JOIN {self.mysql_database}.anchor a ON u.user_id = a.user_id
        WHERE u.is_anchor = 1 AND a.user_id IS NULL
        """

        success, result = self.run_mysql_query(query)
        if success and result:
            error_count = result[0].get('error_count', 0) if isinstance(result[0], dict) else result[0].get('col_0', 0)
            checks['user_anchor_consistency'] = {
                "status": "PASS" if int(error_count) == 0 else "FAIL",
                "error_count": error_count,
                "description": "用户-主播关联一致性"
            }

        # 3. 商品库存有效性检查
        query = f"""
        SELECT 
            COUNT(*) as negative_stock_count
        FROM {self.mysql_database}.product
        WHERE stock_quantity < 0
        """

        success, result = self.run_mysql_query(query)
        if success and result:
            error_count = result[0].get('negative_stock_count', 0) if isinstance(result[0], dict) else result[0].get(
                'col_0', 0)
            checks['product_stock_validity'] = {
                "status": "PASS" if int(error_count) == 0 else "FAIL",
                "error_count": error_count,
                "description": "商品库存有效性"
            }

        # 4. 直播订单GMV有效性检查
        query = f"""
        SELECT 
            COUNT(*) as invalid_gmv_count
        FROM {self.mysql_database}.order_item
        WHERE live_room_id IS NOT NULL 
        AND is_gmv_valid NOT IN (0, 1)
        """

        success, result = self.run_mysql_query(query)
        if success and result:
            error_count = result[0].get('invalid_gmv_count', 0) if isinstance(result[0], dict) else result[0].get(
                'col_0', 0)
            checks['gmv_validity'] = {
                "status": "PASS" if int(error_count) == 0 else "FAIL",
                "error_count": error_count,
                "description": "GMV有效性标记"
            }

        logger.info(f"数据一致性检查完成: {len(checks)}项检查")
        return checks

    def check_cdc_sync_lag(self) -> Dict:
        """检查CDC同步延迟 - 修复snapshot读取问题"""
        logger.info("检查CDC同步延迟...")
        lag_info = {}

        # 检查关键表的同步延迟
        tables_to_check = [
            ("`user`", "ods_dim_user", "用户表"),
            ("product", "ods_dim_product", "商品表"),
            ("`order`", "ods_dim_order", "订单表"),
            ("live_room", "ods_dim_live_room", "直播间表")
        ]

        for source_table, target_table, table_desc in tables_to_check:
            try:
                # 1. 获取MySQL最新更新时间
                logger.debug(f"检查 {table_desc} (MySQL表: {source_table})")

                query = f"""
                SELECT 
                    UNIX_TIMESTAMP(MAX(updated_time)) * 1000 as mysql_latest_ms,
                    COUNT(*) as row_count,
                    DATE_FORMAT(MAX(updated_time), '%%Y-%%m-%%d %%H:%%i:%%s') as mysql_latest_str,
                    DATE_FORMAT(NOW(), '%%Y-%%m-%%d %%H:%%i:%%s') as current_db_time
                FROM {self.mysql_database}.{source_table}
                """

                success, mysql_result = self.run_mysql_query(query)

                if not success:
                    lag_info[target_table] = {
                        "status": "ERROR",
                        "description": table_desc,
                        "message": f"MySQL查询失败",
                        "details": str(mysql_result) if mysql_result else "无详情"
                    }
                    logger.error(f"  {table_desc}: MySQL查询失败")
                    continue

                if not mysql_result or len(mysql_result) == 0:
                    lag_info[target_table] = {
                        "status": "WARNING",
                        "description": table_desc,
                        "message": "MySQL查询结果为空"
                    }
                    logger.warning(f"  {table_desc}: MySQL查询结果为空")
                    continue

                mysql_data = mysql_result[0]
                mysql_latest_ms = 0
                mysql_latest_str = ""
                mysql_row_count = 0

                if isinstance(mysql_data, dict):
                    mysql_latest_ms = int(float(mysql_data.get('mysql_latest_ms', 0))) if mysql_data.get(
                        'mysql_latest_ms') else 0
                    mysql_latest_str = mysql_data.get('mysql_latest_str', '')
                    mysql_row_count = mysql_data.get('row_count', 0)
                else:
                    # 处理表格格式
                    mysql_latest_ms = int(float(mysql_data.get('col_0', 0))) if mysql_data.get('col_0') else 0
                    mysql_latest_str = mysql_data.get('col_2', '')  # 注意：日期在第3列
                    mysql_row_count = mysql_data.get('col_1', 0)  # 行数在第2列

                # 2. 获取Paimon最新更新时间
                table_path = f"{self.paimon_warehouse}/{self.ods_database}/{target_table}"
                hdfs_path = f"hdfs://{self.hdfs_namenode}{table_path}"
                snapshot_path = f"{hdfs_path}/snapshot"

                # 先检查snapshot目录是否存在
                cmd = f"hdfs dfs -test -d {snapshot_path} && echo 'exists'"
                success, snapshot_exists = self.run_hdfs_command(cmd)

                if not success or 'exists' not in snapshot_exists:
                    lag_info[target_table] = {
                        "status": "WARNING",
                        "description": table_desc,
                        "message": "Paimon snapshot目录不存在",
                        "mysql_latest": mysql_latest_str,
                        "mysql_row_count": mysql_row_count
                    }
                    logger.warning(f"  {table_desc}: snapshot目录不存在")
                    continue

                # 获取最新的snapshot文件 - 使用更可靠的方法
                cmd = f"hdfs dfs -ls {snapshot_path}/snapshot-* | tail -1"
                success, latest_snapshot_file = self.run_hdfs_command(cmd)

                if not success or not latest_snapshot_file:
                    # 尝试另一种方法
                    cmd = f"hdfs dfs -ls {snapshot_path} | grep '^-' | tail -1"
                    success, latest_snapshot_file = self.run_hdfs_command(cmd)

                if not success or not latest_snapshot_file:
                    lag_info[target_table] = {
                        "status": "WARNING",
                        "description": table_desc,
                        "message": "无法获取最新snapshot文件",
                        "mysql_latest": mysql_latest_str,
                        "mysql_row_count": mysql_row_count
                    }
                    logger.warning(f"  {table_desc}: 无snapshot文件")
                    continue

                # 提取文件名
                parts = latest_snapshot_file.split()
                if len(parts) >= 8:
                    snapshot_filename = parts[-1]
                else:
                    # 如果格式不对，尝试从行中提取文件名
                    snapshot_filename = latest_snapshot_file.split('/')[-1].strip()

                # 修正：需要完整的路径
                snapshot_file_path = f"{snapshot_path}/{snapshot_filename}"

                # 读取snapshot文件内容 - 使用head只读取部分内容
                cmd = f"hdfs dfs -cat {snapshot_file_path} | head -1000"
                success, snapshot_content = self.run_hdfs_command(cmd)

                if not success or not snapshot_content:
                    # 尝试另一种读取方法
                    cmd = f"hadoop fs -cat {snapshot_file_path} | head -1000"
                    success, snapshot_content = self.run_hdfs_command(cmd)

                if not success or not snapshot_content:
                    lag_info[target_table] = {
                        "status": "WARNING",
                        "description": table_desc,
                        "message": "无法读取snapshot文件，可能是文件格式问题或权限问题",
                        "mysql_latest": mysql_latest_str,
                        "mysql_row_count": mysql_row_count
                    }
                    logger.warning(f"  {table_desc}: 无法读取snapshot文件")
                    continue

                # 解析snapshot内容 - 修复版
                try:
                    # 首先尝试直接解析整个内容
                    try:
                        snapshot_data = json.loads(snapshot_content)
                    except json.JSONDecodeError as e:
                        # 如果直接解析失败，尝试清理内容
                        logger.debug(f"直接JSON解析失败，尝试清理内容: {str(e)}")

                        # 尝试找到JSON部分（snapshot文件可能包含其他内容）
                        lines = snapshot_content.split('\n')
                        json_content = ""
                        for line in lines:
                            if line.strip().startswith('{'):
                                json_content = line.strip()
                                break

                        if not json_content:
                            # 如果没有找到JSON，使用第一行
                            json_content = lines[0].strip() if lines else ""

                        if not json_content:
                            raise Exception("无法找到有效的JSON内容")

                        snapshot_data = json.loads(json_content)

                    # 调试：打印snapshot数据结构
                    logger.debug(f"Snapshot数据结构: {list(snapshot_data.keys())}")

                    # Paimon snapshot中的时间戳
                    paimon_latest_ms = 0

                    # 尝试从不同字段获取时间戳 - 根据您提供的JSON格式
                    possible_time_fields = [
                        'timeMillis',  # 根据您提供的JSON，时间戳在这里
                        'commitIdentifier',
                        'commitTime',
                        'timestamp',
                        'commitTimeMillis'
                    ]

                    for field in possible_time_fields:
                        if field in snapshot_data:
                            paimon_latest_ms = snapshot_data[field]
                            logger.debug(f"从字段 {field} 获取时间戳: {paimon_latest_ms}")

                            # 处理不同的数据类型
                            if isinstance(paimon_latest_ms, str) and paimon_latest_ms.isdigit():
                                paimon_latest_ms = int(paimon_latest_ms)
                            elif isinstance(paimon_latest_ms, int):
                                # 已经是整数，直接使用
                                pass
                            elif isinstance(paimon_latest_ms, float):
                                paimon_latest_ms = int(paimon_latest_ms)
                            break

                    # 如果还是没找到，尝试从snapshot文件名解析
                    if paimon_latest_ms == 0 and '-E-' in snapshot_filename:
                        try:
                            # 文件名格式: snapshot-1-0-E-1702000000000
                            parts = snapshot_filename.split('-E-')
                            if len(parts) > 1:
                                paimon_latest_ms = int(parts[1].split('.')[0])
                                logger.debug(f"从文件名解析时间戳: {paimon_latest_ms}")
                        except Exception as e:
                            logger.debug(f"从文件名解析时间戳失败: {str(e)}")

                    # 转换时间格式
                    paimon_latest_str = ''
                    if paimon_latest_ms > 0:
                        try:
                            # 将毫秒时间戳转换为datetime
                            paimon_latest_str = datetime.fromtimestamp(paimon_latest_ms / 1000).strftime(
                                '%Y-%m-%d %H:%M:%S')
                            logger.debug(f"转换时间戳 {paimon_latest_ms} 为 {paimon_latest_str}")
                        except Exception as e:
                            paimon_latest_str = f"时间戳: {paimon_latest_ms}"
                            logger.debug(f"时间戳转换失败: {str(e)}")

                    # 计算延迟
                    current_ms = int(time.time() * 1000)
                    lag_seconds = 0

                    if mysql_latest_ms > 0 and paimon_latest_ms > 0:
                        # 使用MySQL和Paimon的时间差
                        lag_seconds = (mysql_latest_ms - paimon_latest_ms) / 1000
                        logger.debug(
                            f"MySQL时间: {mysql_latest_ms}, Paimon时间: {paimon_latest_ms}, 延迟: {lag_seconds}秒")
                    elif paimon_latest_ms > 0:
                        # 使用当前时间和Paimon的时间差
                        lag_seconds = (current_ms - paimon_latest_ms) / 1000
                        logger.debug(f"当前时间: {current_ms}, Paimon时间: {paimon_latest_ms}, 延迟: {lag_seconds}秒")

                    # 确定状态
                    if mysql_latest_ms == 0:
                        status = "WARNING"
                        message = f"MySQL无更新时间记录"
                    elif paimon_latest_ms == 0:
                        status = "WARNING"
                        message = f"无法获取Paimon时间戳"
                    elif lag_seconds < 0:
                        status = "WARNING"
                        message = f"Paimon时间超前MySQL: {-lag_seconds:.2f}秒"
                    elif lag_seconds < self.thresholds['cdc_lag_seconds']:
                        status = "HEALTHY"
                        message = f"延迟正常: {lag_seconds:.2f}秒"
                    else:
                        status = "WARNING"
                        message = f"延迟较高: {lag_seconds:.2f}秒"

                    lag_info[target_table] = {
                        "status": status,
                        "description": table_desc,
                        "mysql_latest": mysql_latest_str,
                        "mysql_latest_ms": mysql_latest_ms,
                        "paimon_latest": paimon_latest_str,
                        "paimon_latest_ms": paimon_latest_ms,
                        "lag_seconds": round(lag_seconds, 2),
                        "row_count": mysql_row_count,
                        "snapshot_file": snapshot_filename,
                        "message": message,
                        "debug_info": {
                            "snapshot_keys": list(snapshot_data.keys()),
                            "found_time_field": next((f for f in possible_time_fields if f in snapshot_data), None)
                        }
                    }

                    logger.info(
                        f"  {table_desc}: MySQL={mysql_latest_str}, Paimon={paimon_latest_str}, 延迟={lag_seconds:.2f}秒, 状态={status}")

                except json.JSONDecodeError as e:
                    error_msg = f"JSON解析失败: {str(e)}"
                    # 添加更多调试信息
                    logger.error(f"  {table_desc}: {error_msg}")
                    logger.error(f"  Snapshot内容前200字符: {snapshot_content[:200] if snapshot_content else '空'}")

                    lag_info[target_table] = {
                        "status": "WARNING",
                        "description": table_desc,
                        "message": error_msg,
                        "mysql_latest": mysql_latest_str,
                        "mysql_row_count": mysql_row_count,
                        "snapshot_preview": snapshot_content[:200] if snapshot_content else "空"
                    }
                except Exception as e:
                    error_msg = f"处理异常: {str(e)}"
                    logger.error(f"  {table_desc}: {error_msg}")
                    lag_info[target_table] = {
                        "status": "ERROR",
                        "description": table_desc,
                        "message": error_msg,
                        "mysql_latest": mysql_latest_str,
                        "mysql_row_count": mysql_row_count
                    }

            except Exception as e:
                error_msg = f"检查过程中异常: {str(e)}"
                logger.error(f"  {table_desc}: {error_msg}")
                lag_info[target_table] = {
                    "status": "ERROR",
                    "description": table_desc,
                    "message": error_msg
                }

        logger.info(f"CDC同步延迟检查完成: {len(lag_info)}个表")
        return lag_info

    def check_file_system_health(self) -> Dict:
        """检查文件系统健康状态"""
        logger.info("检查文件系统健康状态...")
        fs_info = {}

        try:
            # 检查HDFS健康状态 - 使用更可靠的方法
            cmd = f"hdfs dfsadmin -report"
            success, output = self.run_hdfs_command(cmd)

            if success:
                lines = output.split('\n')
                for line in lines:
                    if 'Live datanodes' in line:
                        fs_info['live_datanodes'] = line.split(':')[-1].strip()
                    elif 'Configured Capacity' in line:
                        fs_info['configured_capacity'] = line.split(':')[-1].strip()
                    elif 'Present Capacity' in line:
                        fs_info['present_capacity'] = line.split(':')[-1].strip()
                    elif 'DFS Used' in line:
                        fs_info['dfs_used'] = line.split(':')[-1].strip()
                    elif 'Non DFS Used' in line:
                        fs_info['non_dfs_used'] = line.split(':')[-1].strip()

            # 如果没找到，尝试简化查询
            if 'live_datanodes' not in fs_info:
                cmd = f"hdfs dfsadmin -report | head -20"
                success, output = self.run_hdfs_command(cmd)
                if success:
                    lines = output.split('\n')
                    for line in lines:
                        if 'Live datanodes' in line:
                            fs_info['live_datanodes'] = line.split(':')[-1].strip()

            # 检查Paimon目录使用情况 - 使用更精确的方法
            cmd = f"hdfs dfs -du -s {self.paimon_warehouse}"
            success, output = self.run_hdfs_command(cmd)

            if success:
                parts = output.split()
                if len(parts) >= 2:
                    try:
                        total_bytes = int(parts[0])
                        fs_info['paimon_total_bytes'] = total_bytes
                        fs_info['paimon_total_size_mb'] = round(total_bytes / (1024 * 1024), 2)
                        fs_info['paimon_total_size_gb'] = round(total_bytes / (1024 * 1024 * 1024), 2)
                    except ValueError:
                        logger.warning(f"无法解析Paimon目录大小: {output}")
            else:
                # 尝试另一种方法
                cmd = f"hdfs dfs -count -q {self.paimon_warehouse}"
                success, output = self.run_hdfs_command(cmd)
                if success:
                    parts = output.split()
                    if len(parts) >= 4:
                        try:
                            total_bytes = int(parts[2])  # 第三个字段是字节数
                            fs_info['paimon_total_bytes'] = total_bytes
                            fs_info['paimon_total_size_mb'] = round(total_bytes / (1024 * 1024), 2)
                            fs_info['paimon_total_size_gb'] = round(total_bytes / (1024 * 1024 * 1024), 2)
                        except ValueError:
                            pass

            # 检查snapshot文件数量 - 使用更精确的方法
            cmd = f"hdfs dfs -count {self.paimon_warehouse}/{self.ods_database}/*/snapshot 2>/dev/null | wc -l"
            success, output = self.run_hdfs_command(cmd)
            if success and output.strip().isdigit():
                fs_info['tables_with_snapshots'] = int(output.strip())

            # 获取总snapshot文件数
            cmd = f"hdfs dfs -ls -R {self.paimon_warehouse} 2>/dev/null | grep -c 'snapshot-'"
            success, output = self.run_hdfs_command(cmd)
            if success and output.strip().isdigit():
                fs_info['total_snapshot_files'] = int(output.strip())

            logger.info(f"文件系统状态: {fs_info.get('live_datanodes', 'N/A')}个活跃节点, "
                        f"Paimon大小: {fs_info.get('paimon_total_size_gb', 0):.2f}GB, "
                        f"Snapshot文件: {fs_info.get('total_snapshot_files', 0)}个")

        except Exception as e:
            logger.error(f"检查文件系统健康状态异常: {e}")

        return fs_info

    def check_kafka_topics(self) -> Dict:
        """检查Kafka Topic状态"""
        logger.info("检查Kafka Topic状态...")
        topics_info = {}

        topics_to_check = [
            "client_log_events",
            "server_biz_events",
            "external_data_events"
        ]

        for topic in topics_to_check:
            # 检查Topic是否存在
            cmd = f"kafka-topics.sh --bootstrap-server {self.kafka_bootstrap} --list | grep -w {topic}"
            success, output = self.run_hdfs_command(cmd)

            if success and topic in output:
                # 获取Topic详情
                cmd = f"kafka-topics.sh --bootstrap-server {self.kafka_bootstrap} --describe --topic {topic}"
                success, details = self.run_hdfs_command(cmd)

                if success:
                    # 解析分区信息
                    partitions = 0
                    for line in details.split('\n'):
                        if 'Partition:' in line:
                            partitions += 1

                    # 获取消息量（估算）
                    cmd = f"kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server {self.kafka_bootstrap} --topic {topic} --time -1"
                    success, offset_info = self.run_hdfs_command(cmd)

                    total_messages = 0
                    if success:
                        for line in offset_info.split('\n'):
                            if ':' in line:
                                try:
                                    offset = int(line.split(':')[-1])
                                    total_messages += offset
                                except:
                                    pass

                    topics_info[topic] = {
                        "status": "ACTIVE",
                        "partitions": partitions,
                        "estimated_messages": total_messages,
                        "description": f"{topic} Topic状态"
                    }

                    logger.info(f"  {topic}: {partitions}分区, 约{total_messages}条消息")
            else:
                topics_info[topic] = {
                    "status": "NOT_FOUND",
                    "description": f"{topic} Topic状态",
                    "message": "Topic不存在"
                }

        return topics_info

    def generate_summary_report(self,
                                table_status: Dict,
                                data_consistency: Dict,
                                cdc_lag: Dict,
                                fs_health: Dict,
                                kafka_status: Dict) -> Dict:
        """生成汇总报告"""
        logger.info("生成监控汇总报告...")

        summary = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "HEALTHY",
            "component_status": {},
            "warnings": [],
            "errors": []
        }

        # 检查表状态
        table_warnings = 0
        table_errors = 0
        for table_name, info in table_status.items():
            if info.get("status") == "WARNING":
                table_warnings += 1
                summary["warnings"].append(f"表 {table_name}: {info.get('warning', '未知警告')}")
            elif info.get("status") == "ERROR":
                table_errors += 1
                summary["errors"].append(f"表 {table_name}: {info.get('message', '未知错误')}")
            elif info.get("status") == "NOT_EXIST":
                table_errors += 1
                summary["errors"].append(f"表 {table_name}: 不存在")

        summary["component_status"]["tables"] = {
            "total": len(table_status),
            "healthy": len(table_status) - table_warnings - table_errors,
            "warnings": table_warnings,
            "errors": table_errors
        }

        # 检查数据一致性
        consistency_failures = sum(1 for check in data_consistency.values()
                                   if check.get("status") == "FAIL")
        if consistency_failures > 0:
            summary["overall_status"] = "WARNING"
            summary["warnings"].append(f"数据一致性检查失败: {consistency_failures}项")

        summary["component_status"]["data_consistency"] = {
            "total_checks": len(data_consistency),
            "failed_checks": consistency_failures
        }

        # 检查CDC延迟
        cdc_warnings = sum(1 for info in cdc_lag.values()
                           if info.get("status") == "WARNING")
        cdc_errors = sum(1 for info in cdc_lag.values()
                         if info.get("status") == "ERROR")

        # 详细记录CDC错误信息
        cdc_error_details = []
        for table_name, info in cdc_lag.items():
            if info.get("status") == "ERROR":
                cdc_error_details.append(f"{info.get('description', table_name)}: {info.get('message', '未知错误')}")
            elif info.get("status") == "WARNING":
                cdc_error_details.append(f"{info.get('description', table_name)}: {info.get('message', '未知警告')}")

        if cdc_warnings > 0:
            summary["overall_status"] = "WARNING"
            summary["warnings"].append(f"CDC同步延迟警告: {cdc_warnings}个表")
            for detail in cdc_error_details:
                summary["warnings"].append(f"  - {detail}")

        if cdc_errors > 0:
            summary["overall_status"] = "ERROR"
            summary["errors"].append(f"CDC同步错误: {cdc_errors}个表")
            for detail in cdc_error_details:
                summary["errors"].append(f"  - {detail}")

        summary["component_status"]["cdc_sync"] = {
            "total_tables": len(cdc_lag),
            "warnings": cdc_warnings,
            "errors": cdc_errors,
            "details": cdc_error_details
        }

        # Kafka状态
        kafka_issues = sum(1 for info in kafka_status.values()
                           if info.get("status") != "ACTIVE")
        if kafka_issues > 0:
            summary["overall_status"] = "WARNING"
            summary["warnings"].append(f"Kafka Topic问题: {kafka_issues}个")

        summary["component_status"]["kafka"] = {
            "total_topics": len(kafka_status),
            "inactive_topics": kafka_issues
        }

        # 文件系统信息
        summary["component_status"]["filesystem"] = fs_health

        # 生成建议
        summary["recommendations"] = self.generate_recommendations(
            table_status, data_consistency, cdc_lag, kafka_status
        )

        return summary

    def generate_recommendations(self, table_status, data_consistency, cdc_lag, kafka_status):
        """生成改进建议"""
        recommendations = []

        # 检查缺失的表
        missing_tables = []
        for name, info in table_status.items():
            if info.get("status") == "NOT_EXIST":
                missing_tables.append(name)

        if missing_tables:
            recommendations.append(f"创建缺失的表: {', '.join(missing_tables)}")

        # 检查数据一致性
        failed_checks = []
        for name, info in data_consistency.items():
            if info.get("status") == "FAIL":
                failed_checks.append(name)

        if failed_checks:
            recommendations.append(f"修复数据一致性: {', '.join(failed_checks)}")

        # 检查CDC延迟
        cdc_problems = []
        for name, info in cdc_lag.items():
            if info.get("status") in ["WARNING", "ERROR"]:
                cdc_problems.append(f"{info.get('description', name)}: {info.get('message', '')}")

        if cdc_problems:
            recommendations.append("解决CDC同步问题:")
            for problem in cdc_problems[:3]:  # 只显示前3个问题
                recommendations.append(f"  - {problem}")
            if len(cdc_problems) > 3:
                recommendations.append(f"  ... 还有 {len(cdc_problems) - 3} 个CDC问题")

        # 检查Kafka
        inactive_topics = []
        for name, info in kafka_status.items():
            if info.get("status") != "ACTIVE":
                inactive_topics.append(name)

        if inactive_topics:
            recommendations.append(f"修复Kafka Topic: {', '.join(inactive_topics)}")

        # 通用建议
        if not recommendations:
            recommendations.append("系统运行正常，建议定期备份snapshot")
            recommendations.append("监控磁盘使用情况，及时清理旧数据")
        else:
            recommendations.append("检查MySQL updated_time字段是否被正确更新")
            recommendations.append("验证Flink CDC作业是否正常运行")

        return recommendations

    def save_report(self, report: Dict, output_dir: str = "reports"):
        """保存监控报告"""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/ods_monitor_report_{timestamp}.json"

        try:
            with open(filename, 'w') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
            logger.info(f"报告已保存到: {filename}")

            # 同时保存简版报告
            simple_filename = f"{output_dir}/latest_report.json"
            with open(simple_filename, 'w') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)

            return filename
        except Exception as e:
            logger.error(f"保存报告失败: {e}")
            return None

    def print_report_summary(self, report: Dict):
        """打印报告摘要"""
        print("\n" + "=" * 80)
        print("ODS层监控报告摘要")
        print("=" * 80)

        print(f"生成时间: {report.get('timestamp', 'N/A')}")
        print(f"总体状态: {report.get('overall_status', 'UNKNOWN')}")

        print(f"\n组件状态:")
        for component, status in report.get('component_status', {}).items():
            if isinstance(status, dict):
                if component == 'cdc_sync' and 'details' in status:
                    print(f"  {component}: total={status.get('total_tables')}, "
                          f"healthy={status.get('total_tables') - status.get('warnings', 0) - status.get('errors', 0)}, "
                          f"warnings={status.get('warnings', 0)}, errors={status.get('errors', 0)}")
                else:
                    print(f"  {component}: {status}")
            else:
                print(f"  {component}: {status}")

        if report.get('warnings'):
            print(f"\n警告 ({len(report['warnings'])}个):")
            for warning in report['warnings'][:10]:  # 显示前10个
                print(f"  ⚠  {warning}")
            if len(report['warnings']) > 10:
                print(f"  ... 还有 {len(report['warnings']) - 10} 个警告")

        if report.get('errors'):
            print(f"\n错误 ({len(report['errors'])}个):")
            for error in report['errors'][:10]:  # 显示前10个
                print(f"  ✗ {error}")
            if len(report['errors']) > 10:
                print(f"  ... 还有 {len(report['errors']) - 10} 个错误")

        if report.get('recommendations'):
            print(f"\n建议 ({len(report['recommendations'])}条):")
            for rec in report['recommendations']:
                print(f"  → {rec}")

        print("\n" + "=" * 80)

    def run_monitoring_cycle(self):
        """运行一个完整的监控周期"""
        logger.info(f"开始监控周期 - {datetime.now()}")

        try:
            # 1. 检查Paimon表状态
            table_status = self.check_paimon_tables_status()

            # 2. 检查数据一致性
            data_consistency = self.check_data_consistency()

            # 3. 检查CDC同步延迟
            cdc_lag = self.check_cdc_sync_lag()

            # 4. 检查文件系统健康
            fs_health = self.check_file_system_health()

            # 5. 检查Kafka状态
            kafka_status = self.check_kafka_topics()

            # 6. 生成汇总报告
            summary = self.generate_summary_report(
                table_status, data_consistency, cdc_lag, fs_health, kafka_status
            )

            # 7. 保存报告
            report_file = self.save_report(summary)

            # 8. 打印摘要
            self.print_report_summary(summary)

            # 9. 检查是否需要报警
            self.check_alerts(summary)

            logger.info(f"监控周期完成 - {datetime.now()}")
            return True

        except Exception as e:
            logger.error(f"监控周期执行失败: {e}", exc_info=True)
            return False

    def check_alerts(self, report: Dict):
        """检查是否需要发送报警"""
        if report.get('overall_status') == 'ERROR':
            logger.error("系统状态错误，需要立即处理！")
            # 这里可以集成邮件、钉钉、企业微信等报警
            self.send_alert("ERROR", report)
        elif report.get('overall_status') == 'WARNING':
            logger.warning("系统状态警告，需要注意！")
            self.send_alert("WARNING", report)

    def send_alert(self, level: str, report: Dict):
        """发送报警（示例）"""
        alert_message = f"[{level}] ODS监控报警\n"
        alert_message += f"时间: {report.get('timestamp')}\n"
        alert_message += f"状态: {report.get('overall_status')}\n"

        if report.get('errors'):
            alert_message += f"错误数: {len(report['errors'])}\n"
            for error in report['errors'][:3]:
                alert_message += f"- {error}\n"

        if report.get('warnings'):
            alert_message += f"警告数: {len(report['warnings'])}\n"
            for warning in report['warnings'][:3]:
                alert_message += f"- {warning}\n"

        logger.info(f"报警消息:\n{alert_message}")
        # 实际发送报警的代码（根据您的报警系统实现）

    def debug_snapshot_issue(self):
        """调试snapshot读取问题"""
        print("\n=== 调试Snapshot读取问题 ===")

        test_tables = [
            ("ods_dim_user", "用户表"),
            ("ods_dim_product", "商品表")
        ]

        for table_name, table_desc in test_tables:
            print(f"\n调试表: {table_desc} ({table_name})")

            table_path = f"{self.paimon_warehouse}/{self.ods_database}/{table_name}"
            hdfs_path = f"hdfs://{self.hdfs_namenode}{table_path}"
            snapshot_path = f"{hdfs_path}/snapshot"

            print(f"  HDFS路径: {snapshot_path}")

            # 1. 检查目录是否存在
            cmd = f"hdfs dfs -test -d {snapshot_path} && echo '存在' || echo '不存在'"
            success, output = self.run_hdfs_command(cmd)
            print(f"  Snapshot目录: {output}")

            # 2. 列出文件
            cmd = f"hdfs dfs -ls {snapshot_path} 2>/dev/null | head -5"
            success, output = self.run_hdfs_command(cmd)
            print(f"  文件列表（前5个）:")
            if success and output:
                for line in output.split('\n'):
                    if line.strip():
                        print(f"    {line}")
            else:
                print(f"    无法列出文件或目录为空")

            # 3. 尝试读取一个文件
            cmd = f"hdfs dfs -ls {snapshot_path}/snapshot-* 2>/dev/null | head -1"
            success, snapshot_file = self.run_hdfs_command(cmd)

            if success and snapshot_file:
                parts = snapshot_file.split()
                if len(parts) >= 8:
                    filename = parts[-1]
                    print(f"  测试读取文件: {filename}")

                    # 尝试多种读取方法
                    methods = [
                        ("hdfs dfs -cat", f"hdfs dfs -cat {snapshot_path}/{filename} 2>/dev/null | head -10"),
                        ("hadoop fs -cat", f"hadoop fs -cat {snapshot_path}/{filename} 2>/dev/null | head -10"),
                        ("hdfs dfs -text", f"hdfs dfs -text {snapshot_path}/{filename} 2>/dev/null | head -10")
                    ]

                    for method_name, method_cmd in methods:
                        success, content = self.run_hdfs_command(method_cmd)
                        if success and content:
                            print(f"    使用 {method_name} 成功，内容预览:")
                            print(f"      {content[:200]}")
                            break
                        else:
                            print(f"    使用 {method_name} 失败")
            else:
                print(f"  未找到snapshot文件")

        print("\n=== 调试完成 ===")


def main():
    """主函数"""
    print("""
    ███████╗██╗   ██╗███████╗████████╗███████╗███╗   ███╗
    ██╔════╝██║   ██║██╔════╝╚══██╔══╝██╔════╝████╗ ████║
    █████╗  ██║   ██║███████╗   ██║   █████╗  ██╔████╔██║
    ██╔══╝  ██║   ██║╚════██║   ██║   ██╔══╝  ██║╚██╔╝██║
    ██║     ╚██████╔╝███████║   ██║   ███████╗██║ ╚═╝ ██║
    ╚═╝      ╚═════╝ ╚══════╝   ╚═╝   ╚══════╝╚═╝     ╚═╝
    """)

    # 解析命令行参数
    parser = argparse.ArgumentParser(description='Paimon ODS层监控与验证工具')
    parser.add_argument('--config', type=str, default='config.json', help='配置文件路径')
    parser.add_argument('--interval', type=int, default=30, help='监控间隔（秒）')
    parser.add_argument('--single-run', action='store_true', help='单次运行模式')
    parser.add_argument('--output-dir', type=str, default='reports', help='报告输出目录')
    parser.add_argument('--debug-cdc', action='store_true', help='调试CDC问题')
    parser.add_argument('--test-mysql', action='store_true', help='测试MySQL连接')
    parser.add_argument('--test-hdfs', action='store_true', help='测试HDFS连接')
    parser.add_argument('--debug-snapshot', action='store_true', help='调试snapshot读取问题')

    args = parser.parse_args()

    # 创建监控器
    monitor = ODSMonitor(args.config)
    if args.interval:
        monitor.monitor_interval = args.interval

    print(f"监控配置:")
    print(f"  监控间隔: {monitor.monitor_interval}秒")
    print(f"  HDFS: {monitor.hdfs_namenode}")
    print(f"  MySQL: {monitor.mysql_host}:{monitor.mysql_port}/{monitor.mysql_database}")
    print(f"  Kafka: {monitor.kafka_bootstrap}")
    print(f"  报告目录: {args.output_dir}")
    print("-" * 60)

    # 调试选项
    if args.debug_cdc:
        monitor.debug_cdc_issue()
        return

    if args.test_mysql:
        monitor.test_mysql_connection()
        return

    if args.test_hdfs:
        print("测试HDFS连接...")
        path = f"hdfs://{monitor.hdfs_namenode}{monitor.paimon_warehouse}"
        cmd = f"hdfs dfs -ls -R {path} | head -20"
        success, output = monitor.run_hdfs_command(cmd)
        if success:
            print("HDFS目录内容（前20行）:")
            for line in output.split('\n'):
                print(f"  {line}")
        else:
            print(f"HDFS访问失败: {output}")
        return

    if args.debug_snapshot:
        monitor.debug_snapshot_issue()
        return

    if args.single_run:
        # 单次运行模式
        print("执行单次监控检查...")
        monitor.run_monitoring_cycle()
    else:
        # 持续监控模式
        print(f"开始持续监控，每{monitor.monitor_interval}秒执行一次...")
        print("按 Ctrl+C 停止监控\n")

        cycle_count = 0
        try:
            while True:
                cycle_count += 1
                print(f"\n第 {cycle_count} 次监控检查 ({datetime.now().strftime('%H:%M:%S')})")
                print("-" * 60)

                success = monitor.run_monitoring_cycle()

                if not success:
                    logger.error("监控检查失败，等待下一个周期")

                time.sleep(monitor.monitor_interval)

        except KeyboardInterrupt:
            print("\n监控已停止")
        except Exception as e:
            logger.error(f"监控程序异常退出: {e}", exc_info=True)
            sys.exit(1)


if __name__ == "__main__":
    main()