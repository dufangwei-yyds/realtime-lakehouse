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
        self.ods_database = self.config.get('ods_database', 'ods')

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
            'ods_database': 'ods',
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
        """执行MySQL查询"""
        cmd = f"mysql -h{self.mysql_host} -P{self.mysql_port} -u{self.mysql_user} -p{self.mysql_password} "
        cmd += f"-D {self.mysql_database} -e \"{query}\" -s -N"

        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode != 0:
                return False, [{"error": result.stderr.strip()}]

            # 解析结果
            lines = result.stdout.strip().split('\n')
            if not lines or lines[0] == '':
                return True, []

            # 尝试解析为JSON数组
            if lines[0].startswith('['):
                return True, json.loads(result.stdout)
            else:
                # 简单表格格式
                data = []
                for line in lines:
                    if line.strip():
                        data.append({"row": line})
                return True, data

        except Exception as e:
            return False, [{"error": str(e)}]

    def check_paimon_tables_status(self) -> Dict:
        """检查Paimon表状态"""
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

        for table_name, table_desc in tables:
            table_path = f"{self.paimon_warehouse}/{self.ods_database}/{table_name}"
            hdfs_path = f"hdfs://{self.hdfs_namenode}{table_path}"

            # 检查表目录是否存在
            cmd = f"hdfs dfs -test -d {hdfs_path} && echo 'exists'"
            success, output = self.run_hdfs_command(cmd)

            if not success or 'exists' not in output:
                results[table_name] = {
                    "status": "NOT_EXIST",
                    "description": table_desc,
                    "message": "表目录不存在"
                }
                continue

            # 检查snapshot文件
            snapshot_path = f"{hdfs_path}/snapshot"
            cmd = f"hdfs dfs -ls {snapshot_path} 2>/dev/null | grep '^-' | wc -l"
            success, output = self.run_hdfs_command(cmd)

            snapshot_count = int(output.strip()) if success and output.strip().isdigit() else 0

            # 检查manifest文件
            manifest_path = f"{hdfs_path}/manifest"
            cmd = f"hdfs dfs -test -f {manifest_path} && echo 'exists'"
            success, manifest_exists = self.run_hdfs_command(cmd)

            # 获取最新snapshot信息
            latest_snapshot = None
            if snapshot_count > 0:
                cmd = f"hdfs dfs -cat {snapshot_path}/snapshot-* 2>/dev/null | tail -1"
                success, snapshot_info = self.run_hdfs_command(cmd)
                if success and snapshot_info:
                    try:
                        latest_snapshot = json.loads(snapshot_info)
                    except:
                        latest_snapshot = {"raw": snapshot_info[:100]}

            results[table_name] = {
                "status": "HEALTHY",
                "description": table_desc,
                "snapshot_count": snapshot_count,
                "manifest_exists": 'exists' in manifest_exists if manifest_exists else False,
                "latest_snapshot": latest_snapshot,
                "hdfs_path": hdfs_path
            }

            # 检查snapshot是否过旧
            if latest_snapshot and 'commitIdentifier' in latest_snapshot:
                commit_time = latest_snapshot.get('commitIdentifier', 0)
                if isinstance(commit_time, int):
                    age_hours = (time.time() * 1000 - commit_time) / (1000 * 3600)
                    if age_hours > self.thresholds['snapshot_age_hours']:
                        results[table_name]["status"] = "WARNING"
                        results[table_name]["warning"] = f"snapshot过旧: {age_hours:.2f}小时"

            logger.info(f"  {table_desc}: {snapshot_count}个snapshot, 状态: {results[table_name]['status']}")

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
            error_count = result[0].get('error_count', 0) if isinstance(result[0], dict) else 0
            checks['order_amount_consistency'] = {
                "status": "PASS" if error_count == 0 else "FAIL",
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
            error_count = result[0].get('error_count', 0) if isinstance(result[0], dict) else 0
            checks['user_anchor_consistency'] = {
                "status": "PASS" if error_count == 0 else "FAIL",
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
            error_count = result[0].get('negative_stock_count', 0) if isinstance(result[0], dict) else 0
            checks['product_stock_validity'] = {
                "status": "PASS" if error_count == 0 else "FAIL",
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
            error_count = result[0].get('invalid_gmv_count', 0) if isinstance(result[0], dict) else 0
            checks['gmv_validity'] = {
                "status": "PASS" if error_count == 0 else "FAIL",
                "error_count": error_count,
                "description": "GMV有效性标记"
            }

        logger.info(f"数据一致性检查完成: {len(checks)}项检查")
        return checks

    def check_cdc_sync_lag(self) -> Dict:
        """检查CDC同步延迟"""
        logger.info("检查CDC同步延迟...")
        lag_info = {}

        # 检查关键表的同步延迟
        tables_to_check = [
            ("user", "ods_dim_user", "用户表"),
            ("product", "ods_dim_product", "商品表"),
            ("order", "ods_dim_order", "订单表"),
            ("live_room", "ods_dim_live_room", "直播间表")
        ]

        for source_table, target_table, table_desc in tables_to_check:
            # 获取MySQL最新更新时间
            query = f"""
            SELECT 
                MAX(updated_time) as mysql_latest,
                COUNT(*) as row_count,
                NOW() as current_time
            FROM {self.mysql_database}.{source_table}
            """

            success, mysql_result = self.run_mysql_query(query)
            if not success or not mysql_result:
                lag_info[target_table] = {
                    "status": "ERROR",
                    "description": table_desc,
                    "message": "无法获取MySQL数据"
                }
                continue

            mysql_data = mysql_result[0] if isinstance(mysql_result[0], dict) else {}
            mysql_latest_str = mysql_data.get('mysql_latest', '')

            # 获取Paimon最新更新时间
            table_path = f"{self.paimon_warehouse}/{self.ods_database}/{target_table}"
            snapshot_path = f"hdfs://{self.hdfs_namenode}{table_path}/snapshot"

            cmd = f"hdfs dfs -cat {snapshot_path}/snapshot-* 2>/dev/null | tail -1"
            success, snapshot_info = self.run_hdfs_command(cmd)

            if not success or not snapshot_info:
                lag_info[target_table] = {
                    "status": "ERROR",
                    "description": table_desc,
                    "message": "无法获取Paimon snapshot"
                }
                continue

            try:
                snapshot_data = json.loads(snapshot_info)
                paimon_latest_ts = snapshot_data.get('commitIdentifier', 0)

                # 计算延迟（秒）
                if mysql_latest_str and paimon_latest_ts > 0:
                    # 简单的时间比较（实际应该解析mysql时间字符串）
                    current_ts = time.time() * 1000
                    lag_seconds = (current_ts - paimon_latest_ts) / 1000

                    status = "HEALTHY" if lag_seconds < self.thresholds['cdc_lag_seconds'] else "WARNING"

                    lag_info[target_table] = {
                        "status": status,
                        "description": table_desc,
                        "mysql_latest": mysql_latest_str,
                        "paimon_latest_ts": datetime.fromtimestamp(paimon_latest_ts / 1000).strftime(
                            '%Y-%m-%d %H:%M:%S'),
                        "lag_seconds": round(lag_seconds, 2),
                        "row_count": mysql_data.get('row_count', 0)
                    }

                    logger.info(f"  {table_desc}: 延迟 {lag_seconds:.2f}秒, 状态: {status}")

            except Exception as e:
                lag_info[target_table] = {
                    "status": "ERROR",
                    "description": table_desc,
                    "message": f"解析数据失败: {str(e)}"
                }

        return lag_info

    def check_file_system_health(self) -> Dict:
        """检查文件系统健康状态"""
        logger.info("检查文件系统健康状态...")
        fs_info = {}

        # 检查HDFS健康状态
        cmd = f"hdfs dfsadmin -report | grep -E '(Live datanodes|Configured Capacity|Present Capacity)'"
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

        # 检查Paimon目录使用情况
        cmd = f"hdfs dfs -du -h {self.paimon_warehouse}"
        success, output = self.run_hdfs_command(cmd)

        if success:
            total_size = 0
            for line in output.split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            size = float(parts[0])
                            total_size += size
                        except:
                            pass

            fs_info['paimon_total_size_mb'] = round(total_size / (1024 * 1024), 2)
            fs_info['paimon_total_size_gb'] = round(total_size / (1024 * 1024 * 1024), 2)

        # 检查snapshot文件数量
        cmd = f"hdfs dfs -ls -R {self.paimon_warehouse} 2>/dev/null | grep 'snapshot-' | wc -l"
        success, output = self.run_hdfs_command(cmd)

        if success and output.strip().isdigit():
            fs_info['total_snapshot_files'] = int(output.strip())

        logger.info(f"文件系统状态: {fs_info.get('live_datanodes', 'N/A')}个活跃节点, "
                    f"Paimon大小: {fs_info.get('paimon_total_size_gb', 0):.2f}GB")

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

        if cdc_warnings > 0:
            summary["overall_status"] = "WARNING"
            summary["warnings"].append(f"CDC同步延迟警告: {cdc_warnings}个表")

        if cdc_errors > 0:
            summary["overall_status"] = "ERROR"
            summary["errors"].append(f"CDC同步错误: {cdc_errors}个表")

        summary["component_status"]["cdc_sync"] = {
            "total_tables": len(cdc_lag),
            "warnings": cdc_warnings,
            "errors": cdc_errors
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
        missing_tables = [name for name, info in table_status.items()
                          if info.get("status") == "NOT_EXIST"]
        if missing_tables:
            recommendations.append(f"创建缺失的表: {', '.join(missing_tables)}")

        # 检查数据一致性
        failed_checks = [name for name, info in data_consistency.items()
                         if info.get("status") == "FAIL"]
        if failed_checks:
            recommendations.append(f"修复数据一致性: {', '.join(failed_checks)}")

        # 检查CDC延迟
        high_lag_tables = [name for name, info in cdc_lag.items()
                           if info.get("status") == "WARNING"]
        if high_lag_tables:
            recommendations.append(f"优化CDC同步: {', '.join(high_lag_tables)}")

        # 检查Kafka
        inactive_topics = [name for name, info in kafka_status.items()
                           if info.get("status") != "ACTIVE"]
        if inactive_topics:
            recommendations.append(f"修复Kafka Topic: {', '.join(inactive_topics)}")

        # 通用建议
        if not recommendations:
            recommendations.append("系统运行正常，建议定期备份snapshot")
            recommendations.append("监控磁盘使用情况，及时清理旧数据")

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
                print(f"  {component}: {status}")
            else:
                print(f"  {component}: {status}")

        if report.get('warnings'):
            print(f"\n警告 ({len(report['warnings'])}个):")
            for warning in report['warnings'][:5]:  # 只显示前5个
                print(f"  ⚠  {warning}")
            if len(report['warnings']) > 5:
                print(f"  ... 还有 {len(report['warnings']) - 5} 个警告")

        if report.get('errors'):
            print(f"\n错误 ({len(report['errors'])}个):")
            for error in report['errors'][:5]:  # 只显示前5个
                print(f"  ✗ {error}")
            if len(report['errors']) > 5:
                print(f"  ... 还有 {len(report['errors']) - 5} 个错误")

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
    import argparse
    parser = argparse.ArgumentParser(description='Paimon ODS层监控与验证工具')
    parser.add_argument('--config', type=str, default='config.json', help='配置文件路径')
    parser.add_argument('--interval', type=int, default=30, help='监控间隔（秒）')
    parser.add_argument('--single-run', action='store_true', help='单次运行模式')
    parser.add_argument('--output-dir', type=str, default='reports', help='报告输出目录')

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