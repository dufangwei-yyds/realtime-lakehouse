import os
import time
from datetime import datetime
import schedule
from hdfs import InsecureClient

"""
数据集成模块: Nginx服务器日志数据-离线处理案例
"""

# 配置参数
LOCAL_LOG_PATH = "D:/software/nginx-1.28.0/nginx-1.28.0/logs/access.log"  # 本地日志路径 ？
HDFS_ROOT = "/user/logs/nginx"  # HDFS根目录
HDFS_NAMENODE = "http://192.168.63.128:9870"  # HDFS namenode地址
# SYNC_MARKER = "/home/dufangwei/logs/hdfs_sync_marker.txt"  # 同步位置标记文件
SYNC_MARKER = "D:/software/nginx-1.28.0/nginx-1.28.0/logs/hdfs_sync_marker.txt" # 同步位置标记文件

# 创建HDFS客户端
hdfs_client = InsecureClient(HDFS_NAMENODE)


def get_last_sync_position():
    """读取上次同步的文件位置（字节偏移量）"""
    if not os.path.exists(SYNC_MARKER):
        return 0  # 首次同步从0开始
    with open(SYNC_MARKER, "r") as f:
        return int(f.read().strip())


def update_sync_position(position):
    """更新同步位置（记录当前文件末尾）"""
    with open(SYNC_MARKER, "w") as f:
        f.write(str(position))


def sync_to_hdfs():
    """增量同步日志到HDFS（每小时执行）"""
    # 1. 计算时间分区（dt=日期, hour=小时）
    now = datetime.now()
    dt = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    hdfs_path = f"{HDFS_ROOT}/dt={dt}/hour={hour}/access.log"

    # 2. 读取本地日志增量内容
    local_file_size = os.path.getsize(LOCAL_LOG_PATH)
    last_pos = get_last_sync_position()

    if last_pos >= local_file_size:
        print("无新日志需要同步到HDFS")
        return

    # 3. 增量读取并写入HDFS
    try:
        with open(LOCAL_LOG_PATH, "rb") as local_f:
            local_f.seek(last_pos)  # 跳到上次同步位置
            content = local_f.read()  # 读取新增内容

            # 使用 HDFS 客户端写入（追加模式）
            if hdfs_client.status(hdfs_path, strict=False):
                # 文件存在，追加写入
                with hdfs_client.write(hdfs_path, append=True) as writer:
                    writer.write(content)
            else:
                # 文件不存在，新建并写入
                with hdfs_client.write(hdfs_path) as writer:
                    writer.write(content)

            print(f"同步成功：本地[{last_pos}-{local_file_size}]  HDFS {hdfs_path}")

            # 更新同步标记（记录当前文件大小）
            update_sync_position(local_file_size)
    except Exception as e:
        print(f"同步失败：{e}")


if __name__ == "__main__":
    # 立即执行一次同步（测试用）
    sync_to_hdfs()

    # 每小时执行一次同步
    schedule.every().hour.do(sync_to_hdfs)
    print("开始每小时同步日志到HDFS...")

    # 持续运行
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次任务
