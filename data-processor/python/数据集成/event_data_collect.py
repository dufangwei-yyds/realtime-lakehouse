from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer
import json
import logging
import time

"""
数据集成模块: 用户行为埋点数据
"""

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 处理跨域
app = Flask(__name__)
CORS(app)

# 配置Kafka生产者
try:
    producer = KafkaProducer(
        bootstrap_servers=['192.168.63.128:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3,
        linger_ms=10,
        max_block_ms=3000,
    )
    logger.info("Kafka producer initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize Kafka producer: {e}")
    producer = None


# 健康检查
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"}), 200


# 埋点数据接收
@app.route('/api/track/event', methods=['POST', 'OPTIONS'])
def track_event():
    # 处理OPTIONS预检请求
    if request.method == 'OPTIONS':
        return jsonify({"status": "ok"}), 200

    try:
        data = request.json

        # 验证必填字段
        required_fields = ['event_type', 'timestamp', 'product_id']
        for field in required_fields:
            if field not in data:
                logger.warning(f"Missing required field: {field}")
                return jsonify({"error": f"Missing {field}"}), 400

        # 添加额外信息
        data['received_at'] = int(time.time() * 1000)
        data['ip_address'] = request.remote_addr

        # 发送到Kafka
        if producer:
            future = producer.send('tracking_events', data)
            # 等待确认(生产环境注释,影响性能)
            result = future.get(timeout=5)
            logger.info(f"Event sent to Kafka: {data['event_type']}")
            return jsonify({"status": "success"}), 200
        else:
            logger.error("Kafka producer not initialized")
            return jsonify({"error": "Internal server error"}), 500

    except json.JSONDecodeError:
        logger.warning("Invalid JSON format in request")
        return jsonify({"error": "Invalid JSON"}), 400
    except Exception as e:
        logger.error(f"Failed to process event: {str(e)}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)