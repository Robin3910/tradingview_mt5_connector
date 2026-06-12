"""
TradingView-MT5 Bridge - Python Flask Server
接收 TradingView 警报并转发到 MT5 EA
支持 WebSocket 实时推送和心跳检测
"""

import logging
import yaml
import json
import threading
import time
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit

from message_handler import MessageHandler
from mt5_socket_client import MT5SocketClient

app = Flask(__name__)
app.config['SECRET_KEY'] = 'tradingview-mt5-bridge-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

config = None
message_handler = None
mt5_clients = {}
client_lock = threading.Lock()
heartbeat_thread = None
running = True


def load_config():
    global config
    try:
        with open('../config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info("配置文件加载成功")
        return config
    except Exception as e:
        logger.error(f"配置文件加载失败: {e}")
        return None


def broadcast_status():
    """广播连接状态给所有WebSocket客户端"""
    status_data = {
        'server_time': datetime.now().isoformat(),
        'mt5_connections': {},
        'total_connections': 0,
        'active_connections': 0
    }

    with client_lock:
        for conn_id, client in mt5_clients.items():
            is_connected = client.is_connected()
            account_info = client.get_account_info()

            status_data['mt5_connections'][conn_id] = {
                'connected': is_connected,
                'ip': client.host,
                'port': client.port,
                'last_heartbeat': client.last_heartbeat,
                'last_message': client.last_message_time,
                'account_id': account_info.get('account_id', ''),
                'account_name': account_info.get('account_name', ''),
                'account_server': account_info.get('account_server', ''),
                'balance': account_info.get('balance', 0),
                'equity': account_info.get('equity', 0),
                'positions_count': account_info.get('positions_count', 0),
                'reconnect_count': client.reconnect_count
            }

            if is_connected:
                status_data['active_connections'] += 1

        status_data['total_connections'] = len(mt5_clients)

    socketio.emit('status_update', status_data)
    return status_data


def heartbeat_checker():
    """心跳检测线程"""
    global running

    logger.info("心跳检测线程启动")
    while running:
        try:
            with client_lock:
                for conn_id, client in mt5_clients.items():
                    if client.is_connected():
                        # 发送心跳
                        if client.send_heartbeat():
                            logger.debug(f"[{conn_id}] 心跳发送成功")
                        else:
                            logger.warning(f"[{conn_id}] 心跳发送失败")

            # 广播状态
            broadcast_status()

            # 每5秒检测一次
            time.sleep(5)

        except Exception as e:
            logger.error(f"心跳检测错误: {e}")

    logger.info("心跳检测线程结束")


def init_mt5_clients():
    global mt5_clients, message_handler

    message_handler = MessageHandler(config, socketio)

    for conn in config.get('mt5_connections', []):
        client = MT5SocketClient(
            conn_id=conn['id'],
            host=conn['ip'],
            port=conn['port'],
            message_handler=message_handler,
            socketio=socketio
        )
        mt5_clients[conn['id']] = client
        threading.Thread(target=client.connect, daemon=True).start()
        logger.info(f"MT5 连接初始化: {conn['id']} ({conn['ip']}:{conn['port']})")


@app.route('/')
def index():
    """仪表盘首页"""
    return render_template('dashboard.html')


@app.route('/webhook', methods=['POST'])
def webhook():
    """接收 TradingView 警报"""
    try:
        data = request.get_json()

        if not data:
            logger.warning("收到空请求")
            return jsonify({'error': 'Empty request'}), 400

        logger.info(f"收到 TradingView 警报: {json.dumps(data)}")

        if config.get('security', {}).get('api_token'):
            token = request.headers.get('Authorization', '').replace('Bearer ', '')
            if token != config['security']['api_token']:
                logger.warning(f"Token 验证失败")
                return jsonify({'error': 'Unauthorized'}), 401

        allowed_ips = config.get('security', {}).get('allowed_ips', [])
        if allowed_ips:
            client_ip = request.remote_addr
            if client_ip not in allowed_ips:
                logger.warning(f"IP {client_ip} 不在白名单中")
                return jsonify({'error': 'IP not allowed'}), 403

        result = message_handler.process_message(data, mt5_clients)

        if result['success']:
            socketio.emit('trade_executed', {
                'timestamp': datetime.now().isoformat(),
                'result': result
            })
            return jsonify({
                'status': 'success',
                'message': 'Order sent to MT5',
                'details': result.get('details', {})
            })
        else:
            socketio.emit('trade_error', {
                'timestamp': datetime.now().isoformat(),
                'error': result.get('error', 'Unknown error')
            })
            return jsonify({
                'status': 'error',
                'message': result.get('error', 'Unknown error')
            }), 500

    except Exception as e:
        logger.error(f"处理请求时出错: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@app.route('/status', methods=['GET'])
def status():
    """获取连接状态 (REST API)"""
    return jsonify(broadcast_status())


@app.route('/send', methods=['POST'])
def manual_send():
    """手动发送交易命令"""
    try:
        data = request.get_json()
        logger.info(f"收到手动交易请求: {json.dumps(data)}")

        result = message_handler.process_message(data, mt5_clients)
        return jsonify(result)

    except Exception as e:
        logger.error(f"手动交易请求出错: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@socketio.on('connect')
def on_connect():
    logger.info("WebSocket 客户端连接")
    emit('connected', {'status': 'ok'})
    broadcast_status()


@socketio.on('disconnect')
def on_disconnect():
    logger.info("WebSocket 客户端断开")


@socketio.on('request_status')
def on_request_status():
    broadcast_status()


def main():
    global config, heartbeat_thread, running

    config = load_config()
    if not config:
        logger.error("无法加载配置，程序退出")
        return

    flask_config = config.get('flask', {})
    host = flask_config.get('host', '0.0.0.0')
    port = flask_config.get('port', 5000)
    debug = flask_config.get('debug', False)

    init_mt5_clients()

    # 启动心跳检测线程
    running = True
    heartbeat_thread = threading.Thread(target=heartbeat_checker, daemon=True)
    heartbeat_thread.start()

    logger.info("=" * 60)
    logger.info("TradingView-MT5 Bridge Server")
    logger.info(f"仪表盘: http://{host}:{port}/")
    logger.info(f"Webhook: http://{host}:{port}/webhook")
    logger.info(f"状态API: http://{host}:{port}/status")
    logger.info("=" * 60)

    socketio.run(app, host=host, port=port, debug=debug)


if __name__ == '__main__':
    main()
