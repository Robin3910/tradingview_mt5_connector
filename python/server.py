# -*- coding: utf-8 -*-


"""
TradingView-MT5 Bridge - Python Flask Server
接收 TradingView 警报并转发到 MT5 EA
支持 WebSocket 实时推送和心跳检测
EA 主动连接服务器
"""

import logging
import yaml
import json
import threading
import time
import socket
import selectors
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from flask_socketio import SocketIO, emit

from message_handler import MessageHandler

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

# EA 连接服务器配置
socket_server_config = {
    'host': '0.0.0.0',
    'port': 9000
}
socket_server = None
socket_server_thread = None


def load_config():
    """加载配置文件"""
    global config
    try:
        with open('../config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        logger.info("配置文件加载成功")
        return config
    except Exception as e:
        logger.error(f"配置文件加载失败: {e}")
        return None


class EAClient:
    """EA 客户端连接"""
    
    def __init__(self, conn_id, conn, addr, message_handler):
        self.conn_id = conn_id
        self.conn = conn
        self.addr = addr
        self.message_handler = message_handler
        self.buffer = ""
        self.last_heartbeat = None
        self.last_message_time = None
        self.reconnect_count = 0
        self._connected = True
        self._lock = threading.Lock()
        
        # 账户信息
        self.account_info = {
            'account_id': '',
            'account_name': '',
            'account_server': '',
            'balance': 0,
            'equity': 0,
            'positions_count': 0
        }
        
    def is_connected(self):
        with self._lock:
            return self._connected
            
    def set_connected(self, value):
        with self._lock:
            self._connected = value
            
    def get_account_info(self):
        return self.account_info.copy()
    
    def send_message(self, message):
        """发送消息到 EA"""
        try:
            if not self.is_connected():
                return False
            self.conn.sendall((message + "\n").encode('utf-8'))
            self.last_message_time = datetime.now()
            return True
        except Exception as e:
            logger.error(f"[{self.conn_id}] 发送消息失败: {e}")
            self.set_connected(False)
            return False
    
    def handle_message(self, message):
        """处理收到的消息"""
        self.last_message_time = datetime.now()
        
        parts = message.strip().split('|')
        if not parts:
            return
            
        cmd = parts[0]
        
        if cmd == "REGISTER":
            # EA 注册: REGISTER|EaBridge|version|magic_number
            if len(parts) >= 4:
                logger.info(f"[{self.conn_id}] EA 注册: version={parts[2]}, magic={parts[3]}")
                self.send_message("REGISTER_ACK")
                
        elif cmd == "ACCOUNT_INFO":
            Print("接收到账号信息，开始处理....")
            # 账户信息: ACCOUNT_INFO|login|name|server|balance|equity|pos_count
            if len(parts) >= 7:
                self.account_info['account_id'] = parts[1]
                self.account_info['account_name'] = parts[2]
                self.account_info['account_server'] = parts[3]
                self.account_info['balance'] = float(parts[4]) if parts[4] else 0
                self.account_info['equity'] = float(parts[5]) if parts[5] else 0
                self.account_info['positions_count'] = int(parts[6]) if parts[6] else 0
                logger.info(f"[{self.conn_id}] 账户信息: {parts[1]} - {parts[2]}")
                
        elif cmd == "HEARTBEAT":
            # 心跳: HEARTBEAT|timestamp
            self.last_heartbeat = datetime.now()
            self.send_message("PONG")
            
        elif cmd == "ACK":
            # 确认消息: ACK|SEQ|data
            logger.info(f"[{self.conn_id}] 收到 ACK: {message}")
            
        elif cmd == "ERROR":
            # 错误消息: ERROR|SEQ|code|message
            logger.warning(f"[{self.conn_id}] 收到错误: {message}")
            
        elif cmd == "PONG":
            logger.debug(f"[{self.conn_id}] 收到 PONG 响应")
            
        else:
            logger.debug(f"[{self.conn_id}] 未知命令: {cmd}")
    
    def receive_loop(self):
        """接收消息循环"""
        logger.info(f"[{self.conn_id}] 开始接收消息, 来自: {self.addr}")
        self.last_heartbeat = datetime.now()
        
        empty_count = 0
        while self.is_connected():
            try:
                data = self.conn.recv(4096)
                recv_len = len(data)
                logger.info(f"收到数据：{data}|长度为：{recv_len}")
                if not data:
                    empty_count += 1
                    logger.info(f"[{self.conn_id}] recv() 返回空数据（第{empty_count}次）, 连续空数据次数={empty_count}")
                    if empty_count >= 5:
                        logger.info(f"[{self.conn_id}] 连续5次收到空数据，判定连接已关闭")
                        break
                else:
                    empty_count = 0
                    
                data_str = data.decode('utf-8', errors='ignore')
                logger.info(f"[{self.conn_id}] 收到原始数据: {repr(data_str)}")
                
                self.buffer += data_str
                
                # 处理缓冲区中的所有完整消息
                while '\n' in self.buffer:
                    line, self.buffer = self.buffer.split('\n', 1)
                    if line.strip():
                        logger.info(f"[{self.conn_id}] 处理消息: {line.strip()}")
                        self.handle_message(line)
                        
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"[{self.conn_id}] 接收错误: {e}")
                break
                
        logger.info(f"[{self.conn_id}] 连接关闭")
        self.set_connected(False)
        self.close()
        
    def close(self):
        """关闭连接"""
        try:
            self.conn.close()
        except:
            pass


def accept_connections(server_socket):
    """接受 EA 连接"""
    global mt5_clients
    
    server_socket.settimeout(1.0)
    
    while running:
        try:
            conn, addr = server_socket.accept()
            logger.info(f"========== 收到 EA 连接 ==========")
            logger.info(f"  连接来源: {addr}")
            logger.info(f"  对端IP: {addr[0]}, 端口: {addr[1]}")
            logger.info(f"===================================")
            
            # 生成连接 ID
            conn_id = f"EA_{addr[0]}_{addr[1]}"
            
            # 创建 EA 客户端
            ea_client = EAClient(conn_id, conn, addr, message_handler)
            
            with client_lock:
                mt5_clients[conn_id] = ea_client
                
            # 启动接收线程
            threading.Thread(target=ea_client.receive_loop, daemon=True).start()
            
            # 广播状态
            broadcast_status()
            
        except socket.timeout:
            continue
        except Exception as e:
            if running:
                logger.error(f"接受连接错误: {e}")


def start_socket_server():
    """启动 EA 连接服务器"""
    global socket_server
    
    host = socket_server_config['host']
    port = socket_server_config['port']
    
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen(10)
        socket_server = server
        
        logger.info(f"EA Socket 服务器启动: {host}:{port}")
        logger.info(f"等待 EA 连接中...")
        
        accept_connections(server)
        
    except OSError as e:
        if "Address already in use" in str(e):
            logger.error(f"端口 {port} 已被占用！请检查: lsof -i:{port}")
        else:
            logger.error(f"Socket 服务器错误: {e}")
    except Exception as e:
        logger.error(f"Socket 服务器错误: {e}")


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
                'ip': f"{client.addr[0]}:{client.addr[1]}",
                'last_heartbeat': client.last_heartbeat.isoformat() if client.last_heartbeat else None,
                'last_message': client.last_message_time.isoformat() if client.last_message_time else None,
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
                disconnected = []
                
                for conn_id, client in mt5_clients.items():
                    if not client.is_connected():
                        disconnected.append(conn_id)
                        continue
                        
                    # 检查心跳超时
                    if client.last_heartbeat:
                        elapsed = (datetime.now() - client.last_heartbeat).total_seconds()
                        if elapsed > 120:  # 2分钟无心跳
                            logger.warning(f"[{conn_id}] 心跳超时")
                            
                # 移除断开的连接
                for conn_id in disconnected:
                    del mt5_clients[conn_id]

            # 广播状态
            broadcast_status()

            # 每5秒检测一次
            time.sleep(5)

        except Exception as e:
            logger.error(f"心跳检测错误: {e}")

    logger.info("心跳检测线程结束")


def init_message_handler():
    global message_handler
    message_handler = MessageHandler(config, socketio)


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
    global config, heartbeat_thread, running, socket_server_config, socket_server_thread

    config = load_config()
    if not config:
        logger.error("无法加载配置，程序退出")
        return

    # 获取 Socket 服务器配置
    socket_config = config.get('socket_server', {})
    socket_server_config['host'] = socket_config.get('host', '0.0.0.0')
    socket_server_config['port'] = socket_config.get('port', 9000)
    
    flask_config = config.get('flask', {})
    host = flask_config.get('host', '0.0.0.0')
    port = flask_config.get('port', 5000)
    debug = flask_config.get('debug', False)

    init_message_handler()

    # 启动 EA Socket 服务器（被动接收连接）
    running = True
    socket_server_thread = threading.Thread(target=start_socket_server, daemon=True)
    socket_server_thread.start()

    # 启动心跳检测线程
    heartbeat_thread = threading.Thread(target=heartbeat_checker, daemon=True)
    heartbeat_thread.start()

    logger.info("=" * 60)
    logger.info("TradingView-MT5 Bridge Server")
    logger.info(f"EA 连接端口: {socket_server_config['host']}:{socket_server_config['port']}")
    logger.info(f"仪表盘: http://{host}:{port}/")
    logger.info(f"Webhook: http://{host}:{port}/webhook")
    logger.info(f"状态API: http://{host}:{port}/status")
    logger.info("=" * 60)

    socketio.run(app, host=host, port=port, debug=debug)


if __name__ == '__main__':
    main()
