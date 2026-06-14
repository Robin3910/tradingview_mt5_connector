# -*- coding: utf-8 -*-


"""
MT5 Socket 客户端模块
管理与 MT5 EA 的 TCP Socket 连接，支持心跳检测
"""

import socket
import threading
import logging
import time
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class MT5SocketClient:
    """MT5 Socket 连接客户端"""

    HEARTBEAT_INTERVAL = 30  # 心跳间隔（秒）

    def __init__(self, conn_id, host, port, message_handler, socketio=None, reconnect_interval=5):
        self.conn_id = conn_id
        self.host = host
        self.port = port
        self.message_handler = message_handler
        self.socketio = socketio
        self.reconnect_interval = reconnect_interval

        self.socket = None
        self.connected = False
        self.running = False
        self.lock = threading.Lock()

        self.last_heartbeat = None
        self.last_message_time = None
        self.sequence = 0
        self.reconnect_count = 0

        self.receive_thread = None
        self.heartbeat_thread = None

        # MT5 账户信息
        self._account_info = {
            'account_id': '',
            'account_name': '',
            'account_server': '',
            'balance': 0,
            'equity': 0,
            'positions_count': 0
        }

    def is_connected(self):
        with self.lock:
            return self.connected

    def get_account_info(self):
        with self.lock:
            return self._account_info.copy()

    def update_account_info(self, info):
        with self.lock:
            self._account_info.update(info)
            self.last_heartbeat = datetime.now().isoformat()

        logger.info(f"[{self.conn_id}] 账户信息更新: {info}")

    def connect(self):
        self.running = True
        while self.running:
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(30)
                self.socket.connect((self.host, self.port))

                with self.lock:
                    self.connected = True

                logger.info(f"[{self.conn_id}] 连接成功: {self.host}:{self.port}")

                # 发送注册消息，包含连接ID
                self._send_register()

                self._start_receive()
                self._start_heartbeat()

            except socket.timeout:
                logger.warning(f"[{self.conn_id}] 连接超时")
            except ConnectionRefusedError:
                logger.warning(f"[{self.conn_id}] 连接被拒绝")
            except Exception as e:
                logger.error(f"[{self.conn_id}] 连接错误: {e}")

            with self.lock:
                self.connected = False

            self.reconnect_count += 1
            logger.info(f"[{self.conn_id}] {self.reconnect_interval} 秒后尝试重连...")

            for _ in range(self.reconnect_interval):
                if not self.running:
                    break
                time.sleep(1)

    def _send_register(self):
        """发送注册消息"""
        register_msg = f"REGISTER|{self.conn_id}\n"
        try:
            self.socket.sendall(register_msg.encode('utf-8'))
            logger.info(f"[{self.conn_id}] 注册消息已发送")
        except Exception as e:
            logger.error(f"[{self.conn_id}] 注册消息发送失败: {e}")

    def _start_receive(self):
        self.receive_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self.receive_thread.start()

    def _start_heartbeat(self):
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _heartbeat_loop(self):
        """心跳循环"""
        while self.running and self.connected:
            time.sleep(self.HEARTBEAT_INTERVAL)
            if self.connected:
                self.send_heartbeat()

    def send_heartbeat(self):
        """发送心跳包"""
        with self.lock:
            if not self.connected or not self.socket:
                return False

            try:
                heartbeat = f"HEARTBEAT|{datetime.now().isoformat()}\n"
                self.socket.sendall(heartbeat.encode('utf-8'))
                return True
            except Exception as e:
                logger.error(f"[{self.conn_id}] 心跳发送失败: {e}")
                self.connected = False
                return False

    def _receive_loop(self):
        buffer = ""
        while self.running and self.connected:
            try:
                data = self.socket.recv(4096)
                if not data:
                    logger.warning(f"[{self.conn_id}] 服务器关闭连接")
                    break

                buffer += data.decode('utf-8')

                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    if line:
                        self._handle_message(line)

            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"[{self.conn_id}] 接收错误: {e}")
                break

    def _handle_message(self, message):
        self.last_message_time = datetime.now().isoformat()

        try:
            parts = message.split('|')

            if len(parts) == 0:
                return

            msg_type = parts[0]

            if msg_type == 'ACK':
                logger.info(f"[{self.conn_id}] 命令确认: {parts}")

            elif msg_type == 'ERROR':
                logger.error(f"[{self.conn_id}] MT5 错误: {parts}")

            elif msg_type == 'POSITION_OPENED':
                logger.info(f"[{self.conn_id}] 订单开仓成功: {parts}")
                self._notify_socketio('position_opened', parts)

            elif msg_type == 'POSITION_CLOSED':
                logger.info(f"[{self.conn_id}] 订单平仓成功: {parts}")
                self._notify_socketio('position_closed', parts)

            elif msg_type == 'PONG':
                self.last_heartbeat = datetime.now().isoformat()
                logger.debug(f"[{self.conn_id}] 收到心跳响应")

            elif msg_type == 'HEARTBEAT':
                self.last_heartbeat = datetime.now().isoformat()
                self.send(f"PONG|{datetime.now().isoformat()}\n")

            elif msg_type == 'ACCOUNT_INFO':
                # MT5 发送账户信息
                if len(parts) >= 6:
                    account_info = {
                        'account_id': parts[1],
                        'account_name': parts[2],
                        'account_server': parts[3],
                        'balance': float(parts[4]) if parts[4] else 0,
                        'equity': float(parts[5]) if parts[5] else 0,
                        'positions_count': int(parts[6]) if len(parts) > 6 and parts[6] else 0
                    }
                    self.update_account_info(account_info)

            elif msg_type == 'REGISTER_ACK':
                logger.info(f"[{self.conn_id}] 注册确认: {parts}")
                if len(parts) >= 2:
                    logger.info(f"[{self.conn_id}] MT5 EA 版本: {parts[1]}")

            else:
                logger.debug(f"[{self.conn_id}] 收到消息: {message}")

        except Exception as e:
            logger.error(f"[{self.conn_id}] 消息处理错误: {e}")

    def _notify_socketio(self, event, data):
        """通过SocketIO通知前端"""
        if self.socketio:
            self.socketio.emit('mt5_event', {
                'connection_id': self.conn_id,
                'event': event,
                'data': data,
                'timestamp': datetime.now().isoformat()
            })

    def send(self, message):
        with self.lock:
            if not self.connected or not self.socket:
                logger.warning(f"[{self.conn_id}] 未连接，无法发送消息")
                return False

            try:
                if not message.endswith('\n'):
                    message += '\n'

                self.socket.sendall(message.encode('utf-8'))
                logger.debug(f"[{self.conn_id}] 发送: {message.strip()}")
                return True

            except Exception as e:
                logger.error(f"[{self.conn_id}] 发送失败: {e}")
                return False

    def send_order(self, symbol, order_type, volume, sl_points=0, tp_points=0, comment=""):
        self.sequence += 1
        message = f"OPEN|{symbol}|{order_type}|{volume}|{sl_points}|{tp_points}|{comment}|SEQ:{self.sequence}"
        return self.send(message)

    def close_order(self, ticket):
        self.sequence += 1
        message = f"CLOSE|{ticket}|SEQ:{self.sequence}"
        return self.send(message)

    def close_all_orders(self):
        self.sequence += 1
        message = f"CLOSE_ALL|SEQ:{self.sequence}"
        return self.send(message)

    def disconnect(self):
        self.running = False

        with self.lock:
            self.connected = False

            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass

        logger.info(f"[{self.conn_id}] 连接已断开")
