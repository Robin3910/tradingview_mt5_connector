# -*- coding: utf-8 -*-


"""
消息处理模块
解析 TradingView 消息并转换为 MT5 命令
"""

import logging
import json
import threading
import time

logger = logging.getLogger(__name__)


class MessageHandler:
    """TradingView 消息处理器"""

    def __init__(self, config, socketio=None):
        self.config = config
        self.trading_config = config.get('trading', {})
        self.socketio = socketio
        self.sequence = 0
        self.seq_lock = threading.Lock()
        self.pending_acks = {}
        
    def get_next_sequence(self):
        """获取下一个序列号"""
        with self.seq_lock:
            self.sequence += 1
            return self.sequence
            
    def process_message(self, data, mt5_clients):
        try:
            action = data.get('action', '').upper()

            if not action:
                return {'success': False, 'error': 'Missing action field'}

            if action not in ['BUY', 'SELL', 'CLOSE', 'CLOSE_ALL']:
                return {'success': False, 'error': f'Invalid action: {action}'}

            # 获取客户端
            account_id = data.get('account_id')
            
            if account_id:
                # 查找指定账户
                if account_id not in mt5_clients:
                    return {'success': False, 'error': f'Account {account_id} not found'}
                client = mt5_clients[account_id]
            else:
                # 使用第一个可用连接
                if not mt5_clients:
                    return {'success': False, 'error': 'No MT5 connection available'}
                client = list(mt5_clients.values())[0]
                account_id = client.conn_id

            if not client.is_connected():
                return {'success': False, 'error': f'MT5 {account_id} not connected'}

            symbol = data.get('symbol', self.trading_config.get('default_symbol', 'EURUSD'))
            volume = float(data.get('volume', self.trading_config.get('default_volume', 0.1)))
            sl_points = int(data.get('sl_points', self.trading_config.get('default_sl_points', 0)))
            tp_points = int(data.get('tp_points', self.trading_config.get('default_tp_points', 0)))
            comment = data.get('comment', 'TV_Signal')
            
            seq = self.get_next_sequence()

            if action == 'BUY':
                order_type = 0  # ORDER_TYPE_BUY
                msg = f"OPEN|{symbol}|{order_type}|{volume}|{sl_points}|{tp_points}|{comment}|SEQ:{seq}"
                
            elif action == 'SELL':
                order_type = 1  # ORDER_TYPE_SELL
                msg = f"OPEN|{symbol}|{order_type}|{volume}|{sl_points}|{tp_points}|{comment}|SEQ:{seq}"
                
            elif action == 'CLOSE':
                ticket = data.get('ticket')
                if not ticket:
                    return {'success': False, 'error': 'Missing ticket for close action'}
                msg = f"CLOSE|{ticket}|SEQ:{seq}"
                
            elif action == 'CLOSE_ALL':
                msg = f"CLOSE_ALL|SEQ:{seq}"
            
            # 发送消息
            logger.info(f"发送命令到 {account_id}: {msg}")
            
            if client.send_message(msg):
                return {
                    'success': True,
                    'details': {
                        'action': action,
                        'symbol': symbol if action in ['BUY', 'SELL'] else None,
                        'ticket': data.get('ticket') if action == 'CLOSE' else None,
                        'volume': volume if action in ['BUY', 'SELL'] else None,
                        'account': account_id,
                        'sequence': seq
                    }
                }
            else:
                return {'success': False, 'error': 'Failed to send message to MT5'}

        except Exception as e:
            logger.error(f"消息处理错误: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def validate_message(self, data):
        if not isinstance(data, dict):
            return False, "Message must be JSON object"

        required_fields = ['action']
        for field in required_fields:
            if field not in data:
                return False, f"Missing required field: {field}"

        action = data.get('action', '').upper()
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'CLOSE_ALL']

        if action not in valid_actions:
            return False, f"Invalid action. Must be one of: {valid_actions}"

        return True, None
