"""
消息处理模块
解析 TradingView 消息并转换为 MT5 命令
"""

import logging
import json

logger = logging.getLogger(__name__)


class MessageHandler:
    """TradingView 消息处理器"""

    def __init__(self, config, socketio=None):
        self.config = config
        self.trading_config = config.get('trading', {})
        self.socketio = socketio

    def process_message(self, data, mt5_clients):
        try:
            action = data.get('action', '').upper()

            if not action:
                return {'success': False, 'error': 'Missing action field'}

            if action not in ['BUY', 'SELL', 'CLOSE', 'CLOSE_ALL']:
                return {'success': False, 'error': f'Invalid action: {action}'}

            account_id = data.get('account_id', 'MT5_001')

            if account_id not in mt5_clients:
                available = list(mt5_clients.keys())
                if available:
                    account_id = available[0]
                    logger.warning(f"指定的账户 {data.get('account_id')} 不存在，使用默认: {account_id}")
                else:
                    return {'success': False, 'error': 'No MT5 connection available'}

            client = mt5_clients[account_id]

            if not client.is_connected():
                return {'success': False, 'error': f'MT5 {account_id} not connected'}

            symbol = data.get('symbol', self.trading_config.get('default_symbol', 'EURUSD'))
            volume = float(data.get('volume', self.trading_config.get('default_volume', 0.1)))
            sl_points = int(data.get('sl_points', self.trading_config.get('default_sl_points', 0)))
            tp_points = int(data.get('tp_points', self.trading_config.get('default_tp_points', 0)))
            comment = data.get('comment', 'TV_Signal')

            if action in ['BUY', 'SELL']:
                success = client.send_order(
                    symbol=symbol,
                    order_type=action,
                    volume=volume,
                    sl_points=sl_points,
                    tp_points=tp_points,
                    comment=comment
                )

                if success:
                    return {
                        'success': True,
                        'details': {
                            'action': action,
                            'symbol': symbol,
                            'volume': volume,
                            'account': account_id
                        }
                    }
                else:
                    return {'success': False, 'error': 'Failed to send order to MT5'}

            elif action == 'CLOSE':
                ticket = data.get('ticket')
                if not ticket:
                    return {'success': False, 'error': 'Missing ticket for close action'}

                success = client.close_order(ticket)
                if success:
                    return {
                        'success': True,
                        'details': {
                            'action': 'CLOSE',
                            'ticket': ticket,
                            'account': account_id
                        }
                    }
                else:
                    return {'success': False, 'error': 'Failed to send close order to MT5'}

            elif action == 'CLOSE_ALL':
                success = client.close_all_orders()
                if success:
                    return {
                        'success': True,
                        'details': {
                            'action': 'CLOSE_ALL',
                            'account': account_id
                        }
                    }
                else:
                    return {'success': False, 'error': 'Failed to send close all orders to MT5'}

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
