"""
工具函数模块
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def get_current_time_str():
    """获取当前时间的字符串格式"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def get_current_time_ms():
    """获取当前时间的毫秒时间戳"""
    return int(datetime.now().timestamp() * 1000)


def normalize_symbol(symbol):
    """
    规范化交易品种名称

    Args:
        symbol: 原始品种名

    Returns:
        str: 规范化后的品种名
    """
    return symbol.upper().strip()


def validate_volume(volume):
    """
    验证交易量

    Args:
        volume: 交易量

    Returns:
        float: 验证后的交易量
    """
    try:
        vol = float(volume)
        if vol <= 0:
            return None
        if vol > 100:
            vol = 100
        return round(vol, 2)
    except (ValueError, TypeError):
        return None


def get_order_type_code(order_type):
    """
    获取订单类型代码

    Args:
        order_type: 订单类型字符串 (BUY/SELL)

    Returns:
        int: MT5 订单类型代码
    """
    order_types = {
        'BUY': 0,
        'SELL': 1,
    }
    return order_types.get(order_type.upper(), -1)


def format_message(*args):
    """
    格式化消息，用 | 分隔

    Args:
        *args: 消息部件

    Returns:
        str: 格式化后的消息
    """
    return '|'.join(str(arg) for arg in args)


def parse_message(message):
    """
    解析消息

    Args:
        message: 原始消息字符串

    Returns:
        list: 消息部件列表
    """
    return [part.strip() for part in message.split('|')]
