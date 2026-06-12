# TradingView-MT5 Bridge

TradingView 警报信号 → Python Flask 服务器 → MT5 EA 自动交易

## 项目架构

```
[TradingView Alerts]
       │
       ▼ (HTTP POST /webhook)
[Python Flask Server]
       │
       ▼ (TCP Socket + WebSocket)
[MT5 EA (Expert Advisor)]
       │
       ▼
[MT5 Terminal] → 执行交易
```

## 核心功能

### 1. 心跳检测
- 服务器每 5 秒检测一次连接状态
- MT5 EA 每 30 秒发送心跳包
- 服务器主动发送心跳，EA 响应 PONG
- 双向心跳确保连接存活

### 2. WebSocket 实时推送
- 使用 `flask-socketio` 实现 WebSocket
- 状态变更实时推送到所有连接的浏览器
- 交易执行结果实时通知

### 3. 可视化仪表盘
- 实时显示所有 MT5 连接状态
- 显示连接数、在线账户、总持仓
- 事件日志实时更新
- 深色主题，美观的 UI

### 4. MT5 账户信息同步
- EA 连接时自动发送 `REGISTER` 消息
- 发送 `ACCOUNT_INFO` 包含：
  - 账户ID、账户名、服务器
  - 余额、净值
  - 持仓数量
- 每次交易后更新持仓信息

## 界面预览

```
┌─────────────────────────────────────────────────────┐
│ 📡 TradingView-MT5 Bridge    ● 已连接              │
├─────────────────────────────────────────────────────┤
│ 连接数: 2/2    在线: 2    总持仓: 3    12:00:00   │
├─────────────────────────────────────────────────────┤
│ 🔗 Webhook URL                                      │
│ http://localhost:5000/webhook    [复制]             │
├─────────────────────────────────────────────────────┤
│ 📊 MT5 连接状态                                    │
│ ┌─────────────────┐  ┌─────────────────┐         │
│ │ MT5_001    ●在线 │  │ MT5_002    ●在线 │         │
│ │ 账户ID: 123456   │  │ 账户ID: 789012   │         │
│ │ 余额: $10,000    │  │ 余额: $5,000     │         │
│ │ 净值: $10,250    │  │ 净值: $4,950     │         │
│ └─────────────────┘  └─────────────────┘         │
├─────────────────────────────────────────────────────┤
│ 📋 事件日志                          [清除]        │
│ 12:00:01 MT5_001 交易执行 BUY EURUSD 0.1          │
│ 12:00:05 MT5_002 交易执行 SELL GBPUSD 0.2         │
└─────────────────────────────────────────────────────┘
```

## 快速开始

### 1. 安装依赖

```bash
cd python
pip install -r requirements.txt
```

### 2. 启动服务器

```bash
python server.py
```

### 3. 打开仪表盘

浏览器访问: http://localhost:5000/

### 4. MT5 EA 配置

1. 复制 `mt5/EaBridge.mq5` 和 `mt5/EaBridge.mqh` 到 `MT5/MQL5/Experts/`
2. 在 MT5 中编译并运行 EA
3. EA 自动连接服务器，仪表盘实时显示连接状态

### 5. TradingView Alert 配置

在 TradingView 中创建 Alert，设置 Webhook URL：
```
http://YOUR_SERVER_IP:5000/webhook
```

JSON 警报内容示例：
```json
{"action": "BUY", "symbol": "EURUSD", "volume": 0.1}
```

## 仪表盘功能

| 功能 | 说明 |
|------|------|
| 连接状态 | 实时显示所有 MT5 连接状态 |
| 账户信息 | 显示账户ID、余额、净值 |
| 持仓数量 | 实时统计各账户持仓 |
| 事件日志 | 记录所有交易和系统事件 |
| Webhook | 显示可复制的 Webhook URL |

## 安装与依赖

### Python 依赖

```
Flask>=2.0.0
PyYAML>=6.0
flask-socketio>=5.0.0
python-socketio>=5.0.0
eventlet>=0.33.0
```

### MT5 要求

- MetaTrader 5 平台
- MQL5 编译器

## 目录结构

```
tradingview_mt5_connector/
├── config.yaml              # 配置文件
├── python/
│   ├── requirements.txt     # Python 依赖
│   ├── server.py            # Flask 服务器主程序
│   ├── mt5_socket_client.py # MT5 Socket 客户端
│   ├── message_handler.py   # TradingView 消息处理
│   ├── utils.py             # 工具函数
│   └── templates/
│       └── dashboard.html   # 可视化仪表盘
└── mt5/
    ├── EaBridge.mq5        # MT5 EA 主程序
    ├── EaBridge.mqh        # MT5 EA 头文件
    └── EaBridge.json       # MT5 EA 参数配置
```

## 配置说明

### Python Server (config.yaml)

```yaml
flask:
  host: "0.0.0.0"
  port: 5000
  debug: false

mt5_connections:
  - id: "MT5_001"
    ip: "127.0.0.1"
    port: 9000
    enabled: true
```

### MT5 EA 参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| InpSocketServerIP | 服务器IP | 127.0.0.1 |
| InpSocketServerPort | 服务器端口 | 9000 |
| InpSymbol | 交易品种 | EURUSD |
| InpMagicNumber | 魔术号码 | 20250612 |
| InpHeartbeatInterval | 心跳间隔(秒) | 30 |

## TradingView Alert 配置

### Webhook URL
```
http://YOUR_SERVER_IP:5000/webhook
```

### JSON 消息格式

```json
{
  "action": "BUY",
  "symbol": "EURUSD",
  "volume": 0.1,
  "sl_points": 100,
  "tp_points": 200,
  "account_id": "MT5_001"
}
```

### 支持的交易指令

| action | 说明 |
|--------|------|
| `BUY` | 开多仓 |
| `SELL` | 开空仓 |
| `CLOSE` | 平指定订单 |
| `CLOSE_ALL` | 平所有订单 |

## 协议说明

### Socket 通信协议

Python ↔ MT5 使用文本协议，以 `\n` 分隔：

```
REGISTER|EaBridge|version|magic_number
ACCOUNT_INFO|login|name|server|balance|equity|pos_count
HEARTBEAT|timestamp
OPEN|SYMBOL|TYPE|VOLUME|SL|TP|COMMENT|SEQ:n
CLOSE|TICKET|SEQ:n
CLOSE_ALL|SEQ:n
ACK|SEQ|data
ERROR|SEQ|code|message
```

### WebSocket 事件

| 事件 | 说明 |
|------|------|
| `status_update` | 连接状态更新 |
| `trade_executed` | 交易执行成功 |
| `trade_error` | 交易执行失败 |
| `mt5_event` | MT5 事件通知 |

## 安全建议

1. **内网部署**: Flask 服务器应部署在安全的内网环境中
2. **防火墙**: 仅允许 TradingView IP 访问 Webhook 端口
3. **Token 验证**: 可在 `config.yaml` 中配置 `api_token`
4. **IP 白名单**: 可在 `config.yaml` 中配置 `allowed_ips`

## 故障排除

### MT5 EA 无法连接服务器
1. 检查服务器是否运行
2. 检查防火墙设置
3. 确认 IP 和端口配置正确

### TradingView 警报未执行
1. 检查 Webhook URL 是否正确
2. 检查 JSON 格式是否正确
3. 查看服务器日志中的错误信息

### 心跳检测失败
1. 网络连接是否稳定
2. 尝试调整 `InpHeartbeatInterval` 参数
