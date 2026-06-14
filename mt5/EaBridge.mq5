//+------------------------------------------------------------------+
//|                                                     EaBridge.mq5 |
//|                        TradingView-MT5 Bridge EA |
//|                                                                  |
//+------------------------------------------------------------------+
#property copyright "TradingView-MT5 Bridge"
#property link      ""
#property version   "1.00"
// #property strict  // 暂时禁用严格模式以确保兼容性

//+------------------------------------------------------------------+
//| 常量定义                                                          |
//+------------------------------------------------------------------+
#define BUFFER_SIZE      4096
#define HEARTBEAT_INTERVAL 30
#define RECEIVE_TIMEOUT  30

//+------------------------------------------------------------------+
//| CEaBridge 类 - MT5 Socket 桥接类                                  |
//+------------------------------------------------------------------+
class CEaBridge
{
private:
   string      m_server_ip;
   int         m_server_port;
   string      m_symbol;
   ulong       m_magic_number;
   int         m_max_positions;
   bool        m_auto_reconnect;
   int         m_reconnect_interval;
   int         m_heartbeat_interval;
   
   int         m_socket_handle;
   bool        m_is_connected;
   bool        m_receive_running;
   
   int         m_sequence;
   
   datetime    m_last_activity;
   datetime    m_last_heartbeat_sent;
   int         m_error_count;
   
   string      m_ea_version;

private:
   int         GetLastErrorSocket();
   bool        SetSocketTimeout(int timeout_ms);
   string      ReceiveLine();
   bool        SendLine(string message);
   int         GetSequenceFromMessage(string message);
   void        SendAccountInfo();
   
   bool        ProcessOpenCommand(string& parts[]);
   bool        ProcessCloseCommand(string& parts[]);
   bool        ProcessCloseAllCommand(string& parts[]);
   bool        ProcessHeartbeat(string& parts[]);
   
   ulong       OpenPosition(string symbol, ENUM_ORDER_TYPE type, double volume, 
                            int sl_points, int tp_points, string comment);
   bool        ClosePosition(ulong ticket);
   bool        CloseAllPositions();
   int         CountPositions();
   bool        SendResponse(string msg_type, string seq, string data);
   bool        SendError(string seq, int error_code, string error_msg);

public:
   CEaBridge(string server_ip, int server_port, string symbol, ulong magic, 
             int max_pos, bool auto_reconnect, int reconnect_interval, int heartbeat_interval = 30);
   ~CEaBridge();
   
   bool        Connect();
   void        Disconnect();
   bool        IsConnected() { return m_is_connected; }
   void        KeepAlive();
   
   bool        SendOpenCommand(string symbol, ENUM_ORDER_TYPE type, double volume, 
                               int sl_points, int tp_points, string comment);
   bool        SendCloseCommand(ulong ticket);
   bool        SendCloseAllCommand();
   
   void        ReceiveThread();
   
   // 公开方法用于 OnTimer 异步接收
   string      ReceiveLineQuick(int max_ms = 100);
   bool        ProcessCommand(string command);
   
   datetime    GetLastActivity() { return m_last_activity; }
   int         GetErrorCount() { return m_error_count; }
};

//+------------------------------------------------------------------+
//| 构造函数                                                          |
//+------------------------------------------------------------------+
CEaBridge::CEaBridge(string server_ip, int server_port, string symbol, ulong magic,
                     int max_pos, bool auto_reconnect, int reconnect_interval, int heartbeat_interval)
{
   m_server_ip = server_ip;
   m_server_port = server_port;
   m_symbol = symbol;
   m_magic_number = magic;
   m_max_positions = max_pos;
   m_auto_reconnect = auto_reconnect;
   m_reconnect_interval = reconnect_interval;
   m_heartbeat_interval = heartbeat_interval;
   
   m_socket_handle = INVALID_HANDLE;
   m_is_connected = false;
   m_receive_running = false;
   
   m_sequence = 0;
   m_ea_version = "1.0.0";
   
   m_last_activity = TimeCurrent();
   m_last_heartbeat_sent = TimeCurrent();
   m_error_count = 0;
}

//+------------------------------------------------------------------+
//| 析构函数                                                          |
//+------------------------------------------------------------------+
CEaBridge::~CEaBridge()
{
   Disconnect();
}

//+------------------------------------------------------------------+
//| 连接服务器                                                        |
//+------------------------------------------------------------------+
bool CEaBridge::Connect()
{
   if(m_is_connected)
      return true;
   
   m_socket_handle = SocketCreate();
   if(m_socket_handle == INVALID_HANDLE)
   {
      Print("错误: 无法创建Socket");
      m_error_count++;
      return false;
   }
   
   SetSocketTimeout(10000);
   
   Print("正在连接 ", m_server_ip, ":", m_server_port, "...");
   
   if(!SocketConnect(m_socket_handle, m_server_ip, (ushort)m_server_port, 10000))
   {
      Print("错误: 无法连接到服务器");
      SocketClose(m_socket_handle);
      m_socket_handle = INVALID_HANDLE;
      m_error_count++;
      return false;
   }
   
   m_is_connected = true;
   m_last_activity = TimeCurrent();
   Print("成功连接到服务器");
   
   // 发送注册消息
   string register_msg = StringFormat("REGISTER|EaBridge|%s|%I64u\n", m_ea_version, m_magic_number);
   SendLine(register_msg);
   

   Print("a");
   // 发送账户信息
   // SendAccountInfo();

   Print("b");

   
   // 标记接收线程启动（由 OnTimer 异步调用）
   m_receive_running = true;
   Print("连接已建立，等待 OnTimer 启动接收循环");
   
   return true;
}

//+------------------------------------------------------------------+
//| 发送账户信息                                                       |
//+------------------------------------------------------------------+
void CEaBridge::SendAccountInfo()
{
   if(!m_is_connected)
      return;
   
   long login = AccountInfoInteger(ACCOUNT_LOGIN);
   string name = AccountInfoString(ACCOUNT_NAME);
   string server = AccountInfoString(ACCOUNT_SERVER);
   double balance = AccountInfoDouble(ACCOUNT_BALANCE);
   double equity = AccountInfoDouble(ACCOUNT_EQUITY);
   
   // 统计持仓数量
   int pos_count = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(PositionGetSymbol(i) == m_symbol)
      {
         if(PositionGetInteger(POSITION_MAGIC) == m_magic_number)
            pos_count++;
      }
   }
   
   string info_msg = StringFormat("ACCOUNT_INFO|%I64d|%s|%s|%.2f|%.2f|%d\n",
      login, name, server, balance, equity, pos_count);
   
   SendLine(info_msg);
   Print("账户信息已发送: ", login, " ", name);
}

//+------------------------------------------------------------------+
//| 断开连接                                                          |
//+------------------------------------------------------------------+
void CEaBridge::Disconnect()
{
   m_receive_running = false;
   
   if(m_socket_handle != INVALID_HANDLE)
   {
      SocketClose(m_socket_handle);
      m_socket_handle = INVALID_HANDLE;
   }
   
   m_is_connected = false;
   Print("已断开与服务器的连接");
}

//+------------------------------------------------------------------+
//| 设置Socket超时                                                     |
//+------------------------------------------------------------------+
bool CEaBridge::SetSocketTimeout(int timeout_ms)
{
   if(m_socket_handle == INVALID_HANDLE)
      return false;
   return SocketTimeouts(m_socket_handle, timeout_ms, timeout_ms);
}

//+------------------------------------------------------------------+
//| 接收一行数据                                                       |
//+------------------------------------------------------------------+
string CEaBridge::ReceiveLine()
{
   if(m_socket_handle == INVALID_HANDLE || !m_is_connected)
      return "";

   uchar buffer[];
   string result = "";
   uint timeout = GetTickCount() + RECEIVE_TIMEOUT * 1000;
   
   while(GetTickCount() < timeout && m_receive_running)
   {
      uint bytes = SocketRead(m_socket_handle, buffer, BUFFER_SIZE, 100);
      
      if(bytes > 0)
      {
         string chunk = CharArrayToString(buffer, 0, (int)bytes, CP_ACP);
         result += chunk;
         
         int newline = StringFind(result, "\n");
         if(newline >= 0)
            return StringSubstr(result, 0, newline);
      }
      else if(bytes == 0)
      {
         Sleep(10);
      }
      else
      {
         int err = GetLastErrorSocket();
         if(err != 0 && err != 5270)
         {
            Print("Socket读取错误: ", err);
            m_error_count++;
            m_is_connected = false;
            break;
         }
         Sleep(10);
      }
   }
   
   if(result != "")
      m_last_activity = TimeCurrent();
   
   return result;
}

//+------------------------------------------------------------------+
//| 快速接收一行（非阻塞版本，用于定时器）                              |
//+------------------------------------------------------------------+
string CEaBridge::ReceiveLineQuick(int max_ms = 100)
{
   if(m_socket_handle == INVALID_HANDLE || !m_is_connected)
      return "";
      
   uchar buffer[];
   string result = "";
   uint start = GetTickCount();


   
   while((int)(GetTickCount() - start) < max_ms)
   {
      uint bytes = SocketRead(m_socket_handle, buffer, BUFFER_SIZE, 10);
      
      if(bytes > 0)
      {
         string chunk = CharArrayToString(buffer, 0, (int)bytes, CP_ACP);
         result += chunk;
         
         int newline = StringFind(result, "\n");
         if(newline >= 0)
         {
            m_last_activity = TimeCurrent();
            return StringSubstr(result, 0, newline);
         }
      }
      else if(bytes == 0)
      {
         Sleep(5);
      }
      else
      {
         int err = GetLastError();
         if(err != 0 && err != 5270)
         {
            Print("Socket读取错误: ", err);
            m_is_connected = false;
            break;
         }
         Sleep(5);
      }
   }

   if(result != "")
      m_last_activity = TimeCurrent();
   
   return result;
}

//+------------------------------------------------------------------+
//| 发送一行数据                                                       |
//+------------------------------------------------------------------+
bool CEaBridge::SendLine(string message)
{
   if(m_socket_handle == INVALID_HANDLE || !m_is_connected)
      return false;
   
   if(StringFind(message, "\n") < 0)
      message += "\n";
   
   uchar buffer[];
   int len = StringToCharArray(message, buffer, 0, -1, CP_ACP);
   
   uint sent = SocketSend(m_socket_handle, buffer, (uint)len);
   
   if(sent == 0)
   {
      Print("Socket发送失败");
      m_error_count++;
      m_is_connected = false;
      return false;
   }
   
   m_last_activity = TimeCurrent();
   return true;
}

//+------------------------------------------------------------------+
//| 从消息中获取序列号                                                  |
//+------------------------------------------------------------------+
int CEaBridge::GetSequenceFromMessage(string message)
{
   int seq_pos = StringFind(message, "SEQ:");
   if(seq_pos >= 0)
      return (int)StringToInteger(StringSubstr(message, seq_pos + 4));
   return 0;
}

//+------------------------------------------------------------------+
//| 接收线程主循环                                                     |
//+------------------------------------------------------------------+
void CEaBridge::ReceiveThread()
{
   Print("Socket接收线程启动");
   
   while(m_receive_running)
   {
      if(!m_is_connected)
         break;
         
      string line = ReceiveLine();
      
      if(line == "" || line == NULL)
      {
         Sleep(10);
         continue;
      }
      
      Print("收到命令: ", line);
      ProcessCommand(line);
      
      datetime now = TimeCurrent();
      if(now - m_last_heartbeat_sent >= m_heartbeat_interval)
      {
         SendLine("HEARTBEAT|" + IntegerToString(now));
         m_last_heartbeat_sent = now;
      }
   }
   
   Print("Socket接收线程结束");
   
   if(m_auto_reconnect && m_receive_running)
   {
      Print("将在 ", m_reconnect_interval, " 秒后尝试重连...");
      Sleep(m_reconnect_interval * 1000);
      Connect();
   }
}

//+------------------------------------------------------------------+
//| 处理命令                                                          |
//+------------------------------------------------------------------+
bool CEaBridge::ProcessCommand(string command)
{
   string parts[];
   StringSplit(command, '|', parts);
   
   if(ArraySize(parts) < 1)
      return false;
   
   string cmd = parts[0];
   
   if(cmd == "OPEN")
      return ProcessOpenCommand(parts);
   else if(cmd == "CLOSE")
      return ProcessCloseCommand(parts);
   else if(cmd == "CLOSE_ALL")
      return ProcessCloseAllCommand(parts);
   else if(cmd == "HEARTBEAT")
      return ProcessHeartbeat(parts);
   else if(cmd == "PONG")
   {
      Print("收到PONG响应");
      return true;
   }
   else if(cmd == "REGISTER_ACK")
   {
      Print("注册确认成功");
      return true;
   }
   
   return false;
}

//+------------------------------------------------------------------+
//| 处理心跳                                                          |
//+------------------------------------------------------------------+
bool CEaBridge::ProcessHeartbeat(string& parts[])
{
   m_last_activity = TimeCurrent();
   SendLine("PONG|" + (ArraySize(parts) > 1 ? parts[1] : IntegerToString(TimeCurrent())));
   return true;
}

//+------------------------------------------------------------------+
//| 处理开仓命令                                                       |
//+------------------------------------------------------------------+
bool CEaBridge::ProcessOpenCommand(string& parts[])
{
   if(ArraySize(parts) < 7)
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 1, "Invalid OPEN command format");
      return false;
   }
   
   string symbol = parts[1];
   string type_str = parts[2];
   double volume = StringToDouble(parts[3]);
   int sl_points = (int)StringToInteger(parts[4]);
   int tp_points = (int)StringToInteger(parts[5]);
   string comment = parts[6];
   
   ENUM_ORDER_TYPE order_type;
   if(type_str == "BUY")
      order_type = ORDER_TYPE_BUY;
   else if(type_str == "SELL")
      order_type = ORDER_TYPE_SELL;
   else
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 2, "Invalid order type");
      return false;
   }
   
   if(CountPositions() >= m_max_positions)
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 3, "Max positions reached");
      return false;
   }
   
   ulong ticket = OpenPosition(symbol, order_type, volume, sl_points, tp_points, comment);
   
   if(ticket > 0)
   {
      string response = StringFormat("POSITION_OPENED|%I64u|%s|%s|%.2f|%.5f|%d|%d",
         ticket, symbol, type_str, volume, 
         SymbolInfoDouble(symbol, SYMBOL_BID), sl_points, tp_points);
      SendResponse("ACK", IntegerToString(GetSequenceFromMessage(parts[0])), response);
      SendAccountInfo();
      return true;
   }
   else
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 4, "Failed to open position");
      return false;
   }
}

//+------------------------------------------------------------------+
//| 处理平仓命令                                                       |
//+------------------------------------------------------------------+
bool CEaBridge::ProcessCloseCommand(string& parts[])
{
   if(ArraySize(parts) < 2)
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 1, "Invalid CLOSE command format");
      return false;
   }
   
   ulong ticket = (ulong)StringToInteger(parts[1]);
   
   if(ClosePosition(ticket))
   {
      string response = StringFormat("POSITION_CLOSED|%I64u", ticket);
      SendResponse("ACK", IntegerToString(GetSequenceFromMessage(parts[0])), response);
      SendAccountInfo();
      return true;
   }
   else
   {
      SendError(IntegerToString(GetSequenceFromMessage(parts[0])), 5, "Failed to close position");
      return false;
   }
}

//+------------------------------------------------------------------+
//| 处理平仓全部命令                                                   |
//+------------------------------------------------------------------+
bool CEaBridge::ProcessCloseAllCommand(string& parts[])
{
   int closed = CloseAllPositions();
   string response = StringFormat("CLOSED_COUNT|%d", closed);
   SendResponse("ACK", IntegerToString(GetSequenceFromMessage(parts[0])), response);
   SendAccountInfo();
   return true;
}

//+------------------------------------------------------------------+
//| 开仓                                                              |
//+------------------------------------------------------------------+
ulong CEaBridge::OpenPosition(string symbol, ENUM_ORDER_TYPE type, double volume,
                               int sl_points, int tp_points, string comment)
{
   MqlTradeRequest request = {};
   MqlTradeResult result = {};
   
   request.action = TRADE_ACTION_DEAL;
   request.symbol = symbol;
   request.volume = volume;
   request.type = type;
   request.price = (type == ORDER_TYPE_BUY) ? SymbolInfoDouble(symbol, SYMBOL_ASK) : 
                                               SymbolInfoDouble(symbol, SYMBOL_BID);
   request.deviation = 10;
   request.magic = m_magic_number;
   request.comment = comment;
   
   double point = SymbolInfoDouble(symbol, SYMBOL_POINT);
   double sl_price = 0, tp_price = 0;
   
   if(sl_points > 0)
   {
      if(type == ORDER_TYPE_BUY)
         sl_price = request.price - sl_points * point;
      else
         sl_price = request.price + sl_points * point;
      request.sl = NormalizeDouble(sl_price, SymbolInfoInteger(symbol, SYMBOL_DIGITS));
   }
   
   if(tp_points > 0)
   {
      if(type == ORDER_TYPE_BUY)
         tp_price = request.price + tp_points * point;
      else
         tp_price = request.price - tp_points * point;
      request.tp = NormalizeDouble(tp_price, SymbolInfoInteger(symbol, SYMBOL_DIGITS));
   }
   
   if(!OrderSend(request, result))
   {
      Print("OrderSend 错误: ", GetLastErrorSocket());
      return 0;
   }
   
   if(result.retcode == TRADE_RETCODE_DONE)
   {
      Print("订单开仓成功: ", result.order, " ", symbol, " ", 
            (type == ORDER_TYPE_BUY ? "BUY" : "SELL"), " ", volume);
      return result.order;
   }
   else
   {
      Print("订单开仓失败: ", result.retcode, " ", result.comment);
      return 0;
   }
}

//+------------------------------------------------------------------+
//| 平仓                                                              |
//+------------------------------------------------------------------+
bool CEaBridge::ClosePosition(ulong ticket)
{
   if(!PositionSelectByTicket(ticket))
   {
      Print("找不到订单: ", ticket);
      return false;
   }
   
   MqlTradeRequest request = {};
   MqlTradeResult result = {};
   
   request.action = TRADE_ACTION_DEAL;
   request.position = ticket;
   request.symbol = PositionGetString(POSITION_SYMBOL);
   request.volume = PositionGetDouble(POSITION_VOLUME);
   request.type = (PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY) ? 
                   ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   request.price = SymbolInfoDouble(request.symbol, SYMBOL_BID);
   request.deviation = 10;
   request.magic = m_magic_number;
   request.comment = "Close by Bridge";
   
   if(!OrderSend(request, result))
   {
      Print("平仓 OrderSend 错误: ", GetLastErrorSocket());
      return false;
   }
   
   if(result.retcode == TRADE_RETCODE_DONE)
   {
      Print("订单平仓成功: ", ticket);
      return true;
   }
   else
   {
      Print("订单平仓失败: ", result.retcode, " ", result.comment);
      return false;
   }
}

//+------------------------------------------------------------------+
//| 平仓全部                                                          |
//+------------------------------------------------------------------+
bool CEaBridge::CloseAllPositions()
{
   int count = 0;
   
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(PositionGetSymbol(i) == m_symbol)
      {
         ulong ticket = PositionGetTicket(i);
         if(PositionGetInteger(POSITION_MAGIC) == m_magic_number)
         {
            if(ClosePosition(ticket))
               count++;
         }
      }
   }
   
   Print("平仓完成: ", count, " 个订单");
   return count;
}

//+------------------------------------------------------------------+
//| 计算当前持仓数量                                                   |
//+------------------------------------------------------------------+
int CEaBridge::CountPositions()
{
   int count = 0;
   
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      if(PositionGetSymbol(i) == m_symbol)
      {
         if(PositionGetInteger(POSITION_MAGIC) == m_magic_number)
            count++;
      }
   }
   
   return count;
}

//+------------------------------------------------------------------+
//| 发送响应                                                          |
//+------------------------------------------------------------------+
bool CEaBridge::SendResponse(string msg_type, string seq, string data)
{
   string message = msg_type + "|" + seq + "|" + data;
   Print("发送响应: ", message);
   return SendLine(message);
}

//+------------------------------------------------------------------+
//| 发送错误                                                          |
//+------------------------------------------------------------------+
bool CEaBridge::SendError(string seq, int error_code, string error_msg)
{
   string message = StringFormat("ERROR|%s|%d|%s", seq, error_code, error_msg);
   Print("发送错误: ", message);
   return SendLine(message);
}

//+------------------------------------------------------------------+
//| 保持连接活跃                                                       |
//+------------------------------------------------------------------+
void CEaBridge::KeepAlive()
{
   if(m_is_connected)
   {
      datetime now = TimeCurrent();
      
      // 心跳间隔到，发心跳
      if(now - m_last_heartbeat_sent >= m_heartbeat_interval)
      {
         Print("发送心跳到服务器...");
         SendLine("HEARTBEAT|" + IntegerToString(now));
         m_last_heartbeat_sent = now;
      }
      else if(now - m_last_activity >= 10)  // 超过10秒无活动，发探测
      {
         Print("发送连接探测...");
         SendLine("PING|" + IntegerToString(now));
      }
   }
}

//+------------------------------------------------------------------+
//| 发送开仓命令                                                       |
//+------------------------------------------------------------------+
bool CEaBridge::SendOpenCommand(string symbol, ENUM_ORDER_TYPE type, double volume,
                                  int sl_points, int tp_points, string comment)
{
   m_sequence++;
   string message = StringFormat("OPEN|%s|%s|%.2f|%d|%d|%s|SEQ:%d",
      symbol, (type == ORDER_TYPE_BUY ? "BUY" : "SELL"), 
      volume, sl_points, tp_points, comment, m_sequence);
   return SendLine(message);
}

//+------------------------------------------------------------------+
//| 发送平仓命令                                                       |
//+------------------------------------------------------------------+
bool CEaBridge::SendCloseCommand(ulong ticket)
{
   m_sequence++;
   string message = StringFormat("CLOSE|%I64u|SEQ:%d", ticket, m_sequence);
   return SendLine(message);
}

//+------------------------------------------------------------------+
//| 发送平仓全部命令                                                   |
//+------------------------------------------------------------------+
bool CEaBridge::SendCloseAllCommand()
{
   m_sequence++;
   string message = StringFormat("CLOSE_ALL|SEQ:%d", m_sequence);
   return SendLine(message);
}

//+------------------------------------------------------------------+
//| 获取Socket错误                                                     |
//+------------------------------------------------------------------+
int CEaBridge::GetLastErrorSocket()
{
   return GetLastError();
}

//+------------------------------------------------------------------+
//| 输入参数                                                          |
//+------------------------------------------------------------------+
input string   InpSocketServerIP = "aigpt6.com";       // Socket服务器IP地址
input int      InpSocketServerPort = 8088;            // Socket服务器端口
input string   InpSymbol = "EURUSD";                  // 交易品种
input ulong    InpMagicNumber = 20250612;             // 魔术号码
input int      InpMaxPositions = 5;                  // 最大持仓数量
input double   InpLotSize = 0.1;                     // 默认交易量
input int      InpStopLossPoints = 100;              // 默认止损点数
input int      InpTakeProfitPoints = 200;            // 默认止盈点数
input bool     InpAutoReconnect = true;              // 自动重连
input int      InpReconnectInterval = 5;             // 重连间隔(秒)
input int      InpHeartbeatInterval = 30;            // 心跳间隔(秒)

//+------------------------------------------------------------------+
//| 全局变量                                                          |
//+------------------------------------------------------------------+
CEaBridge* g_bridge = NULL;

//+------------------------------------------------------------------+
//| EA初始化函数                                                       |
//+------------------------------------------------------------------+
int OnInit()
{
   Print("========================================");
   Print("TradingView-MT5 Bridge EA 初始化中...");
   Print("服务器: ", InpSocketServerIP, ":", InpSocketServerPort);
   Print("交易品种: ", InpSymbol);
   Print("魔术号码: ", InpMagicNumber);
   Print("========================================");
   
   // 创建桥接对象
   g_bridge = new CEaBridge(
      InpSocketServerIP,
      InpSocketServerPort,
      InpSymbol,
      InpMagicNumber,
      InpMaxPositions,
      InpAutoReconnect,
      InpReconnectInterval,
      InpHeartbeatInterval
   );
   
   if(g_bridge == NULL)
   {
      Print("错误: 无法创建桥接对象");
      return INIT_FAILED;
   }
   
   // 尝试连接
   if(!g_bridge.Connect())
   {
      Print("警告: 初始连接失败，EA将在后台自动重连");
   }
   else
   {
      Print("成功连接到Python服务器");
   }
   
   // 启动定时器（每秒触发一次，用于心跳和异步接收）
   EventSetTimer(1);
   
   return INIT_SUCCEEDED;
}

//+------------------------------------------------------------------+
//| EA卸载函数                                                        |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   Print("========================================");
   Print("TradingView-MT5 Bridge EA 卸载中...");
   Print("原因: ", reason);
   Print("========================================");
   
   // 停止定时器
   EventKillTimer();
   
   if(g_bridge != NULL)
   {
      g_bridge.Disconnect();
      delete g_bridge;
      g_bridge = NULL;
   }
}

//+------------------------------------------------------------------+
//| EA主函数                                                          |
//+------------------------------------------------------------------+
void OnTick()
{
   if(g_bridge != NULL)
   {
      g_bridge.KeepAlive();
   }
}

//+------------------------------------------------------------------+
//| 定时器事件                                                        |
//+------------------------------------------------------------------+
void OnTimer()
{
   if(g_bridge != NULL)
   {
      if(!g_bridge.IsConnected())
      {
         Print("断联了");
         static datetime last_reconnect_attempt = 0;
         datetime current_time = TimeCurrent();
         
         if(current_time - last_reconnect_attempt >= InpReconnectInterval)
         {
            Print("尝试重新连接服务器...");
            g_bridge.Connect();
            last_reconnect_attempt = current_time;
         }
      }
      else
      {
         Print("还连着");

         // 异步读取命令（非阻塞，最多 50ms）
         string line = g_bridge.ReceiveLineQuick(50);
         if(line != "" && line != NULL)
         {
            Print("收到命令: ", line);
            g_bridge.ProcessCommand(line);
         }

         // 心跳保活
         g_bridge.KeepAlive();
      }
   }
}
//+------------------------------------------------------------------+
