//+------------------------------------------------------------------+
//|                                                     EaBridge.mq5 |
//|                        TradingView-MT5 Bridge EA |
//|                                                                  |
//+------------------------------------------------------------------+
#property copyright "TradingView-MT5 Bridge"
#property link      ""
#property version   "1.00"
#property strict

#include <EaBridge.mqh>

//+------------------------------------------------------------------+
//| 输入参数                                                          |
//+------------------------------------------------------------------+
input string   InpSocketServerIP = "127.0.0.1";       // Socket服务器IP地址
input int      InpSocketServerPort = 9000;           // Socket服务器端口
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
         static datetime last_reconnect_attempt = 0;
         datetime current_time = TimeCurrent();
         
         if(current_time - last_reconnect_attempt >= InpReconnectInterval)
         {
            Print("尝试重新连接服务器...");
            g_bridge.Connect();
            last_reconnect_attempt = current_time;
         }
      }
   }
}
//+------------------------------------------------------------------+
