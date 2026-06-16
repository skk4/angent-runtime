from typing import Annotated, TypedDict
from langgraph.graph.message import add_messages

# 数据是追加的，用add_messages装饰器来标记这个字段，以便在状态更新时正确处理消息的追加。

'''
多个 agent 通过读写这些字段通信（不靠对话）：
分析师写 analysis → 研究员读 analysis 写 debate_view → 主笔读两者写 final_report。
'''

class InvestState(TypedDict):
    """State of the investment process."""
    messages: Annotated[list, add_messages]  #消息流（ReAct/固定线用，多 agent 不依赖它）
    symbol: str          # 标的，如"泡泡玛特"  —— 多 agent 新增
    stock_data: dict     # 行情数据（固定线用）
    financials: dict     # 财报数据（固定线用
    sentiment: dict     # 舆情数据（固定线用）
    analysis: str      # analysis: str        # ① 分析师产出
    debate_view: str     # ② 多空研究员产出  —— 多 agent 新增
    final_report: str    # ③ 报告主笔产出    —— 多 agent 新增
    retry_count: int    # 重试计数（固定线用）
    thread_id: str      # 线程 ID（checkpoint 用）
    bull_view: str       # 多头研究员论据    —— 新增
    bear_view: str       # 空头研究员论据    —— 新增

