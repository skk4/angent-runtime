from typing import Annotated, TypedDict
from langgraph.graph.message import add_messages

# 数据是追加的，用add_messages装饰器来标记这个字段，以便在状态更新时正确处理消息的追加。

class InvestState(TypedDict):
    """State of the investment process."""
    messages: Annotated[list, add_messages]
    stock_data: dict
    financials: dict
    sentiment: dict
    analysis: str
    retry_count: int
    thread_id: str
