from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.sqlite import SqliteSaver
from src.state import InvestState
from src.nodes import fetch_price, fetch_finance, fetch_sentiment, analyze
from src.nodes import should_retry

# 这里创建了一个状态图，并定义了从START到END的路径，路径上依次调用了fetch_price、fetch_finance、fetch_sentiment和analyze函数来更新状态。
# 最后，使用SqliteSaver来保存状态图的状态，以便在需要时可以恢复或分析。


def build_graph(memory):
    builder = StateGraph(InvestState)
    builder.add_node("fetch_price", fetch_price)
    builder.add_node("fetch_finance", fetch_finance)
    builder.add_node("fetch_sentiment", fetch_sentiment)
    builder.add_node("analyze", analyze)
    builder.add_edge(START, "fetch_price")  
    builder.add_edge("fetch_price", "fetch_finance")  
    builder.add_edge("fetch_finance", "fetch_sentiment")  
    builder.add_edge("fetch_sentiment", "analyze")  
    # builder.add_edge("analyze", END) 
    # memory = SqliteSaver.from_conn_string("checkpoint.db")
    builder.add_conditional_edges("analyze", should_retry, {
        "retry": "fetch_price",  # 如果需要重试，就回到fetch_price重新开始
        "end": END  # 否则就结束
    })
    return builder.compile(checkpointer=memory)
  
