from src.graph import build_graph
# from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.postgres import PostgresSaver

# config = {"configurable": {"thread_id": "invest_006"}}
config = {"configurable": {"thread_id": "invest_pg_013"}}
inputs = {"messages": [{"role": "user", "content": "分析泡泡玛特"}],
        "thread_id": config["configurable"]["thread_id"]}

# with SqliteSaver.from_conn_string("checkpoint.db") as memory:
DB_URL = "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db"
with PostgresSaver.from_conn_string(DB_URL) as memory:
    memory.setup()  # 确保表结构已创建
    graph = build_graph(memory)
    # existing = memory.get(config)  

    # graph = build_graph(memory)
    
    # 自动判断：有 checkpoint 就恢复，没有就从头跑
    existing = memory.get(config)
    if existing is None:
        print("首次运行，传入初始数据")
        result = graph.invoke(inputs, config=config)
    else:
        print("发现断点，从上次继续")
        result = graph.invoke(None, config=config)
    
    print("=== 分析结果 ===")
    print(result["analysis"])