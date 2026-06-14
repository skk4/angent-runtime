"""
react_agent 独立调试脚本：脱离 api.py 和 Temporal，用内存 checkpointer 直接测 ReAct 循环。
在项目根目录运行：python test_react.py

目的：把 ReAct 逻辑（LLM 选工具 → 执行 → 再推理）单独验对，
     排除 PostgreSQL 和 HTTP 层的干扰。验对后再接进 api.py。
"""
from langgraph.checkpoint.memory import MemorySaver
from src.react_agent import build_react_graph

# 内存 checkpointer：不连 PG，测完即丢，专门用来验 ReAct 逻辑
memory = MemorySaver()
graph = build_react_graph(memory)

# 用一个"需要组合多个工具"的问题，看 LLM 会不会动态选工具
QUESTION = "泡泡玛特现在临近什么大促？最近股价表现怎么样？"

config = {"configurable": {"thread_id": "test-react-1"}, "recursion_limit": 15}
inputs = {
    "messages": [{"role": "user", "content": QUESTION}],
    "thread_id": "test-react-1",
}

print(f"问题：{QUESTION}\n")
print("=" * 60)

result = graph.invoke(inputs, config=config)

# 打印完整 ReAct 过程：Human -> AI(tool_calls) -> Tool -> AI(最终答案)
for m in result["messages"]:
    m.pretty_print()

print("=" * 60)
print(f"\n最终答案：\n{result['messages'][-1].content}")
