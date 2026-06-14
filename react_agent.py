"""
ReAct Agent —— 双线兼容的"通用问答线"。

与固定 pipeline 线（graph.py）的分工：
- 固定线：写死 fetch_price → finance → sentiment → analyze，确定性，适合标准投研日报
- ReAct 线：LLM 自己决定调哪些工具、调几次，灵活，适合开放式探索问题

刻意保持与固定线对称，便于将来 Supervisor 统一调度：
- LLM：复用 src.nodes.llm（DeepSeek），与固定线同一个实例
- State：复用 InvestState（已含 messages），两条线共享同一 State
- checkpointer：由调用方注入（build_react_graph 与 build_graph 同签名）

标准 ReAct 循环：
    START → agent → (有 tool_calls?) → tools → agent → ... → (无) → END
"""

import json

from langgraph.graph import StateGraph, START, END
from langchain_core.messages import SystemMessage, ToolMessage

from src.state import InvestState
from src.tools import TOOLS
from src.nodes import llm   # 复用固定线的 DeepSeek 实例（后续可抽到 src/llm.py 统一）

# 把工具绑定给 LLM：LLM 看到每个工具的签名和 docstring，自行决定调用哪个
llm_with_tools = llm.bind_tools(TOOLS)

# name -> tool 映射，供工具执行节点按名字查找
_tools_by_name = {t.name: t for t in TOOLS}

SYSTEM_PROMPT = SystemMessage(content=(
    "你是泡泡玛特(09992.HK)的投研助手。你可以调用以下工具获取真实数据：\n"
    "- get_stock_price：股价行情（最新价、区间涨跌、高低）\n"
    "- get_finance_data：财务报表与关键指标（营收/利润/同比/ROE）\n"
    "- get_sentiment：市场热度 / 舆情（各来源热度与趋势）\n"
    "- get_product_cycle：销售周期 / 大促 / 财报窗口（当前与临近）\n\n"
    "规则：\n"
    "1. 根据用户问题，判断需要哪些数据，调用对应工具（可多次、可组合）\n"
    "2. 只基于工具返回的真实数据分析，绝不编造任何数字\n"
    "3. 如果工具返回 {'error': ...}，向用户说明数据获取失败，不要编造\n"
    "4. 数据齐全后，给出简洁、有依据的投研分析"
))


def agent_node(state: InvestState) -> dict:
    """
    ReAct 的'思考'节点：LLM 看历史消息，决定调工具还是直接给最终答案。

    每次临时把 SYSTEM_PROMPT 拼在最前（不写回 state，避免重复累积）。
    返回的 AIMessage 可能带 tool_calls（要调工具）或纯文本（最终答案）。
    """
    messages = [SYSTEM_PROMPT] + state["messages"]
    response = llm_with_tools.invoke(messages)
    return {"messages": [response]}


def tool_node(state: InvestState) -> dict:
    """
    工具执行节点（手写替代 langgraph.prebuilt.ToolNode）。

    遍历最后一条 AI 消息里的 tool_calls，逐个执行对应的 @tool，
    把返回值包成 ToolMessage（用 tool_call_id 对应回去）回填到 messages。
    工具本身已做容错（返回 error dict 不抛异常），这里只负责调度。
    """
    last = state["messages"][-1]
    tool_messages = []
    for call in last.tool_calls:
        tool = _tools_by_name.get(call["name"])
        if tool is None:
            result = {"error": f"未知工具：{call['name']}"}
        else:
            result = tool.invoke(call["args"])
        tool_messages.append(ToolMessage(
            content=json.dumps(result, ensure_ascii=False),
            tool_call_id=call["id"],
            name=call["name"],
        ))
    return {"messages": tool_messages}


def should_continue(state: InvestState) -> str:
    """条件边：最后一条 AI 消息带 tool_calls 就去执行工具，否则结束。"""
    last = state["messages"][-1]
    if getattr(last, "tool_calls", None):
        return "tools"
    return "end"


def build_react_graph(memory):
    """
    构建 ReAct 图，签名与 build_graph(memory) 一致（依赖注入 checkpointer）。

    agent 思考 → 若要调工具走 tools 执行 → 结果回 agent 继续推理，
    直到 agent 不再发起 tool_calls，给出最终答案。
    """
    builder = StateGraph(InvestState)
    builder.add_node("agent", agent_node)
    builder.add_node("tools", tool_node)

    builder.add_edge(START, "agent")
    builder.add_conditional_edges("agent", should_continue, {
        "tools": "tools",   # LLM 要调工具
        "end": END,         # LLM 给出最终答案
    })
    builder.add_edge("tools", "agent")  # 工具结果回到 agent 继续推理

    return builder.compile(checkpointer=memory)
