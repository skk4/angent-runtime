# src/research_team.py
"""投研多 agent 协作模块：分析师 → 多空研究员 → 报告主笔。

三个 agent 通过共享 state 通信（不靠对话），用 LangGraph 串成流水线：
    analyst（取数+分析） → researcher（多空权衡） → writer（综合成报告）

复用项目现有资源，不另起炉灶：
    - 取数工具：src.tools 的四个 @tool
    - LLM 实例：src.nodes.llm（与固定线、ReAct 同一个 DeepSeek 实例）
    - 状态定义：src.state.InvestState（已扩展 symbol/debate_view/final_report）
"""

from langchain_core.messages import SystemMessage, HumanMessage
from langgraph.graph import StateGraph, START, END

from src.state import InvestState
from src.nodes import llm   # 复用项目统一的 DeepSeek 实例
from src.tools import (
    get_stock_price,      # 行情
    get_finance_data,     # 财报
    get_sentiment,        # 舆情
    get_product_cycle,    # 活动周期
)


# ========== 数据聚合：一次取齐四个维度 ==========
def gather_data(symbol: str) -> dict:
    """调用四个原子工具，把行情/财报/舆情/活动汇总成一个 dict。

    工具是 @tool 装饰的，直接调用要用 .invoke()；它们参数都有默认值，传空 dict 即可。
    """
    return {
        "行情": get_stock_price.invoke({}),           # 取行情走势
        "财报": get_finance_data.invoke({"periods": 4}),  # 取最近 4 期财报
        "舆情": get_sentiment.invoke({}),             # 取市场情绪
        "活动周期": get_product_cycle.invoke({}),      # 取促销/新品节奏
    }


# ========== 三个 agent 各自的 system prompt ==========
# 分析师：只做客观分析，不下买卖结论
ANALYST_PROMPT = """你是投研分析师。基于给定的多源数据，输出一份结构化分析报告，覆盖：
① 基本面（财报关键指标的解读）
② 技术面（行情走势特征）
③ 舆情（市场情绪倾向）
④ 活动周期（促销/新品节奏对业绩的潜在影响）
要求：每个维度给出客观的关键发现，只做事实分析，不要下买/卖结论——那是研究员的工作。"""

# 研究员：基于分析做多空权衡（核心是"辩论"，避免一边倒）
RESEARCHER_PROMPT = """你是投研团队的多空研究员。基于分析师的报告，做一次结构化的多空权衡：
【看多论据】列出支持看多的关键理由
【看空论据】列出支持看空的关键理由
【综合研判】权衡后给出倾向（看多/看空/中性）及核心理由
要求：两方论据都要扎实，避免一边倒；结论要落在分析师给的事实上，不要凭空发挥。"""

# 主笔：综合分析+研判，产出最终报告（出报告，不出实盘指令）
WRITER_PROMPT = """你是投研报告主笔。综合"分析师报告"和"多空研判"，撰写一份最终投研报告：
一、核心结论（一句话给出投资倾向）
二、关键依据（支撑结论的 3-5 条核心论据）
三、风险提示（需警惕的下行风险）
要求：结论要有依据可循、风险要诚实披露；这是研究报告，不提供具体买卖/实盘指令。"""


# ========== 三个 agent 节点 ==========
def analyst_node(state: InvestState) -> dict:
    """① 分析师：取数 → LLM 分析 → 写入 analysis 字段。"""
    symbol = state["symbol"]
    data = gather_data(symbol)              # 取齐四维数据

    # 调 LLM 生成分析报告（system 设定角色，human 喂数据）
    resp = llm.invoke([
        SystemMessage(content=ANALYST_PROMPT),
        HumanMessage(content=f"标的：{symbol}\n\n数据：\n{data}"),
    ])
    # 只更新 analysis 字段，写回共享 state（研究员会从这里读）
    return {"analysis": resp.content}


def researcher_node(state: InvestState) -> dict:
    """② 多空研究员：读 analysis → 多空权衡 → 写入 debate_view 字段。"""
    resp = llm.invoke([
        SystemMessage(content=RESEARCHER_PROMPT),
        HumanMessage(content=f"分析师报告：\n{state['analysis']}"),  # 从 state 读分析
    ])
    return {"debate_view": resp.content}


def writer_node(state: InvestState) -> dict:
    """③ 报告主笔：综合 analysis + debate_view → 写入 final_report 字段。"""
    resp = llm.invoke([
        SystemMessage(content=WRITER_PROMPT),
        HumanMessage(content=(                          # 同时喂分析和研判
            f"分析师报告：\n{state['analysis']}\n\n"
            f"多空研判：\n{state['debate_view']}"
        )),
    ])
    return {"final_report": resp.content}


# ========== 图构建：三个节点串成流水线 ==========
def build_research_team():
    """把三个 agent 用 LangGraph 串成 analyst → researcher → writer 流水线。

    三者通过共享 state 通信：前一个写字段，后一个读字段（不靠对话）。
    """
    g = StateGraph(InvestState)            # 用扩展后的 InvestState 建图

    # 注册三个 agent 节点
    g.add_node("analyst", analyst_node)
    g.add_node("researcher", researcher_node)
    g.add_node("writer", writer_node)

    # 连边：START → 分析师 → 研究员 → 主笔 → END（线性流水线）
    g.add_edge(START, "analyst")
    g.add_edge("analyst", "researcher")
    g.add_edge("researcher", "writer")
    g.add_edge("writer", END)

    # 编译成可执行图（下一步接 checkpointer 做崩溃恢复：compile(checkpointer=...)）
    return g.compile()


# ========== 本地测试入口 ==========
if __name__ == "__main__":
    team = build_research_team()

    # 初始 state：只需给 symbol，其余字段由各节点逐步填充（先给空值占位）
    result = team.invoke({
        "symbol": "泡泡玛特",
        "messages": [],
        "stock_data": {},
        "financials": {},
        "sentiment": {},
        "analysis": "",
        "debate_view": "",
        "final_report": "",
        "retry_count": 0,
        "thread_id": "research-test-001",
    })

    # 打印三个 agent 各自的产出，肉眼确认协作链路通了
    print("===== ① 分析师报告 =====")
    print(result["analysis"])
    print("\n===== ② 多空研判 =====")
    print(result["debate_view"])
    print("\n===== ③ 最终投研报告 =====")
    print(result["final_report"])