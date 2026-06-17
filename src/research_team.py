# src/research_team.py
"""投研多 agent 协作模块：分析师 → 多空【多轮辩论】 → 研究经理裁决 → 报告主笔。
 
辩论流程（多轮）：
    analyst → [ bull ⇄ bear ] × N 轮 → judge → writer
    多头和空头看着累积的辩论记录来回交锋，达到 N 轮后由研究经理裁决。
 
────────────────────────────────────────────────────────────────────
【相对「一轮辩论版」的修改点总览】  在代码里搜 "★改动" 可逐个定位
────────────────────────────────────────────────────────────────────
  ★改动1  新增常量 MAX_DEBATE_ROUNDS —— 控制辩论轮数（一轮版没有，写死一轮）
  ★改动2  state 通信载体变了 —— 一轮版用 bull_view / bear_view 两个独立字段；
          多轮版改用 debate_history（累积全程） + debate_round（轮次计数）
  ★改动3  bull_node 改造 —— 改读 debate_history（看完整辩论）；prompt 支持
          "首轮陈述 or 回应空头"；输出从"覆盖 bull_view"改为"追加进 debate_history"
  ★改动4  bear_node 改造 —— 改读/追加 debate_history；新增 debate_round += 1
  ★改动5  新增 should_continue_debate —— 条件边判断函数（一轮版没有，无循环）
  ★改动6  judge_node 改造 —— 改读完整 debate_history，而非 bull_view + bear_view
  ★改动7  build_research_team 改造 —— bear 之后从"固定边直达 judge"改为"条件边"，
          形成 bull⇄bear 循环（一轮版是 analyst→bull→bear→judge 的纯线性）
  ★改动8  __main__ 改造 —— 初始 state 加 debate_history/debate_round；
          打印改为输出 debate_history（多轮全记录），而非分别打印 bull/bear
────────────────────────────────────────────────────────────────────
 
复用项目现有的 LLM 实例与取数工具，通过共享 state 通信。
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
# 多空各发言几轮（1 轮 = 多头发言 + 空头反驳各一次）。2 即来回交锋两个回合，可调。
MAX_DEBATE_ROUNDS = 3

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


# ==========  agent 各自的 system prompt ==========
# 分析师：只做客观分析，不下买卖结论
# 分析师：只做客观分析，不下买卖结论（与一轮版相同，未改动）
ANALYST_PROMPT = """你是投研分析师。基于给定的多源数据，输出一份结构化分析报告，覆盖：
① 基本面（财报关键指标的解读）
② 技术面（行情走势特征）
③ 舆情（市场情绪倾向）
④ 活动周期（促销/新品节奏对业绩的潜在影响）
要求：每个维度给出客观的关键发现，只做事实分析，不要下买/卖结论——那是研究员的工作。"""

# ========== 多空辩论的三个 system prompt ==========
# 多头研究员：火力全开找看多理由，不自我否定
# ★改动（prompt 部分）：多头 prompt 增加"分两种情况"——一轮版只需首轮陈述，
#                        多轮版还要能在后续轮次回应空头的反驳
BULL_PROMPT = """你是多头研究员，立场坚定看多。
- 如果辩论刚开始（还没有空头发言）：基于分析师报告，列出 3-5 条最有力的看多论据。
- 如果空头已经发言：针对性回应空头的反驳，指出其反驳的不成立之处，并巩固、补强你的多头立场。
要求：火力全开，论据落在分析师给的事实上，不要自我否定。"""

# 空头 prompt：逐条反驳多头最新论据 + 补充独立空头论据（与一轮版基本一致）
BEAR_PROMPT = """你是空头研究员，立场坚定看空。基于分析师报告和当前辩论记录：
① 逐条反驳多头最新一轮的论据（指出其漏洞或被高估之处）
② 补充独立的看空论据
要求：火力全开找多头的破绽，论据要具体，不要和稀泥。"""

# ★改动（prompt 部分）：裁判 prompt 强调读"完整多轮辩论记录"，并关注攻防过程
#                        （一轮版只看一次性的多空两段，没有"多轮攻防"概念）
JUDGE_PROMPT = """你是投研团队的研究经理，保持中立。你将看到多头和空头的【完整多轮辩论记录】，请：
【辩论焦点】总结双方分歧的核心点，以及辩论过程中立场是否有松动
【证据权衡】判断哪一方的论据更扎实、更经得起对方反驳
【最终研判】给出倾向（看多/看空/中性）及核心理由
要求：基于多轮辩论中论据的质量与攻防表现裁决，不偏袒任何一方。"""


# 主笔：综合分析+研判，产出最终报告（出报告，不出实盘指令）
WRITER_PROMPT = """你是投研报告主笔。综合"分析师报告"和"多空研判"，撰写一份最终投研报告：
一、核心结论（一句话给出投资倾向）
二、关键依据（支撑结论的 3-5 条核心论据）
三、风险提示（需警惕的下行风险）
要求：结论要有依据可循、风险要诚实披露；这是研究报告，不提供具体买卖/实盘指令。"""


# ========== 三个 agent 节点 ==========
def analyst_node(state: InvestState) -> dict:
    """① 分析师：取数 → LLM 分析 → 写入 analysis 字段。"""
    print("① 分析师：取数 + 分析中…", flush=True)   
    symbol = state["symbol"]
    data = gather_data(symbol)              # 取齐四维数据

    # 调 LLM 生成分析报告（system 设定角色，human 喂数据）
    resp = llm.invoke([
        SystemMessage(content=ANALYST_PROMPT),
        HumanMessage(content=f"标的：{symbol}\n\n数据：\n{data}"),
    ])
    # 只更新 analysis 字段，写回共享 state（研究员会从这里读）
    return {"analysis": resp.content}


# ========== 多空辩论的三个节点 ==========
def bull_node(state: InvestState) -> dict:
    """② 多头：读 analysis + 辩论记录 → 发言（首轮陈述 or 回应空头）→ 追加到 debate_history。
 
    改动3：相对一轮版的三处变化——
        (a) 计算 round_no，知道自己是第几轮（一轮版无轮次概念）
        (b) 读 debate_history（看到完整辩论），而非只读 analysis
        (c) 输出"追加"到 debate_history，而非覆盖写入 bull_view
    """
    round_no = state.get("debate_round", 0) + 1      # (a) 当前进行到第几轮
    print(f"② 多头研究员（第 {round_no} 轮）…", flush=True)
    history = state.get("debate_history", "")         # (b) 读已有辩论记录
    resp = llm.invoke([
        SystemMessage(content=BULL_PROMPT),
        HumanMessage(content=(
            f"分析师报告：\n{state['analysis']}\n\n"
            # 空记录 → 提示首轮陈述；有记录 → 让它回应空头
            f"当前辩论记录：\n{history or '（辩论刚开始，请做多头首轮陈述）'}\n\n"
            f"请做多头第 {round_no} 轮发言。"
        )),
    ])
    # (c) 追加本轮发言（累积，下个节点能看到完整历史）
    return {"debate_history": history + f"\n\n【多头·第{round_no}轮】\n{resp.content}"}


def bear_node(state: InvestState) -> dict:
    """③ 空头：读 analysis + 辩论记录 → 逐条反驳 → 追加到 debate_history，并把轮次 +1。
 
    ★改动4：相对一轮版的变化——
        (a) 改读/追加 debate_history（一轮版是读 bull_view、写 bear_view）
        (b) 新增 debate_round += 1 —— 这是控制循环的计数器，一轮版没有
    """
    round_no = state.get("debate_round", 0) + 1
    print(f"③ 空头研究员（第 {round_no} 轮）…", flush=True)
    history = state.get("debate_history", "")
 
    resp = llm.invoke([
        SystemMessage(content=BEAR_PROMPT),
        HumanMessage(content=(
            f"分析师报告：\n{state['analysis']}\n\n"
            f"当前辩论记录：\n{history}\n\n"
            f"请做空头第 {round_no} 轮发言，重点反驳多头最新一轮的论据。"
        )),
    ])
    return {
        # (a) 追加空头本轮发言
        "debate_history": history + f"\n\n【空头·第{round_no}轮】\n{resp.content}",
        # (b) 一轮（多头+空头各一次）结束，轮次 +1 —— 这个值决定循环是否继续
        "debate_round": round_no,
    }

# ★改动：全新函数。条件边的判断逻辑，一轮版没有（它没有循环，bear 后直接 judge）
def should_continue_debate(state: InvestState) -> str:
    """条件边：空头发言后判断走向。
 
    没到 N 轮 → 返回 'bull'，回多头开下一轮；
    到了 N 轮 → 返回 'judge'，辩论结束交给裁判。
    """
    if state["debate_round"] < MAX_DEBATE_ROUNDS:
        return "bull"     # 继续下一轮辩论
    return "judge"        # 辩论结束
 


def judge_node(state: InvestState) -> dict:
    """④ 研究经理：读【完整多轮辩论记录】→ 中立裁决 → 写入 debate_view。
 
    ★改动6：相对一轮版，输入从"bull_view + bear_view 两段"改为"完整 debate_history"，
            这样裁判看到的是多轮攻防全过程，而非一次性的两段陈述。
    """
    print("④ 研究经理：权衡裁决…", flush=True)
    resp = llm.invoke([
        SystemMessage(content=JUDGE_PROMPT),
        HumanMessage(content=(
            f"分析师报告：\n{state['analysis']}\n\n"
            f"完整辩论记录：\n{state['debate_history']}"   # 喂全程辩论，而非单条
        )),
    ])
    return {"debate_view": resp.content}

def writer_node(state: InvestState) -> dict:
    """③ 报告主笔：综合 analysis + debate_view → 写入 final_report 字段。"""
    print("⑤ 主笔：撰写报告中…", flush=True)
    resp = llm.invoke([
        SystemMessage(content=WRITER_PROMPT),
        HumanMessage(content=(                          # 同时喂分析和研判
            f"分析师报告：\n{state['analysis']}\n\n"
            f"多空研判：\n{state['debate_view']}"
        )),
    ])
    return {"final_report": resp.content}


def build_research_team():
    """analyst → bull → bear → judge → writer，多空辩论 + 中立裁决。"""
    g = StateGraph(InvestState)

    # 注册五个节点
    g.add_node("analyst", analyst_node)
    g.add_node("bull", bull_node)        # 多头
    g.add_node("bear", bear_node)        # 空头
    g.add_node("judge", judge_node)      # 研究经理（裁判）
    g.add_node("writer", writer_node)

    # 连边：分析 → 多头 → 空头（看到多头论据）→ 裁决 → 成文
    g.add_edge(START, "analyst")
    g.add_edge("analyst", "bull")
    g.add_edge("bull", "bear")
    # g.add_edge("bear", "judge") # 这部分被条件取代了
    # ★改动7：核心改动。一轮版这里是 g.add_edge("bear", "judge") 一条固定边；
    #         多轮版改成条件边：空头之后按轮次决定「回多头继续辩」还是「去裁判」，
    #         "bull": "bull" 这条回边就是循环的来源。
    g.add_conditional_edges(
        "bear",                          # 从空头节点出发
        should_continue_debate,          # 用这个函数判断去向
        {
            "bull": "bull",              # 返回 'bull' → 回多头，开下一轮（← 形成循环）
            "judge": "judge",            # 返回 'judge' → 辩论结束，去裁判
        },
    )
 


    g.add_edge("judge", "writer")
    g.add_edge("writer", END)

    return g.compile()


# ========== 本地测试入口 ==========
if __name__ == "__main__":
    team = build_research_team()
 
    # ★改动8（初始 state）：相对一轮版，新增 debate_history="" 和 debate_round=0；
    #                      bull_view/bear_view 仍保留占位以兼容 state 定义，多轮模式下不再使用。
    result = team.invoke({
        "symbol": "泡泡玛特",
        "messages": [],
        "stock_data": {},
        "financials": {},
        "sentiment": {},
        "analysis": "",
        "bull_view": "",          # 多轮模式下不再单独使用，保留占位
        "bear_view": "",          # 同上
        "debate_history": "",     # 辩论记录（每轮累积）—— 多轮新增
        "debate_round": 0,        # 轮次计数，从 0 开始 —— 多轮新增
        "debate_view": "",
        "final_report": "",
        "retry_count": 0,
        "thread_id": "research-test-001",
    })
 

  # ★改动（打印部分）：相对一轮版，把"分别打印 bull_view / bear_view"改为
    #                    "打印完整 debate_history"，多轮来回交锋全程可见。
    print("\n===== ① 分析师报告 =====")
    print(result["analysis"])
    print("\n===== ② 多空多轮辩论全记录 =====")
    print(result["debate_history"])
    print("\n===== ③ 研究经理裁决 =====")
    print(result["debate_view"])
    print("\n===== ④ 最终投研报告 =====")
    print(result["final_report"])