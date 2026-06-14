"""
Supervisor —— 双线兼容的意图路由层。

不自己做分类（复用 question_classifier 的三层策略：Redis 缓存 → 规则 → LLM），
只做"问题类型 → 线路"的映射决策：

- 4 类标准投研问题（buy_advice/financial/risk/price_analysis）→ 固定线
    它们有明确的分析范式，固定 pipeline 的确定性正好匹配（可靠、可复现、成本可控）
- general 及任何未知类型 → ReAct 线
    长尾/开放/对比类问题，最需要 ReAct 的灵活多步推理

这样三层分类器的缓存、规则、ClickHouse 日志全部复用，路由判断顺带享受缓存。
"""

from src.question_classifier import classify_question

# 走固定线的标准投研问题类型；其余（含 general 与未知）默认走 ReAct
FIXED_LINE_TYPES = {"buy_advice", "financial", "risk", "price_analysis"}


async def route(question: str, client_id: str = "default", symbol: str = "") -> dict:
    """
    判断 query 走固定线还是 ReAct 线。

    复用 classify_question 拿到问题类型，再按 FIXED_LINE_TYPES 映射到线路。
    兜底：general 或未知类型默认走 ReAct（长尾问题最需要灵活性）。

    Args:
        question:  用户原始问题
        client_id: 客户 ID（透传给分类器，用于日志）
        symbol:    标的代码（透传给分类器，用于日志）

    Returns:
        {"line": "fixed" | "react", "question_type": "..."}
    """
    q_type = await classify_question(question, client_id, symbol)
    line = "fixed" if q_type in FIXED_LINE_TYPES else "react"
    return {"line": line, "question_type": q_type}
