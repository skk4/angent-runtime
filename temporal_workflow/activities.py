from temporalio import activity
import asyncio

# ============================================================
# Activity 1：调 LangGraph Runtime 跑投研分析
# ============================================================
@activity.defn
async def run_agent(question: str) -> str:
    """
    调用你的 LangGraph Runtime，返回分析结果。
    现在先用 DEMO 数据，后面替换成真实调用。
    """
    activity.logger.info(f"run_agent 开始，问题：{question}")
    
    # TODO：后面替换成真实的 LangGraph 调用
    # 现在先模拟耗时操作
    await asyncio.sleep(2)
    
    # 模拟返回分析结果
    return f"泡泡玛特投研分析（DEMO）：当前股价 168.5 HKD，收入同比 +184.7%，净利润同比 +316.3%，问题：{question}"


# ============================================================
# Activity 2：人工审核
# 现在模拟自动通过，后面改成真实的 Temporal Signal 等待
# ============================================================
@activity.defn
async def human_review(analysis: str) -> str:
    """
    等待人工审核。
    现在自动通过，后面用 Temporal Signal 实现真实的人工等待。
    """
    activity.logger.info(f"human_review 开始，内容长度：{len(analysis)}")
    
    # TODO：后面改成 Temporal Signal 等待真实审核
    # 现在直接通过
    await asyncio.sleep(1)
    
    return "approve"


# ============================================================
# Activity 3：交付报告（精确一次，不会重复发送）
# ============================================================
@activity.defn
async def deliver_report(analysis: str, approval: str) -> str:
    """
    发送最终报告。
    被 @activity.defn 装饰后，Temporal 保证精确一次执行——
    就算进程崩了重启，这个函数不会被重复调用。
    """
    activity.logger.info(f"deliver_report 开始，审核结果：{approval}")
    
    if approval != "approve":
        return "报告被拒绝，未发送"
    
    # TODO：后面改成真实发送（Lark / 邮件 / 数据库落库）
    print(f"\n{'='*50}")
    print(f"📊 投研报告已发送")
    print(f"{analysis}")
    print(f"{'='*50}\n")
    
    return "delivered"




