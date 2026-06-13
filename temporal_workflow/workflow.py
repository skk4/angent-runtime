from temporalio import workflow
from datetime import timedelta

# workflow.unsafe.imports_passed_through() 是一个上下文管理器
# Temporal 的 Workflow 代码在沙箱环境里运行（为了保证确定性重放）
# 沙箱会拦截某些 import，导致普通 import 报错
# 用这个上下文管理器告诉 Temporal："这些 import 直接透传，不拦截"
with workflow.unsafe.imports_passed_through():
    from .activities import run_agent, human_review, deliver_report


@workflow.defn
# @workflow.defn 告诉 Temporal：这个 class 是一个 Workflow 定义
# Temporal 会用它生成 Workflow 类型名（默认用类名 InvestWorkflow）
class InvestWorkflow:

    @workflow.run
    # @workflow.run 标记这是 Workflow 的入口函数
    # 每个 @workflow.defn 的 class 必须有且只有一个 @workflow.run 方法
    async def run(self, question: str) -> str:
        """
        投研 Workflow 主流程：
        1. 调 LangGraph Runtime 跑 Agent 分析
        2. 等人工审核
        3. 审核通过后交付报告

        为什么用 execute_activity 而不是直接调函数：
        - Temporal 通过 execute_activity 把 Activity 的执行记录持久化
        - 进程崩了重启，已完成的 Activity 不会重跑（精确一次）
        - 直接调函数没有这个保证
        """

        # Step 1：调 LangGraph Runtime 跑投研分析
        # schedule_to_close_timeout：从调度到完成的总超时
        # 30 秒内 run_agent 必须跑完，否则 Temporal 自动重试
        analysis = await workflow.execute_activity(
            run_agent,
            question,
            schedule_to_close_timeout=timedelta(seconds=30),
        )

        # Step 2：等人工审核
        # start_to_close_timeout：从开始执行到完成的超时
        # 1 小时内必须审核完，否则超时失败
        # 后面改成 Temporal Signal，可以等天级别
        approval = await workflow.execute_activity(
            human_review,
            analysis,
            start_to_close_timeout=timedelta(hours=1),
        )

        # Step 3：交付报告（精确一次）
        # deliver_report 有两个参数，用 args=[] 列表传入
        # Temporal 保证：就算进程在这里崩了重启，deliver_report 不会重复执行
        # 这正是 LangGraph Checkpointer 做不到的——它只保证状态恢复，不保证副作用精确一次
        result = await workflow.execute_activity(
            deliver_report,
            args=[analysis, approval],
            schedule_to_close_timeout=timedelta(seconds=300),
        )

        return result