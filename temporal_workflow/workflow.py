from temporalio import workflow
from datetime import timedelta
from typing import Optional
from temporalio.common import RetryPolicy
import asyncio

# workflow.unsafe.imports_passed_through() 是一个上下文管理器
# Temporal 的 Workflow 代码在沙箱环境里运行（为了保证确定性重放）
# 沙箱会拦截某些 import，导致普通 import 报错
# 用这个上下文管理器告诉 Temporal："这些 import 直接透传，不拦截"
with workflow.unsafe.imports_passed_through():
    from .activities import run_agent, human_review, deliver_report, send_failure_alert


@workflow.defn
# @workflow.defn 告诉 Temporal：这个 class 是一个 Workflow 定义
# Temporal 会用它生成 Workflow 类型名（默认用类名 InvestWorkflow）
class InvestWorkflow:

    """
    投研 Agent Workflow

    流程：
        1. run_agent：调 LangGraph Runtime 跑投研分析（行情/财报/舆情 → LLM）
        2. 等待人工审核 Signal（最多 24 小时，进程可以死，状态持久化在 Temporal DB）
        3. deliver_report：审核通过发报告，拒绝则终止

    Signal 机制：
        审核人调 send_signal.py 发 Signal → Workflow 从暂停点唤醒 → 继续执行
        这是 Dify 做不到的能力：天级别的等待，进程重启状态不丢

    异常保障：
        - 每个 Activity 有独立 RetryPolicy
        - Workflow 最终失败时发 Lark 告警
        - Temporal 持久化保证进程崩了能恢复       
    """

    def __init__(self):
        # Workflow 内部可以定义成员变量，保持状态
        # 这些变量的值会随着 Workflow 执行不断变化
        # Temporal 会自动把它们持久化到数据库，保证进程崩了重启后状态不丢
        # 审核决定：None = 还在等待，"approve" = 通过，"reject" = 拒绝
        # 初始化为 None，等 Signal 到了再赋值
        self._review_decision: Optional[str] = None
        self._message_id: str = ""  # 原卡片消息 ID

    @workflow.signal
    def review_signal(self, signal_data: dict):
        """
        定义 Signal 处理函数，接收审核结果。
        审核人调 send_signal.py 发 Signal，参数是 "approve" 或 "reject", message_id
        Temporal 收到 Signal 后会自动调用这个函数，传入 decision 参数。
        函数里把审核结果保存到成员变量里，Workflow 主流程从暂停点唤醒继续执行。
        """
        decision = signal_data.get("decision")
        workflow.logger.info(f"收到审核 Signal：{decision}")

        # 校验 decision 值，防止非法输入
        if decision not in ("approve", "reject"):
            workflow.logger.error(f"无效的审核决策：{decision}, 忽略")
            return
        
        self._review_decision = decision  
        self._message_id = signal_data.get("message_id", "")
        workflow.logger.info(f"收到审核 Signal：{decision}, message_id：{self._message_id}") 


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

        workflow_id = workflow.info().workflow_id

        # Step 1：调 LangGraph Runtime 跑投研分析
        # schedule_to_close_timeout：从调度到完成的总超时
        # 30 秒内 run_agent 必须跑完，否则 Temporal 自动重试

        try:
            analysis = await workflow.execute_activity(
                run_agent,
                question,
                schedule_to_close_timeout=timedelta(seconds=300),
                retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=2),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=3,
                    )
            )

            #Step 2：发审核通知（告诉审核人有报告需要审核）
            # human_review 现在只负责发通知，不等结果
            # 结果通过 Signal 异步传回（Step 3）
            # 发 Lark 消息失败不阻断，有降级处理
            await workflow.execute_activity(
                human_review,
                analysis,
                start_to_close_timeout=timedelta(seconds=60),
                retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=2),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=30),
                        maximum_attempts=3,
                    )
            )


            # --------------------------------------------------------
            # Step 3：等待人工审核 Signal（最多 24 小时）
            #
            # wait_condition 的工作原理：
            # - Workflow 挂起，不消耗任何 CPU/内存
            # - 进程可以完全退出，状态持久化在 Temporal DB
            # - Signal 到了（review_signal 被调用）→ _review_decision 有值 → 条件满足 → 唤醒
            # - 超时（24小时没收到 Signal）→ asyncio.TimeoutError
            #
            # 这正是 LangGraph Checkpointer 做不到的：
            # Checkpointer 只保证状态恢复，不保证进程可以长期挂起等外部事件
            try:
                await workflow.wait_condition(lambda: self._review_decision is not None, timeout=timedelta(seconds=5))
                workflow.logger.info(f"收到审核结果：{self._review_decision}")
            except TimeoutError:
                workflow.logger.error("等待审核超时（24小时），自动拒绝")
                self._review_decision = "reject"
                self._message_id = ""
                # 不 return，继续走 Step 4 的 deliver_report，更新卡片状态
            # --------------------------------------------------------
            # Step 4：根据审核结果交付报告
            # deliver_report 被 @activity.defn 装饰，Temporal 保证精确一次执行
            # 就算进程在这里崩了重启，deliver_report 不会重复发送
            result = await workflow.execute_activity(
                deliver_report,
                #deliver_report 需要两个参数，是 "approve" 或 "reject"
                args=[analysis, self._review_decision, self._message_id],#加 message_id
                schedule_to_close_timeout=timedelta(seconds=300),
                retry_policy=RetryPolicy(
                        initial_interval=timedelta(seconds=2),
                        backoff_coefficient=2.0,
                        maximum_interval=timedelta(seconds=60),
                        maximum_attempts=5,  # 最多重试5次
                    )
            )
            workflow.logger.info(f"Workflow 完成，结果：{result}")

            return result
        
        except Exception as e:
            # --------------------------------------------------------
            # 兜底：Workflow 最终失败，发告警通知
            # 用独立的 Activity 发告警，不受主流程影响
            # --------------------------------------------------------
            workflow.logger.error(f"Workflow 最终失败：{e}")
            try:
                await workflow.execute_activity(
                    send_failure_alert,
                    args=[workflow_id, str(e)],
                    schedule_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=2)
                )
            except Exception as alert_e:
                workflow.logger.error(f"告警发送也失败：{alert_e}")
            raise  # 重新抛出，让 Temporal 标记 Workflow 为 Failed