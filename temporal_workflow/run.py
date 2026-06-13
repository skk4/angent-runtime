from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
import sys  
from datetime import date
from src.question_classifier import classify_question

# ============================================================
# 从命令行读取参数，支持生产环境灵活调用
# 用法：python -m temporal_workflow.run "问题" "客户ID" "标的代码"
# 示例：python -m temporal_workflow.run "值得买吗？" "client001" "09992HK"
# 不传参数时使用默认值，方便本地开发测试
# ============================================================


# 投研问题：用户想了解什么
question = sys.argv[1] if len(sys.argv) > 1 else "泡泡玛特现在值得买吗？"

# 客户ID：区分不同客户，保证不同客户的结果互相隔离
client_id = sys.argv[2] if len(sys.argv) > 2 else "default"

# 标的代码：分析哪只股票
symbol = sys.argv[3] if len(sys.argv) > 3 else "09992HK"


async def main():

    # 三层问题分类（Redis缓存 → 规则引擎 → LLM兜底）
    # 同类型问题同一天命中同一个 Workflow ID，幂等不重复跑
    question_type = await classify_question(
        question=question,
        client_id=client_id,
        symbol=symbol,
    )
    print(f"问题类型：{question_type}")

    # Workflow ID：客户 + 标的 + 日期 + 问题类型
    # 同一客户同一天同一标的同一类型 → 只跑一次
    # 不同类型（buy_advice/risk/financial）各自独立跑
    workflow_id = (
        f"invest-{client_id}-{symbol}"
        f"-{date.today().isoformat()}"
        f"-{question_type}"
    )

    print(f"问题：{question}")
    print(f"客户：{client_id} | 标的：{symbol}")
    print(f"Workflow ID：{workflow_id}")


    # 连接 Temporal Server
    client = await Client.connect("localhost:7233")

    # 启动 Workflow
    try:
        workflow_run = await client.start_workflow(
            InvestWorkflow.run, # Workflow 入口函数
            question, # 传给 run(self, question) 的参数
            id=workflow_id,  # Workflow 实例 ID，必须全局唯一
            task_queue="invest-task-queue",  # Worker 监听的 Task Queue 必须和 worker.py 一致
            # ALLOW_DUPLICATE_FAILED_ONLY：
            # - 已完成 → 直接返回缓存结果（幂等，省 LLM 费用）
            # - 失败   → 允许重新跑（容错）
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
        )
    except WorkflowAlreadyStartedError:
        # 同 ID 已完成，直接获取已有结果
        print(f"Workflow 已存在，ID：{workflow_id}，正在查询结果...")
        # 不需要 await，get_workflow_handle 是同步方法）
        workflow_run = client.get_workflow_handle(workflow_id)
    print(f"Workflow 已启动，ID：{workflow_run.id}")

    # 等待 Workflow 完成并获取结果
    result = await workflow_run.result()
    print(f"Workflow 执行结果：{result}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())