from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow
from temporalio.common import WorkflowIDReusePolicy
import sys  
from datetime import date
import hashlib

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


# Workflow ID：客户+标的+日期 三段组合
# 同一客户同一天同一标的只跑一次——幂等性的关键
# 例：invest-client001-09992HK-2026-06-13
# workflow_id = f"invest-{client_id}-{symbol}-{date.today().isoformat()}"


# 问题 hash，同一问题同一天只跑一次
question_hash = hashlib.md5(question.encode()).hexdigest()[:8]
workflow_id = f"invest-{client_id}-{symbol}-{date.today().isoformat()}-{question_hash}"

async def main():
    # 连接 Temporal Server
    client = await Client.connect("localhost:7233")

    # 启动 Workflow
    workflow_run = await client.start_workflow(
        InvestWorkflow.run, # Workflow 入口函数
        question, # 传给 run(self, question) 的参数
        id=workflow_id,  # Workflow 实例 ID，必须全局唯一
        task_queue="invest-task-queue",  # Worker 监听的 Task Queue 必须和 worker.py 一致
        id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,  # 生产环境改成REJECT_DUPLICATE
    )

    print(f"Workflow 已启动，ID：{workflow_run.id}")

    # 等待 Workflow 完成并获取结果
    result = await workflow_run.result()
    print(f"Workflow 执行结果：{result}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())