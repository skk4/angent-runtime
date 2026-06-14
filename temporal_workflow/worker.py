from temporalio.client import Client
from temporalio.worker import Worker
from temporal_workflow.workflow import InvestWorkflow
from temporal_workflow.activities import run_agent, human_review, deliver_report, send_failure_alert
import os
import asyncio
from dotenv import load_dotenv

load_dotenv()


async def main():
    # 连接 Temporal Server
    client = await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"))

    # 创建 Worker，注册 Workflow 和 Activity
    worker = Worker(
        client,
        task_queue="invest-task-queue",
        workflows=[InvestWorkflow],
        activities=[run_agent, human_review, deliver_report, send_failure_alert],
    )

    print("Worker 启动，监听 invest-task-queue...")


    # 启动 Worker，开始处理任务
    # 等待 Workflow 执行时，Worker 会自动调用注册的 Activity 函数
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())