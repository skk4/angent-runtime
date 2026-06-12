from temporalio.client import Client
from temporalio.worker import Worker
from temporal_workflow.workflow import InvestWorkflow
from temporal_workflow.activities import run_agent, human_review, deliver_report



async def main():
    # 连接 Temporal Server
    client = await Client.connect("localhost:7233")

    # 创建 Worker，注册 Workflow 和 Activity
    worker = Worker(
        client,
        task_queue="invest-task-queue",
        workflows=[InvestWorkflow],
        activities=[run_agent, human_review, deliver_report],
    )

    print("Worker 启动，监听 invest-task-queue...")


    # 启动 Worker，开始处理任务
    await worker.run()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())