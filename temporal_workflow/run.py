from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow


async def main():
    # 连接 Temporal Server
    client = await Client.connect("localhost:7233")

    # 启动 Workflow
    workflow_run = await client.start_workflow(
        InvestWorkflow.run,
        "泡泡玛特未来半年股价走势如何？",
        id="invest-workflow-001",  # Workflow 实例 ID，必须全局唯一
        task_queue="invest-task-queue",  # Worker 监听的 Task Queue
    )

    print(f"Workflow 已启动，ID：{workflow_run.id}")

    # 等待 Workflow 完成并获取结果
    result = await workflow_run.result()
    print(f"Workflow 执行结果：{result}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())