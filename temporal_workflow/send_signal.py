"""
手动发送 Temporal Signal 脚本
当 Lark 按钮不可用时，审核人用这个脚本手动发送审核结果

用法：
    python -m temporal_workflow.send_signal <workflow_id> <decision>

参数：
    workflow_id：Temporal Workflow ID
                 例：invest-client001-09992HK-2026-06-13-buy_advice
    decision：   审核结果，只接受 approve 或 reject
                 approve = 通过，报告正式发送
                 reject  = 拒绝，报告终止

示例：
    python -m temporal_workflow.send_signal invest-client001-09992HK-2026-06-13-buy_advice approve
    python -m temporal_workflow.send_signal invest-client001-09992HK-2026-06-13-buy_advice reject
"""

import sys
import asyncio
import os
from dotenv import load_dotenv
from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow
from datetime import timedelta

load_dotenv()

# Temporal Server 地址（从环境变量读，默认本地）
TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")


async def main():
    # --------------------------------------------------------
    # 参数校验
    # --------------------------------------------------------

    # 检查参数数量，不够则打印用法说明退出
    if len(sys.argv) < 3:
        print("❌ 参数不足")
        print()
        print("用法：")
        print("  python -m temporal_workflow.send_signal <workflow_id> <decision>")
        print()
        print("参数说明：")
        print("  workflow_id  Temporal Workflow ID")
        print("               例：invest-client001-09992HK-2026-06-13-buy_advice")
        print("  decision     审核结果：approve（通过）或 reject（拒绝）")
        print()
        print("示例：")
        print("  python -m temporal_workflow.send_signal invest-client001-09992HK-2026-06-13-buy_advice approve")
        sys.exit(1)

    workflow_id = sys.argv[1]  # 第一个参数：Workflow ID
    decision = sys.argv[2]     # 第二个参数：审核结果

    # 校验 decision 值，防止非法输入
    if decision not in ("approve", "reject"):
        print(f"❌ 无效的审核决定：{decision}")
        print("   只接受 approve 或 reject")
        sys.exit(1)

    print(f"正在发送审核 Signal...")
    print(f"  Workflow ID：{workflow_id}")
    print(f"  审核结果：  {decision}")
    print()

    # --------------------------------------------------------
    # 连接 Temporal Server，发 Signal
    # --------------------------------------------------------
    try:
        # 连接 Temporal Server
        client = await Client.connect(TEMPORAL_ADDRESS, rpc_timeout=timedelta(seconds=10))

        # 获取已有 Workflow 的 handle（不新建 Workflow，只是获取引用）
        handle = client.get_workflow_handle(workflow_id)

        # 发 Signal——唤醒正在 wait_condition 等待的 Workflow
        # review_signal 是 workflow.py 里 @workflow.signal 装饰的方法
        await handle.signal(InvestWorkflow.review_signal, {
        "decision": decision,
        "message_id": ""  # 手动发 Signal 没有 message_id
        })

        print(f"✅ Signal 发送成功")
        print(f"   Workflow [{workflow_id}] 已收到审核结果：{decision}")
        print(f"   Workflow 将从等待状态唤醒，继续执行后续步骤")

    except Exception as e:
        print(f"❌ Signal 发送失败：{e}")
        print()
        print("可能的原因：")
        print("  1. Temporal Server 未启动（检查 docker ps | grep temporal）")
        print("  2. Workflow ID 不存在或已完成")
        print("  3. Workflow 未处于等待 Signal 的状态")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())