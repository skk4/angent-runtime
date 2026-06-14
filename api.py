from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow
from pydantic import BaseModel
from langgraph.checkpoint.postgres import PostgresSaver
from src.graph import build_graph
from dotenv import load_dotenv
import os
import time

load_dotenv()

TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
DB_URL = os.getenv("POSTGRES_DB_URL", "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db")
app = FastAPI()

class AnalysisRequest(BaseModel):
    """请求体：用户的投研问题"""
    question: str # 例："泡泡玛特未来半年走势如何？"


class AnalysisResponse(BaseModel):
    """响应体：分析结果和线程ID"""
    analysis: str # LLM 生成的投研分析文字
    thread_id: str #本次运行的唯一 ID，可用于追踪和断点续跑


@app.post("/analyze")
def analyze(req: AnalysisRequest, response_model=AnalysisResponse):

    """
    投研分析接口。

    流程：
    1. 生成唯一 thread_id（用时间戳，保证每次请求独立）
    2. 构建 LangGraph StateGraph（注入 PostgreSQL checkpointer）
    3. 调用 graph.invoke 跑完四个节点（行情/财报/舆情并行 → LLM 分析）
    4. 返回分析结果

    注意：这里每次都是新 thread_id，不做断点续跑
    如果需要断点续跑，由调用方（Temporal Activity）传入固定 thread_id
    """

    # 每次请求生成唯一 thread_id，避免 checkpoint 复用导致返回旧结果
    thread_id = f"api-{int(time.time())}"

    # 初始输入：用户问题写入 messages，thread_id 写入 State 供告警使用
    config = {"configurable": {"thread_id": thread_id}}
    inputs = {
        "messages": [{"role": "user", "content": req.question}],
        "thread_id": thread_id
    }

    # 用 with 语句打开 PostgreSQL checkpointer
    # 每个节点执行完状态自动落盘，进程崩了可从断点恢复
    with PostgresSaver.from_conn_string(DB_URL) as memory:
        # 构建 StateGraph 并注入 checkpointer（依赖注入，切换 SQLite/PG 只改这里）
        graph = build_graph(memory)

        # 同步调用图，跑完所有节点返回最终 State
        result = graph.invoke(inputs, config=config)

        print(f"LangGraph 返回结果：{result['analysis']}")

        # 返回分析结果和 thread_id（调用方可用 thread_id 查询历史或继续运行）
        return AnalysisResponse(analysis=result["analysis"], thread_id=thread_id)

# 审核回调接口
@app.post("/api/review/callback")
async def review_callback(request: Request):
    """
    Lark 回调接口（统一处理两种情况）

    情况1：地址验证
        Lark 发 POST + {"type": "url_verification", "challenge": "xxx"}
        原样返回 challenge，完成验证

    情况2：卡片按钮点击
        Lark 发 POST + action 信息
        解析 workflow_id 和 decision，发 Temporal Signal 唤醒 Workflow
    """
    body = await request.json()
    print(f"Lark 回调 body：{body}")

    # 情况1：地址验证——原样返回 challenge
    if body.get("type") == "url_verification":
        return {"challenge": body.get("challenge")}

    # 情况2：卡片按钮点击——解析 action 发 Signal
    try:
        # 从 action value 里取 workflow_id 和 decision
        # 按钮定义里 value = {"workflow_id": "xxx", "decision": "approve"}
        action = body.get("event", {}).get("action", {})  # 注意加 event 层
        value = action.get("value", {})
        workflow_id = value.get("workflow_id")
        decision = value.get("decision")
        #提取 message_id
        message_id = body.get("event", {}).get("context", {}).get("open_message_id", "")

        if not workflow_id or not decision:
            print(f"⚠️ 缺少必要参数：workflow_id={workflow_id}, decision={decision}")
            return {"status": "ok"}

        if decision not in ("approve", "reject"):
            raise HTTPException(status_code=400, detail=f"无效的审核决定：{decision}")

        # 连接 Temporal Server，发 Signal
        client = await Client.connect(TEMPORAL_ADDRESS)
        handle = client.get_workflow_handle(workflow_id)
        await handle.signal(InvestWorkflow.review_signal, {"decision": decision, "message_id": message_id})

        print(f"✅ Signal 已发送：workflow_id={workflow_id}, decision={decision}")
        # 根据审核结果返回不同的卡片更新
        return {
            "toast": {
                "type": "success" if decision == "approve" else "error", 
                "content": f"审核{'已通过' if decision == 'approve' else '已拒绝'}",
            },
            "card": {
                "type": "raw",
                "data": {
                    "schema": "2.0",
                    "body": {
                        "elements": [
                            {
                                "tag": "markdown",
                                "content": f"**审核结果：{'✅ 已通过' if decision == 'approve' else '❌ 已拒绝'}**\n\nWorkflow ID：`{workflow_id}`\n\n报告{'正在发送到群组...' if decision == 'approve' else '已终止'}"
                            }
                        ]
                    }
                }
            }
        }
    except Exception as e:
        print(f"❌ Signal 发送失败：{e}")
        return {"status": "error", "message": str(e)}

@app.get("/health")
def health():
    """健康检查接口"""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)