from fastapi import FastAPI
from pydantic import BaseModel
from langgraph.checkpoint.postgres import PostgresSaver
from src.graph import build_graph
from dotenv import load_dotenv
import os
import time

load_dotenv()

DB_URL = os.getenv("POSTGRES_DB_URL", "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db")

# DB_URL = "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db"
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
    # DB_URL = "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db"

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


@app.get("/health")
def health():
    """健康检查接口，供监控系统或 Docker healthcheck 使用"""
    return {"status": "ok"}



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=  "0.0.0.0", port=8100)