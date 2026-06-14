from fastapi import FastAPI
from fastapi import HTTPException
from fastapi import Request
from temporalio.client import Client
from temporal_workflow.workflow import InvestWorkflow
from pydantic import BaseModel
from langgraph.checkpoint.postgres import PostgresSaver
from src.graph import build_graph
from src.react_agent import build_react_graph
from src.supervisor import route
from dotenv import load_dotenv
import os
import time
import asyncio
import json
from fastapi.responses import StreamingResponse
from langgraph.checkpoint.memory import InMemorySaver # 同步
import redis.asyncio as aioredis
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver # 异步

load_dotenv()
stream_redis = aioredis.from_url(
    os.getenv("REDIS_URL_2", "redis://localhost:6379/2"),
    decode_responses=True,   # xread 返回 str 而非 bytes
)
STREAM_TTL = 3600  # 事件流保留 1 小时
TASK_TTL = 3600  # task 元数据保留时间


TEMPORAL_ADDRESS = os.getenv("TEMPORAL_ADDRESS", "localhost:7233")
DB_URL = os.getenv("POSTGRES_DB_URL", "postgresql://ceap:ceap_dev@localhost:5432/langgraph_db")
app = FastAPI()


class AnalysisRequest(BaseModel):
    """请求体：用户的投研问题"""
    question: str  # 例："泡泡玛特未来半年走势如何？"


class AnalysisResponse(BaseModel):
    """响应体：分析结果和线程ID"""
    analysis: str   # LLM 生成的投研分析文字
    thread_id: str  # 本次运行的唯一 ID，可用于追踪和断点续跑


def _sse(data: dict) -> str:
    """格式化成 SSE 事件帧：data: <json>\n\n"""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


def _to_event(ev: dict):
    """astream_events 原始事件 → 前端友好的 {type,...}；无关事件返回 None。"""
    kind = ev["event"]
    if kind == "on_chat_model_stream":
        chunk = ev["data"]["chunk"]
        if chunk.content:
            return {"type": "token", "content": chunk.content}
    elif kind == "on_tool_start":
        return {"type": "tool_call", "name": ev["name"], "input": ev["data"].get("input")}
    elif kind == "on_tool_end":
        output = ev["data"].get("output")
        content = getattr(output, "content", output)
        try:
            content = json.loads(content) if isinstance(content, str) else content
        except (json.JSONDecodeError, TypeError):
            pass
        return {"type": "tool_result", "name": ev["name"], "output": content}
    return None



async def _run_agent_to_stream(task_id: str, question: str, resume: bool = False):
    """后台跑 ReAct，每个事件 XADD 进 Redis Stream（与 SSE 请求解耦）。"""
    stream_key = f"stream:{task_id}"
    task_key = f"task:{task_id}"
    thread_id = f"react-{task_id}"
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 15}
    inputs = {"messages": [{"role": "user", "content": question}], "thread_id": thread_id}

    # memory = InMemorySaver()   # 这步先留 InMemorySaver，下一步换 AsyncPostgresSaver 才有崩溃恢复
    # graph = build_react_graph(memory) 跟InMemorySaver一起
    # resume 时 input=None：LangGraph 从 checkpoint 恢复，已完成的节点/工具不重跑
    inputs = None if resume else {"messages": [{"role": "user", "content": question}], "thread_id": thread_id}
    async def push(event: dict):
        await stream_redis.xadd(stream_key, {"data": json.dumps(event, ensure_ascii=False)})
    # 标记 running —— 崩溃后启动扫描靠这个状态找未完成的 task
    await stream_redis.hset(task_key, mapping={"status": "running", "question": question})
    await stream_redis.expire(task_key, TASK_TTL)
    try:
        # 异步 PG checkpointer：每个节点执行完，graph state 落 PG。
        # 进程崩了 state 还在，用同一个 thread_id 重跑能从断点续（已完成的节点/工具不重跑）。
        async with AsyncPostgresSaver.from_conn_string(DB_URL) as memory:
            graph = build_react_graph(memory)
            async for ev in graph.astream_events(inputs, config=config, version="v2"):
                event = _to_event(ev)
                if event:
                    await push(event)
            await push({"type": "done", "thread_id": thread_id})
        await stream_redis.hset(task_key, "status", "done")
    except Exception as e:
        await push({"type": "error", "message": str(e)})
        await stream_redis.hset(task_key, "status", "failed")
    finally:
        await stream_redis.expire(stream_key, STREAM_TTL)   # 流读完后 1 小时自动清理



async def _sse_from_stream(task_id: str, last_id: str = "0"):
    """从 Redis Stream 读事件推成 SSE；last_id 支持断线重连续读。"""
    stream_key = f"stream:{task_id}"
    while True:
        resp = await stream_redis.xread({stream_key: last_id}, block=15000, count=20)
        if not resp:
            yield ": keepalive\n\n"     # 15 秒无新事件，发心跳保活，防中间件掐断
            continue
        for _key, entries in resp:
            for entry_id, fields in entries:
                last_id = entry_id
                # SSE 的 id 字段：前端断线重连时会用 Last-Event-ID 带回来
                yield f"id: {entry_id}\ndata: {fields['data']}\n\n"
                if json.loads(fields["data"]).get("type") in ("done", "error"):
                    return   # 流结束

# async def _react_event_stream(question: str):
#     """
#     跑 ReAct 图，用 astream_events 把每一步转成 SSE 事件。
#     事件类型：
#       token        —— LLM 流式输出的文字
#       tool_call    —— 开始调某个工具
#       tool_result  —— 工具返回结果
#       done / error —— 结束 / 出错
#     """
#     thread_id = f"react-{int(time.time())}"
#     config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 15}
#     inputs = {
#         "messages": [{"role": "user", "content": question}],
#         "thread_id": thread_id,
#     }

#     # 第一版用内存 checkpointer，专注把事件推出去；
#     # 断线恢复要等接上 AsyncPostgresSaver + 后台 worker（下一版）
#     memory = InMemorySaver()
#     graph = build_react_graph(memory)

#     try:
#         # astream_events v2：拿到节点、LLM、工具的细粒度事件流
#         async for ev in graph.astream_events(inputs, config=config, version="v2"):
#             kind = ev["event"]

#             if kind == "on_chat_model_stream":          # LLM 吐 token
#                 chunk = ev["data"]["chunk"]
#                 if chunk.content:
#                     yield _sse({"type": "token", "content": chunk.content})

#             elif kind == "on_tool_start":               # 开始调工具
#                 yield _sse({
#                     "type": "tool_call",
#                     "name": ev["name"],
#                     "input": ev["data"].get("input"),
#                 })

#             elif kind == "on_tool_end":                 # 工具返回
#                 output = ev["data"].get("output")
#                 # output 是 ToolMessage 对象，不能直接 json 序列化 —— 取 .content 拿到工具真正的 JSON 字符串
#                 content = getattr(output, "content", output)
#                 try:
#                     content = json.loads(content) if isinstance(content, str) else content
#                 except (json.JSONDecodeError, TypeError):
#                     pass
#                 yield _sse({
#                     "type": "tool_result",
#                     "name": ev["name"],
#                     "output": content,        # 截断，避免单帧过大
#                 })

#         yield _sse({"type": "done", "thread_id": thread_id})

#     except Exception as e:
#         # 流中途出错也要推一帧，让前端知道，而不是连接静默断开
#         yield _sse({"type": "error", "message": str(e)})


@app.post("/invest/stream")
async def invest_stream(req: AnalysisRequest):
    """启动后台 agent，SSE 从 Redis Stream 读。响应头带 task_id 供断线重连。"""
    task_id = str(int(time.time() * 1000))
    asyncio.create_task(_run_agent_to_stream(task_id, req.question))  # 后台跑，不占请求
    return StreamingResponse(
        _sse_from_stream(task_id),
        media_type="text/event-stream",
        headers={"X-Task-Id": task_id},   # 前端记下来，断线用它重连
    )


@app.get("/invest/stream/{task_id}")
async def invest_stream_resume(task_id: str, request: Request):
    """断线重连：从 Last-Event-ID 之后续读同一个 task 的事件流。"""
    last_id = request.headers.get("Last-Event-ID", "0")
    return StreamingResponse(
        _sse_from_stream(task_id, last_id),
        media_type="text/event-stream",
    )

# ============================================================
# 两条线的执行逻辑（抽出来给三个 endpoint 共用，避免重复）
# 都是同步阻塞调用（graph.invoke + PG IO）
# ============================================================
def _run_fixed(question: str) -> tuple[str, str]:
    """
    固定线：构建 StateGraph 跑全套投研（行情/财报/舆情并行 → LLM analyze）。

    确定性流程，每次都跑同样的节点，适合标准投研。
    返回 (分析文字, thread_id)。
    """
    thread_id = f"api-{int(time.time())}"
    config = {"configurable": {"thread_id": thread_id}}
    inputs = {
        "messages": [{"role": "user", "content": question}],
        "thread_id": thread_id,
    }
    # 用 with 打开 PostgreSQL checkpointer，每个节点执行完状态自动落盘
    with PostgresSaver.from_conn_string(DB_URL) as memory:
        graph = build_graph(memory)
        result = graph.invoke(inputs, config=config)
        print(f"固定线返回结果：{result['analysis']}")
        return result["analysis"], thread_id


def _run_react(question: str) -> tuple[str, str]:
    """
    ReAct 线：LLM 自己决定调哪些工具，多步推理。

    recursion_limit 限制最大推理步数，防止反复调工具失控。
    最终答案在最后一条 AI 消息（不是固定线的 result["analysis"]）。
    返回 (分析文字, thread_id)。
    """
    thread_id = f"react-{int(time.time())}"
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 15}
    inputs = {
        "messages": [{"role": "user", "content": question}],
        "thread_id": thread_id,
    }
    with PostgresSaver.from_conn_string(DB_URL) as memory:
        graph = build_react_graph(memory)
        result = graph.invoke(inputs, config=config)
        answer = result["messages"][-1].content
        print(f"ReAct 线返回结果：{answer}")
        return answer, thread_id


# ============================================================
# 三个 endpoint：
#   /invest         统一智能入口，Supervisor 自动分流
#   /analyze        强制走固定线（Temporal Activity 直连 / 调试）
#   /analyze_react  强制走 ReAct 线（调试）
# ============================================================
@app.post("/invest", response_model=AnalysisResponse)
async def invest(req: AnalysisRequest):
    """
    统一智能入口：Supervisor 自动判断走固定线还是 ReAct 线。

    流程：
    1. route() 调用三层分类器（Redis 缓存 → 规则 → LLM）拿到问题类型
    2. 按类型映射线路：4 类标准投研 → 固定线；general/长尾 → ReAct 线
    3. 分发到对应执行函数
    """
    decision = await route(req.question)
    line = decision["line"]
    print(f"Supervisor 路由：{decision['question_type']} → {line} 线")

    # route 是 async，但 _run_* 是同步阻塞调用；
    # 用 to_thread 把阻塞调用丢到线程池，避免卡住事件循环
    if line == "fixed":
        answer, thread_id = await asyncio.to_thread(_run_fixed, req.question)
    else:
        answer, thread_id = await asyncio.to_thread(_run_react, req.question)

    return AnalysisResponse(analysis=answer, thread_id=thread_id)


@app.post("/analyze", response_model=AnalysisResponse)
def analyze(req: AnalysisRequest):
    """
    强制走固定线（Temporal Activity 直连 / 调试用）。

    注意：每次都是新 thread_id，不做断点续跑；
    如需断点续跑，由调用方传入固定 thread_id。
    """
    answer, thread_id = _run_fixed(req.question)
    return AnalysisResponse(analysis=answer, thread_id=thread_id)


@app.post("/analyze_react", response_model=AnalysisResponse)
def analyze_react(req: AnalysisRequest):
    """强制走 ReAct 线（调试用）。"""
    answer, thread_id = _run_react(req.question)
    return AnalysisResponse(analysis=answer, thread_id=thread_id)


# ============================================================
# 审核回调接口
# ============================================================
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
        # 提取 message_id
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
@app.on_event("startup")
async def _resume_unfinished_tasks():
    """进程启动时扫描上次崩溃遗留的 running task，用原 thread_id 从 checkpoint 续跑。"""
    async for task_key in stream_redis.scan_iter("task:*"):
        if await stream_redis.hget(task_key, "status") == "running":
            task_id = task_key.split(":", 1)[1]
            question = await stream_redis.hget(task_key, "question") or ""
            print(f"[恢复] 发现未完成 task {task_id}，从 checkpoint 续跑")
            asyncio.create_task(_run_agent_to_stream(task_id, question, resume=True))

@app.get("/health")
def health():
    """健康检查接口"""
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)