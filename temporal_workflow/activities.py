from temporalio import activity
import os
from dotenv import load_dotenv
import httpx
import json
import hmac
import hashlib
import base64
import time

load_dotenv()



def _build_lark_sign(secret: str) -> tuple[str, str]:
    """
    生成 Lark webhook 签名
    Lark 安全设置开启签名校验后，每次请求必须带 timestamp 和 sign

    Returns:
        (timestamp, sign) 元组
    """
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256
    ).digest()
    sign = base64.b64encode(hmac_code).decode("utf-8")
    return timestamp, sign


async def _get_lark_token() -> str:
    """
    换取 Lark tenant_access_token
    有效期 2 小时，失败抛明确异常让调用方处理
    生产环境应缓存复用，避免频繁换取（TODO）
    """
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        resp = await client.post(
            "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
            json={
                "app_id": os.getenv("LARK_APP_ID"),
                "app_secret": os.getenv("LARK_APP_SECRET"),
            }
        )
        data = resp.json()
        # 明确校验，失败抛异常让 Temporal 重试
        if "tenant_access_token" not in data:
            raise Exception(f"换取 Lark token 失败：{data}")
        return data["tenant_access_token"]

async def _send_webhook_message(content: str):
    """
    用 webhook 发文本消息到 invest-alert 群
    用于：报告发送 + 卡片更新失败时的降级通知
    """
    timestamp, sign = _build_lark_sign(os.getenv("LARK_SECRET", ""))
    payload = {
        "timestamp": timestamp,
        "sign": sign,
        "msg_type": "text",
        "content": {"text": content}
    }
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        r = await client.post(os.getenv("LARK_WEBHOOK_URL"), json=payload)
        return r


async def _update_lark_card(message_id: str, content: str):
    """
    更新 Lark 卡片内容（PATCH 原消息）
    辅助功能——失败不影响主流程，降级发 webhook 消息
    """
    if not message_id:
        return
    
    try:
        # 换取 tenant_access_token
        token = await _get_lark_token()
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
  
            # PATCH 更新原卡片
            r = await client.patch(
                f"https://open.larksuite.com/open-apis/im/v1/messages/{message_id}",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "msg_type": "interactive",
                    "content": json.dumps({
                        "elements": [{
                            "tag": "div",
                            "text": {
                                "content": content,
                                "tag": "lark_md"
                            }
                        }]
                    })
                }
            )
            activity.logger.info(f"卡片更新结果：{r.status_code}, {r.text}")

            # 非 2xx 降级发新消息
            if r.status_code not in (200, 201):
                activity.logger.warning(f"卡片更新返回非 2xx：{r.status_code}，降级发新消息")
                await _send_webhook_message(f"📋 审核状态更新：{content}")

    except Exception as e:
        # 卡片更新失败只记录，降级发新消息
        activity.logger.warning(f"卡片更新失败（非致命），降级发新消息：{e}")
        try:
            await _send_webhook_message(f"📋 审核状态更新：{content}")
        except Exception as e2:
            # 降级也失败，只记录，绝不影响主流程
            activity.logger.error(f"降级消息也失败：{e2}")


# ============================================================
# Activity 1：调 LangGraph Runtime 跑投研分析
# ============================================================
@activity.defn                                                          # Temporal 装饰器：标记这是一个 Activity（可被持久化、重试）
async def run_agent(question: str, routing_mode: str = "auto") -> str:  # 入参：问题 + 路由模式；默认 auto（向后兼容，旧调用不传也能跑）
    """
    调用 LangGraph Runtime，返回分析结果。
    routing_mode 显式决定走哪条线（调用方声明，不靠内容猜）。
    """
    endpoint = {                                       # 路由模式 → endpoint 映射表
        "auto": "/invest",                             # auto：交给 Supervisor 自动分流（交互场景）
        "fixed": "/analyze",                           # fixed：强制固定线（定时日报，要确定性可复现）
        "react": "/analyze_react",                     # react：强制 ReAct 线
    }.get(routing_mode, "/invest")                     # 按模式取 endpoint；未知模式兜底走 /invest

    activity.logger.info(f"run_agent 开始（mode={routing_mode} → {endpoint}）：{question}")  # 记录模式+endpoint，Temporal 日志可审计
    # trust_env=False：绕过 Shadowrocket 代理，确保本地直连
    async with httpx.AsyncClient(timeout=360, trust_env=False) as client:  # 异步 HTTP 客户端；with 自动关连接
        try:                                           # 包裹请求，任何失败都抛出让 Temporal 按策略重试
            r = await client.post(                     # 发 POST 请求
                f"http://localhost:8100{endpoint}",    # 拼完整 URL（8100 是 api.py 端口）
                json={"question": question}            # 请求体：用户问题
            )
            r.raise_for_status()                       # 非 2xx 直接抛 HTTPStatusError（明确触发重试，不靠 KeyError 间接抛）
            result = r.json()                          # 解析响应 JSON（{analysis, thread_id}）
            activity.logger.info(f"run_agent 完成，thread_id={result.get('thread_id')}")  # thread_id 前缀能看出实际走了哪条线
            return result["analysis"]                  # 返回分析文字给 Workflow
        except Exception as e:                         # 捕获所有异常（网络/超时/非2xx/解析失败）
            activity.logger.error(f"HTTP 请求异常：{type(e).__name__}: {e}")  # 记录异常类型和详情
            raise                                      # 重新抛出，让 Temporal 按 RetryPolicy 重试（最多 3 次）

# ============================================================
# Activity 2：发审核通知（Lark 交互卡片）
# ============================================================
@activity.defn
async def human_review(analysis: str) -> str:
    """
    发 Lark 交互卡片通知审核人
    使用自建应用 API 发送（不是 webhook）
    这样卡片按钮点击才能触发回调
    """
    workflow_id = activity.info().workflow_id
    activity.logger.info(f"human_review 开始，workflow_id：{workflow_id}")

    # --------------------------------------------------------
    # Step 1：用 App ID + App Secret 换取 tenant_access_token
    # token 有效期 2 小时，生产环境应缓存复用
    # --------------------------------------------------------

    token = await _get_lark_token()
    # --------------------------------------------------------
    # Step 2：构建卡片消息（按钮用 value，触发回调）
    # --------------------------------------------------------
    card_message = {
        "elements": [
            {
                "tag": "div",
                "text": {
                    "content": (
                        f"**📊 投研报告待审核**\n\n"
                        f"**Workflow ID**：`{workflow_id}`\n\n"
                        f"**报告摘要**：\n{analysis[:300]}...\n\n"
                        f"请审核以上投研报告，点击下方按钮完成审核："
                    ),
                    "tag": "lark_md"
                }
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"content": "✅ 通过", "tag": "plain_text"},
                        "type": "primary",
                        # value 里的内容会在回调 body 的 action.value 里返回
                        "value": {
                            "workflow_id": workflow_id,
                            "decision": "approve"
                        }
                    },
                    {
                        "tag": "button",
                        "text": {"content": "❌ 拒绝", "tag": "plain_text"},
                        "type": "danger",
                        "value": {
                            "workflow_id": workflow_id,
                            "decision": "reject"
                        }
                    }
                ]
            }
        ]
    }

    # --------------------------------------------------------
    # Step 3：用 Lark API 发消息给指定审核人
    # 用 open_id 指定接收人，不是 webhook 固定群
    # --------------------------------------------------------
    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            r = await client.post(
                "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=open_id",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "receive_id": os.getenv("LARK_REVIEWER_OPEN_ID"),
                    "msg_type": "interactive",
                    "content": json.dumps({"elements": card_message["elements"]})
                }
            )
            activity.logger.info(f"Lark 消息发送结果：{r.status_code}, {r.text}")
            print(f"📨 Lark 审核通知已发送，workflow_id：{workflow_id}")

    except Exception as e:
        activity.logger.warning(f"Lark 通知发送失败（非致命）：{e}")
        print(f"⚠️ Lark 通知发送失败")
        print(f"手动发送：python -m temporal_workflow.send_signal {workflow_id} approve")

    return "notified"

# ============================================================
# Activity 3：交付报告（精确一次，不会重复发送）
# ============================================================

@activity.defn
async def deliver_report(analysis: str, approval: str, message_id: str = "") -> str:
    """
    审核通过 → 发报告到 invest-alert 群（webhook + 签名）
    审核拒绝 → 终止，不发送
    完成后更新原审核卡片状态（如果有 message_id）
    被 @activity.defn 装饰后，Temporal 保证精确一次执行——
    就算进程崩了重启，这个函数不会被重复调用
    """
    activity.logger.info(f"deliver_report 开始，审核结果：{approval}, message_id：{message_id}")

    if approval != "approve":
        print("❌ 报告审核未通过，已终止")
        if message_id:
            await _update_lark_card(message_id, "❌ 审核未通过，报告已终止")
        return "rejected"

    # 生成 webhook 签名
    timestamp, sign = _build_lark_sign(os.getenv("LARK_SECRET", ""))

    # 发报告到 invest-alert 群
    payload = {
        "timestamp": timestamp,
        "sign": sign,
        "msg_type": "text",
        "content": {
            "text": f"📊 投研报告（已审核通过）\n\n{analysis}"
        }
    }

    try:
        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            r = await client.post(
                os.getenv("LARK_WEBHOOK_URL"),
                json=payload
            )
            activity.logger.info(f"报告发送结果：{r.status_code}, {r.text}")

            if r.status_code not in (200, 201):
                raise Exception(f"webhook 返回非 2xx：{r.status_code}, {r.text}")
            print(f"📊 投研报告已发送到 invest-alert 群")

    except Exception as e:
        activity.logger.error(f"报告发送失败：{e}")
        if message_id:
            await _update_lark_card(message_id, f"❌ 报告发送失败：{str(e)[:100]}")
        raise
    

    # 报告发送成功后，单独更新卡片（辅助功能）
    now = time.strftime("%Y-%m-%d %H:%M", time.localtime())
    await _update_lark_card(
        message_id,
        f"✅ 审核通过，报告已发送到 invest-alert 群\n\n发送时间：{now}"
    )
    return "delivered"


# ============================================================
# Activity 4：Workflow 失败告警（兜底）
# ============================================================
@activity.defn
async def send_failure_alert(workflow_id: str, error: str) -> str:
    """
    Workflow 最终失败时发告警给审核员
    让审核员知道流程失败，需要人工处理
    这是兜底 Activity，本身也可能失败——失败只记录，不再重试
    """
    activity.logger.error(f"Workflow 失败告警：{workflow_id}, 原因：{error}")

    try:
        # 换取 token
        token = await _get_lark_token()

        # 发消息给审核员
        now = time.strftime("%Y-%m-%d %H:%M", time.localtime())
        message = (
            f"🚨 投研 Workflow 失败告警\n\n"
            f"**Workflow ID**：`{workflow_id}`\n\n"
            f"**失败时间**：{now}\n\n"
            f"**失败原因**：{error[:200]}\n\n"
            f"**处理建议**：\n"
            f"1. 查看 Temporal UI：http://localhost:8080\n"
            f"2. 确认报告是否已发送\n"
            f"3. 如需重发 Signal：\n"
            f"   `python -m temporal_workflow.send_signal {workflow_id} approve`"
        )

        async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
            r = await client.post(
                "https://open.larksuite.com/open-apis/im/v1/messages?receive_id_type=open_id",
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "receive_id": os.getenv("LARK_REVIEWER_OPEN_ID"),
                    "msg_type": "text",
                    "content": json.dumps({"text": message})
                }
            )
            activity.logger.info(f"失败告警发送结果：{r.status_code}")
            print(f"🚨 失败告警已发送给审核员")

    except Exception as e:
        # 告警本身失败，只记录，不再抛异常
        activity.logger.error(f"失败告警发送失败：{e}")

    return "alerted"

