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

# ============================================================
# Activity 1：调 LangGraph Runtime 跑投研分析
# ============================================================
@activity.defn
async def run_agent(question: str) -> str:
    """
    调用你的 LangGraph Runtime，返回分析结果。
    """
    activity.logger.info(f"run_agent 开始，问题：{question}")
    #trust_env=False：绕过 Shadowrocket 代理，确保本地直连
    async with httpx.AsyncClient(timeout=360, trust_env=False) as client:
        try:
            r = await client.post(
                "http://localhost:8100/analyze",
                json={"question": question}
            )
            return r.json()["analysis"]
        except Exception as e:
            activity.logger.error(f"HTTP 请求异常：{type(e).__name__}: {e}")
            raise


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
    async with httpx.AsyncClient(timeout=10, trust_env=False) as client:
        token_resp = await client.post(
            "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
            json={
                "app_id": os.getenv("LARK_APP_ID"),
                "app_secret": os.getenv("LARK_APP_SECRET"),
            }
        )
        token = token_resp.json()["tenant_access_token"]
        activity.logger.info("tenant_access_token 获取成功")

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
async def deliver_report(analysis: str, approval: str) -> str:
    """
    审核通过 → 发报告到 invest-alert 群（webhook + 签名）
    审核拒绝 → 终止，不发送
    被 @activity.defn 装饰后，Temporal 保证精确一次执行——
    就算进程崩了重启，这个函数不会被重复调用
    """
    activity.logger.info(f"deliver_report 开始，审核结果：{approval}")

    if approval != "approve":
        print("❌ 报告审核未通过，已终止")
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
            print(f"📊 投研报告已发送到 invest-alert 群")
    except Exception as e:
        activity.logger.error(f"报告发送失败：{e}")
        raise

    return "delivered"




