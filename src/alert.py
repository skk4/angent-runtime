import os
from dotenv import load_dotenv
from langfuse import Langfuse
import hashlib
import hmac
import base64
import time
import requests




load_dotenv()

langfuse = Langfuse(
    public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
    secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
    host=os.getenv("LANGFUSE_HOST"),
)


def send_lark_alert(thread_id: str, reason: str):
    import os
    webhook_url = os.getenv("LARK_WEBHOOK_URL")
    secret = os.getenv("LARK_SECRET")
    
    # 计算签名
    timestamp = str(int(time.time()))
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256
    ).digest()
    sign = base64.b64encode(hmac_code).decode("utf-8")
    
    # 发消息
    payload = {
        "timestamp": timestamp,
        "sign": sign,
        "msg_type": "text",
        "content": {
            "text": f"🚨 投研告警\nthread_id: {thread_id}\n原因: {reason}"
        }
    }
    
    r = requests.post(webhook_url, json=payload)
    print(f"Lark 告警状态：{r.status_code}, {r.json()}")

def send_alert(thread_id: str, reason: str):
    print(f"🚨 告警：{thread_id} - {reason}")
    
    with langfuse.start_as_current_observation(
        name="invest-alert",
        metadata={"thread_id": thread_id, "reason": reason}
    ) as span:
        span.update(
            input={"thread_id": thread_id},
            output={"reason": reason},
        )
        langfuse.score_current_trace(
            name="data_quality",
            value=0,
            data_type="NUMERIC",
            comment=f"重试超限：{reason}",
        )
    
    langfuse.flush()
    print(f"✅ 告警已记录到 Langfuse")
    send_lark_alert(thread_id, reason)
    print(f"✅ 告警已记录到 Lark")




