from src.state import InvestState
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from src.alert import send_alert


base_url = "https://api.deepseek.com"
model = "deepseek-chat"
load_dotenv()
llm = ChatOpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
    model="deepseek-chat",
    temperature=0,
)

higher = 150
lower = 100
# 这里定义了几个函数，分别用于获取股票价格、财务数据、社区情绪以及进行分析。这些函数会被节点调用来更新状态中的相应字段。
def fetch_price(state: InvestState) -> dict:
    import requests
    print('fetch_price  called ')
    r = requests.get("http://localhost:8000/api/external/get_stock_price")
    return {"stock_data": r.json()}


def fetch_finance(state: InvestState) -> dict:
    print('fetch_finance  called ')
    import requests
    r = requests.get("http://localhost:8000/api/external/get_financials")
    return {"financials": r.json()}

def fetch_sentiment(state: InvestState) -> dict:
    print('fetch_sentiment  called ')
    # raise Exception("模拟fetch_sentiment失败")  # 模拟这个节点偶尔会失败，测试断点续跑功能
    import requests
    r = requests.get("http://localhost:8000/api/external/get_community_sentiment")
    return {"sentiment": r.json()}

def analyze(state: InvestState) -> dict:
    print('analyze  called ')
    retry_count = state.get("retry_count", 0)
    price = state["stock_data"]["data"][-1]["close"]
    revenue = state["financials"]["data"]["reports"][0]["revenue_yoy"]
    #margin 为 net_profit_yoy有值
    profit = state["financials"]["data"]["reports"][0]["net_profit_yoy"]

    sentiment_data = state.get("sentiment", {}).get("data", {})
    latest_date = sentiment_data[0]["date"]

    from collections import defaultdict
    source_avg = defaultdict(list)
    for item in sentiment_data:
        if item["date"] == latest_date:
            source_avg[item["source"]].append(item["index_value"])

    sentiment_summary = ", ".join([
        f"{source} 热度均值 {sum(vals)/len(vals):.1f}"
        for source, vals in source_avg.items()
    ])

    print(f"舆情摘要：{sentiment_summary}")

    # 读用户原始问题
    question = state["messages"][0].content
    
    # 拼 prompt
    prompt = f"""你是一位专业的股票分析师，请根据以下数据回答用户问题。

    用户问题：{question}

    最新行情：
    - 当前价：{price} HKD

    财务数据（最新一期）：
    - 收入同比：+{revenue}%
    - 净利润同比：+{profit}%

    社区舆情（{latest_date}）：
    - 社区情绪: {sentiment_summary}

    要求：
    1. 给出简洁的投研分析，100字以内
    2. 如果数据明显异常（价格为0或负数、价格高于{higher}或者低于{lower}、财务数据缺失），必须只输出：DATA_INVALID
    3. 不要编造任何数字，只基于上面提供的数据分析"""
    
    response = llm.invoke(prompt)
    return {"analysis": response.content,
            "retry_count": retry_count + 1}


def should_retry(state: InvestState) -> str:
    if state.get("retry_count", 0) >= 2:
        print("已达重试上限，结束")
        send_alert(
            thread_id=state.get("thread_id", "unknown"),
            reason=f"重试超限，最终分析结果：{state.get('analysis', '')[:50]}"
        )
        return "end"
    
    # 数据层检查
    price = state["stock_data"]["data"][-1]["close"]
    if price <= 0:
        print(f"价格异常({price})，触发重试")
        return "retry"
    
    revenue = state["financials"]["data"]["reports"][0]["revenue_yoy"]
    profit = state["financials"]["data"]["reports"][0]["net_profit_yoy"]
    if revenue is None or profit is None:
        print("财务数据缺失，触发重试")
        return "retry"
    
    # LLM 输出层检查（用约定的固定标记）
    if "DATA_INVALID" in state["analysis"]:
        print("LLM 判断数据异常，触发重试")
        return "retry"
    
    print("数据正常，结束")
    return "end"