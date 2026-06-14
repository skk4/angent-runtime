"""
investment data tools —— 封装 popmart 数据服务的只读查询，供 ReAct Agent 调用。

对接 popmart 的 /api/external/* 聚合接口（专为外部消费者设计的标准化数据服务）。

设计原则：
- 只读：对应 popmart 的 read/get 接口，无副作用（采集由 popmart 自己的 collect 任务跑）
- 解耦：通过 HTTP 调 popmart FastAPI，agent-runtime 不依赖 akshare/tushare 等采集重依赖
- 精简：把原始大数组压成结论性摘要，省 token 且让 LLM 抓住重点
- 容错：调用失败返回 error dict，绝不抛异常（不炸掉 ReAct 循环）

依赖：pip install httpx langchain-core
环境变量：POPMART_API_BASE（默认 http://localhost:8000）
"""

import os
from datetime import datetime, date
from typing import Optional

import httpx
from langchain_core.tools import tool

POPMART_API_BASE = os.getenv("POPMART_API_BASE", "http://localhost:8000")
DEFAULT_SYMBOL = "09992"          # 泡泡玛特港股代码
HTTP_TIMEOUT = 10.0


def _get(path: str, params: Optional[dict] = None):
    """
    统一的 GET 调用：成功返回 {"data": ...}，失败返回 {"error": "..."}。

    适配 /api/external/* 的返回结构 {"status": "success", "count", "data"}，
    其中 data 可能是 list（行情/舆情/周期）或 dict（财务的 reports+indicators）。

    trust_env=False 绕过本地 Shadowrocket 代理（tunnel 模式会拦截出站 TCP）。
    任何失败都转成 error dict，绝不抛异常 —— 避免炸掉 Agent 的 ReAct 循环。
    """
    clean = {k: v for k, v in (params or {}).items() if v is not None}
    try:
        with httpx.Client(trust_env=False, timeout=HTTP_TIMEOUT) as client:
            resp = client.get(f"{POPMART_API_BASE}{path}", params=clean)
        if resp.status_code != 200:
            # external.py 失败时抛 HTTPException(500, detail=...)
            try:
                detail = resp.json().get("detail", "")
            except Exception:
                detail = resp.text[:200]
            return {"error": f"popmart API {path} HTTP {resp.status_code}: {detail}"}
        body = resp.json()
        if body.get("status") != "success":
            return {"error": f"popmart API {path} 返回 status={body.get('status')}"}
        return {"data": body.get("data")}
    except httpx.RequestError as e:
        return {"error": f"无法连接 popmart 数据服务：{e}"}
    except Exception as e:
        return {"error": f"调用 popmart API 异常：{e}"}


# ────────────────────────────────────────────────────────────
# 工具 1：股价行情  ->  GET /api/external/get_stock_price
# ────────────────────────────────────────────────────────────
@tool
def get_stock_price(start_date: Optional[str] = None,
                    end_date: Optional[str] = None) -> dict:
    """查询泡泡玛特(09992.HK)的历史股价行情摘要。

    当用户询问股价、涨跌、价格走势、近期股市表现时调用此工具。
    可指定日期范围；不指定则取最近一段时间的数据。

    Args:
        start_date: 起始日期，格式 YYYY-MM-DD，可选
        end_date:   结束日期，格式 YYYY-MM-DD，可选

    Returns:
        含最新收盘价、区间涨跌幅、区间最高/最低、交易天数的摘要；失败返回 {"error": ...}
    """
    r = _get("/api/external/get_stock_price", {
        "stock_code": DEFAULT_SYMBOL,
        "start_date": start_date,
        "end_date": end_date,
        "limit": 365,
    })
    if "error" in r:
        return r

    rows = [x for x in (r["data"] or []) if x.get("close") is not None]
    if not rows:
        return {"error": "无行情数据"}

    rows.sort(key=lambda x: x["date"])
    latest, earliest = rows[-1], rows[0]
    closes = [x["close"] for x in rows]
    change_pct = (latest["close"] - earliest["close"]) / earliest["close"] * 100

    return {
        "symbol": "09992.HK",
        "latest_date": latest["date"],
        "latest_close": round(latest["close"], 2),
        "period_start": earliest["date"],
        "period_change_pct": round(change_pct, 2),
        "period_high": round(max(closes), 2),
        "period_low": round(min(closes), 2),
        "trading_days": len(rows),
    }


# ────────────────────────────────────────────────────────────
# 工具 2：财务数据  ->  GET /api/external/get_financials
# ────────────────────────────────────────────────────────────
@tool
def get_finance_data(periods: int = 4) -> dict:
    """查询泡泡玛特(09992.HK)的财务报表与关键指标。

    当用户询问营收、利润、增长、毛利率、净利率、ROE 等财务表现时调用此工具。

    Args:
        periods: 返回最近几期的财务报表，默认 4 期

    Returns:
        含最近 N 期报表（营收/净利及同比、毛利率、净利率、ROE）与
        最新一期关键指标的摘要；失败返回 {"error": ...}
    """
    # external 的 get_financials 用同一个 limit 控制 reports 和 indicators，
    # 传 20 保证 indicators 取全最新一期，reports 在客户端截取前 periods 期。
    r = _get("/api/external/get_financials", {"stock_code": DEFAULT_SYMBOL, "limit": 20})
    if "error" in r:
        return r

    data = r["data"] or {}
    reports_raw = data.get("reports", [])[:periods]
    indicators_raw = data.get("indicators", [])

    reports = [{
        "report_date": x.get("report_date"),
        "report_type": x.get("report_type"),
        "revenue": x.get("revenue"),
        "revenue_yoy": x.get("revenue_yoy"),
        "net_profit": x.get("net_profit"),
        "net_profit_yoy": x.get("net_profit_yoy"),
        "gross_margin": x.get("gross_margin"),
        "net_margin": x.get("net_margin"),
        "roe": x.get("roe"),
    } for x in reports_raw]

    # 指标是 EAV 长格式（metric/value/period/unit），取最新一期转成 {指标: 值}
    latest_indicators = {}
    if indicators_raw:
        latest_period = max((i.get("period") or "") for i in indicators_raw)
        for i in indicators_raw:
            if i.get("period") == latest_period and i.get("value") is not None:
                latest_indicators[i["metric"]] = f'{i["value"]}{i.get("unit") or ""}'

    return {
        "symbol": "09992.HK",
        "reports": reports,
        "latest_indicators": latest_indicators,
    }


# ────────────────────────────────────────────────────────────
# 工具 3：市场热度 / 舆情  ->  GET /api/external/get_community_sentiment
# ────────────────────────────────────────────────────────────
@tool
def get_sentiment(keyword: Optional[str] = None,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None) -> dict:
    """查询泡泡玛特的市场热度 / 舆情数据摘要。

    当用户询问市场情绪、搜索热度、社媒讨论度、关注度变化时调用此工具。

    Args:
        keyword:    关键词筛选（如 popmart、labubu），可选
        start_date: 起始日期 YYYY-MM-DD，可选
        end_date:   结束日期 YYYY-MM-DD，可选

    Returns:
        含各来源最新热度值、近期均值与趋势、记录数、日期范围的摘要；失败返回 {"error": ...}
    """
    r = _get("/api/external/get_community_sentiment", {
        "keyword": keyword,
        "start_date": start_date,
        "end_date": end_date,
        "limit": 5000,
    })
    if "error" in r:
        return r

    rows = r["data"] or []
    if not rows:
        return {"error": "无舆情数据"}

    rows.sort(key=lambda x: x["date"])

    by_source: dict = {}
    for x in rows:
        by_source.setdefault(x.get("source", "unknown"), []).append(x)

    sources = {}
    for src, items in by_source.items():
        vals = [i["index_value"] for i in items if i.get("index_value") is not None]
        if not vals:
            continue
        recent, baseline = items[-1]["index_value"], items[0]["index_value"]
        trend = "上升" if recent > baseline else "下降" if recent < baseline else "持平"
        sources[src] = {
            "latest": round(recent, 1),
            "period_avg": round(sum(vals) / len(vals), 1),
            "trend": trend,
        }

    return {
        "total_records": len(rows),
        "date_range": [rows[0]["date"], rows[-1]["date"]],
        "by_source": sources,
    }


# ────────────────────────────────────────────────────────────
# 工具 4：产品 / 销售周期  ->  GET /api/external/get_cycles
# ────────────────────────────────────────────────────────────
def _brief(c: dict) -> dict:
    """提取周期事件的关键字段。"""
    return {
        "name": c["name"],
        "category": c["category"],
        "impact_level": c.get("impact_level", 3),
        "description": c.get("description", ""),
    }


def _resolve_window(c: dict, year: int):
    """把 start/end 解析成基准年的 date 区间，兼容 MM-DD / YYYY-MM-DD 与跨年。"""
    def parse(s: str, yr: int) -> date:
        p = s.split("-")
        if len(p) == 3:                       # YYYY-MM-DD
            return date(int(p[0]), int(p[1]), int(p[2]))
        return date(yr, int(p[0]), int(p[1]))  # MM-DD -> 套用基准年

    start_raw = c.get("start_date")
    if not start_raw:
        return None
    start = parse(start_raw, year)
    end = parse(c.get("end_date") or start_raw, year)
    if end < start:                           # 跨年（如 12-28 ~ 01-05）
        end = date(end.year + 1, end.month, end.day)
    return start, end


@tool
def get_product_cycle(category: Optional[str] = None,
                      as_of_date: Optional[str] = None) -> dict:
    """查询泡泡玛特相关的周期性事件（电商大促、年报窗口、季节性节点）。

    当用户询问当前处于什么销售周期、临近哪些大促或财报窗口、
    某段时间有哪些影响业绩的周期节点时调用此工具。

    Args:
        category:   类别筛选 ecommerce|earnings|seasonal|macro，可选
        as_of_date: 基准日期 YYYY-MM-DD，判断"当前/临近"以此为准，默认今天

    Returns:
        含"正在进行(active)"与"未来30天内临近(upcoming)"两类周期事件；失败返回 {"error": ...}
    """
    r = _get("/api/external/get_cycles", {"category": category})
    if "error" in r:
        return r

    cycles = r["data"] or []
    base = (datetime.strptime(as_of_date, "%Y-%m-%d").date()
            if as_of_date else date.today())

    active, upcoming = [], []
    for c in cycles:
        window = _resolve_window(c, base.year)
        if not window:
            continue
        start, end = window
        if start <= base <= end:
            active.append({**_brief(c), "start": str(start), "end": str(end)})
        elif 0 < (start - base).days <= 30:
            upcoming.append({**_brief(c), "start": str(start),
                             "days_until": (start - base).days})

    active.sort(key=lambda x: x["impact_level"], reverse=True)
    upcoming.sort(key=lambda x: x["days_until"])

    return {"as_of": str(base), "active": active, "upcoming": upcoming}


# ReAct Agent 注册用：from tools import TOOLS
TOOLS = [get_stock_price, get_finance_data, get_sentiment, get_product_cycle]
