"""
问题分类器
三层策略：Redis 缓存 → 规则引擎 → LLM 语义分类

设计目标：
- 99% 请求在 10ms 内返回（缓存或规则命中）
- 1% 长尾问题走 LLM（百毫秒级）
- 所有结果缓存 24 小时（同问题不重复分类）
- 分类日志异步写 ClickHouse（不阻塞主流程）
"""

import os
import json
import time
import hashlib
import asyncio
import logging
from typing import Optional
from dotenv import load_dotenv
import redis.asyncio as aioredis
import psycopg2
from langchain_openai import ChatOpenAI

load_dotenv()
logger = logging.getLogger(__name__)

# ============================================================
# Redis 连接（db=1，与 ceap/Dify 的 db=0 隔离）
# key 前缀统一用 agent: 命名空间
# ============================================================
redis_client = aioredis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/1"),
    encoding="utf-8",
    decode_responses=True,
)

# ============================================================
# LLM 客户端（兜底分类用，temperature=0 保证确定性）
# ============================================================
llm = ChatOpenAI(
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    base_url="https://api.deepseek.com",
    model="deepseek-chat",
    temperature=0,
)

# ============================================================
# PostgreSQL 连接（读问题类型和规则）
# ============================================================
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://ceap:ceap_dev@localhost:5432/agent_platform"
)

# ============================================================
# 内存缓存：问题类型和规则（启动时加载，减少数据库查询）
# 生产环境可加定时刷新（每5分钟），支持运营人员动态更新规则
# ============================================================
_types_cache: dict = {}   # {type_id: {name, description, examples, priority}}
_rules_cache: dict = {}   # {type_id: [{keyword, weight, language}]}
_cache_loaded = False


def _load_config_from_db():
    """
    从 agent_platform 数据库加载问题类型和规则到内存
    启动时调用一次，之后用内存缓存
    数据库不可用时降级到硬编码默认规则
    """
    global _types_cache, _rules_cache, _cache_loaded

    try:
        conn = psycopg2.connect(PG_DSN)
        cur = conn.cursor()

        # 加载问题类型（按优先级排序）
        cur.execute("""
            SELECT type_id, name, description, examples, priority
            FROM question_types
            WHERE enabled = true
            ORDER BY priority DESC
        """)
        for row in cur.fetchall():
            type_id, name, description, examples, priority = row
            _types_cache[type_id] = {
                "name": name,
                "description": description,
                "examples": examples or [],
                "priority": priority,
            }

        # 加载关键词规则
        cur.execute("""
            SELECT type_id, keyword, weight, language
            FROM question_rules
            WHERE enabled = true
            ORDER BY weight DESC
        """)
        for row in cur.fetchall():
            type_id, keyword, weight, language = row
            if type_id not in _rules_cache:
                _rules_cache[type_id] = []
            _rules_cache[type_id].append({
                "keyword": keyword,
                "weight": weight,
                "language": language,
            })

        cur.close()
        conn.close()
        _cache_loaded = True
        logger.info(
            f"配置加载完成：{len(_types_cache)} 个类型，"
            f"{sum(len(v) for v in _rules_cache.values())} 条规则"
        )

    except Exception as e:
        logger.warning(f"数据库配置加载失败，降级到默认规则：{e}")
        # 降级：使用硬编码默认规则
        _rules_cache = {
            "buy_advice": [
                {"keyword": "值得买", "weight": 1.0, "language": "zh"},
                {"keyword": "适合入手", "weight": 1.0, "language": "zh"},
                {"keyword": "应该买", "weight": 1.0, "language": "zh"},
                {"keyword": "worth buying", "weight": 1.0, "language": "en"},
            ],
            "financial": [
                {"keyword": "财报", "weight": 1.0, "language": "zh"},
                {"keyword": "收入", "weight": 1.0, "language": "zh"},
                {"keyword": "利润", "weight": 1.0, "language": "zh"},
                {"keyword": "revenue", "weight": 1.0, "language": "en"},
            ],
            "risk": [
                {"keyword": "风险", "weight": 1.0, "language": "zh"},
                {"keyword": "危险", "weight": 1.0, "language": "zh"},
                {"keyword": "risk", "weight": 1.0, "language": "en"},
            ],
            "price_analysis": [
                {"keyword": "股价", "weight": 1.0, "language": "zh"},
                {"keyword": "走势", "weight": 1.0, "language": "zh"},
                {"keyword": "price", "weight": 1.0, "language": "en"},
            ],
        }
        _cache_loaded = True


def _get_cache_key(question: str) -> str:
    """
    生成 Redis 缓存 key
    前缀 agent:qtype: 确保命名空间隔离
    用 hash 避免特殊字符问题
    """
    q_hash = hashlib.md5(question.encode()).hexdigest()[:16]
    return f"agent:qtype:{q_hash}"


def _rule_match(question: str) -> Optional[str]:
    """
    规则引擎：关键词加权匹配
    覆盖 80% 常见问法，微秒级，不消耗 LLM token
    从内存缓存读规则（启动时从数据库加载）
    """
    if not _cache_loaded:
        _load_config_from_db()

    # 按类型累计命中权重
    scores: dict[str, float] = {}
    for type_id, rules in _rules_cache.items():
        for rule in rules:
            if rule["keyword"] in question:
                scores[type_id] = scores.get(type_id, 0) + rule["weight"]

    if not scores:
        return None  # 没有匹配到任何规则

    # 返回得分最高的类型
    return max(scores, key=scores.get)


async def _llm_classify(question: str) -> tuple[str, float]:
    """
    LLM 语义分类：兜底长尾问法
    从数据库读类型定义动态生成 prompt，运营人员新增类型无需改代码
    返回 (类型, 置信度)
    """
    if not _cache_loaded:
        _load_config_from_db()

    valid_types = set(_types_cache.keys()) if _types_cache else {
        "buy_advice", "financial", "risk", "price_analysis", "general"
    }

    # 动态生成类型描述（从数据库加载的类型定义）
    type_lines = []
    for type_id, info in sorted(
        _types_cache.items(),
        key=lambda x: x[1].get("priority", 0),
        reverse=True
    ):
        examples = "、".join(info["examples"][:3]) if info["examples"] else ""
        type_lines.append(f"- {type_id}：{info['name']}（如：{examples}）")

    if not type_lines:
        type_lines = [
            "- buy_advice：买入建议",
            "- financial：财务分析",
            "- risk：风险评估",
            "- price_analysis：价格走势",
            "- general：其他问题",
        ]

    type_desc = "\n".join(type_lines)

    prompt = f"""你是一个投研问题分类器。
将用户问题分类成以下类型之一，只输出 JSON，不要其他内容。

类型列表：
{type_desc}

输出格式：{{"type": "类型名", "confidence": 0.95}}

用户问题：{question}"""

    try:
        response = llm.invoke(prompt)
        result = json.loads(response.content.strip())
        q_type = result.get("type", "general")
        confidence = float(result.get("confidence", 0.5))

        # 防御：类型不在有效列表里归为 general
        if q_type not in valid_types:
            logger.warning(f"LLM 返回了未知类型 {q_type}，归为 general")
            q_type = "general"
            confidence = 0.0

        return q_type, confidence

    except Exception as e:
        logger.error(f"LLM 分类失败：{e}")
        return "general", 0.0


async def _log_to_clickhouse(
    question: str,
    question_hash: str,
    q_type: str,
    method: str,
    confidence: float,
    client_id: str,
    symbol: str,
    latency_ms: int,
):
    """
    异步写分类日志到 ClickHouse（不阻塞主流程）
    用于分析：规则覆盖率、LLM 分类准确率、各类问题分布
    """
    try:
        import clickhouse_connect
        ch = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST", "localhost"),
            port=int(os.getenv("CH_PORT", "8123")),
            username=os.getenv("CH_USER", "default"),
            password=os.getenv("CH_PASSWORD", ""),
        )
        ch.insert(
            "classification_logs",
            [[
                question[:500],   # 截断过长问题
                question_hash,
                q_type,
                method,
                confidence,
                client_id,
                symbol,
                latency_ms,
            ]],
            column_names=[
                "question", "question_hash", "question_type",
                "method", "confidence", "client_id", "symbol", "latency_ms"
            ]
        )
    except Exception as e:
        # 日志写入失败不影响主流程，只记录警告
        logger.warning(f"ClickHouse 日志写入失败（非致命）：{e}")


async def classify_question(
    question: str,
    client_id: str = "default",
    symbol: str = "",
) -> str:
    """
    问题分类主入口——三层策略

    第一层：Redis 缓存（毫秒级）
        命中 → 直接返回，不消耗任何计算资源

    第二层：规则引擎（微秒级）
        关键词加权匹配，覆盖 80% 常见问法
        规则从数据库加载，运营人员可动态维护

    第三层：LLM 语义分类（百毫秒级）
        兜底长尾问法，理解语义而非关键词
        结果缓存 24 小时，同问题不重复调用

    Args:
        question:  用户原始问题
        client_id: 客户ID（用于日志分析）
        symbol:    标的代码（用于日志分析）

    Returns:
        question_type: 如 "buy_advice" / "financial" / "risk" 等
    """
    start_time = time.time()
    cache_key = _get_cache_key(question)
    question_hash = cache_key.split(":")[-1]

    # ① 查 Redis 缓存（db=1，agent 专用）
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            latency = int((time.time() - start_time) * 1000)
            logger.debug(f"缓存命中：{question[:30]} → {cached} ({latency}ms)")
            # 异步写日志，不阻塞返回
            asyncio.create_task(_log_to_clickhouse(
                question, question_hash, cached,
                "cache", 1.0, client_id, symbol, latency
            ))
            return cached
    except Exception as e:
        logger.warning(f"Redis 查询失败，降级到规则匹配：{e}")

    # ② 规则引擎匹配（从数据库加载的规则）
    rule_result = _rule_match(question)
    if rule_result:
        # 写 Redis 缓存（TTL 24小时）
        try:
            await redis_client.setex(cache_key, 86400, rule_result)
        except Exception:
            pass

        latency = int((time.time() - start_time) * 1000)
        logger.debug(f"规则命中：{question[:30]} → {rule_result} ({latency}ms)")
        asyncio.create_task(_log_to_clickhouse(
            question, question_hash, rule_result,
            "rule", 0.9, client_id, symbol, latency
        ))
        return rule_result

    # ③ LLM 语义分类（兜底）
    llm_result, confidence = await _llm_classify(question)

    # 写 Redis 缓存
    try:
        await redis_client.setex(cache_key, 86400, llm_result)
    except Exception:
        pass

    latency = int((time.time() - start_time) * 1000)
    logger.info(
        f"LLM 分类：{question[:30]} → {llm_result} "
        f"(置信度={confidence:.2f}, {latency}ms)"
    )
    asyncio.create_task(_log_to_clickhouse(
        question, question_hash, llm_result,
        "llm", confidence, client_id, symbol, latency
    ))
    return llm_result