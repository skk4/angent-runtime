"""
数据库 Migration 脚本
建库 + 建表 + 插入初始数据 + 验证连接，幂等执行（重复运行不报错）

用法：
    python scripts/migrate.py

包含：
    1. PostgreSQL：
       - 自动创建 agent_platform 数据库（独立于 ceap 业务库）
       - 建 question_types / question_rules 表
       - 插入初始数据
    2. ClickHouse：classification_logs 表
    3. Redis：db=1 连通性验证（与 ceap/Dify 的 db=0 隔离）

所有组件失败均退出——Migration 是上线前的初始化脚本，
必须确认所有组件就绪才能继续。
"""

import os
import sys
import psycopg2
import redis
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# 连接配置（从 .env 读取）
# ============================================================

# 管理员连接（用于建库，连 postgres 默认库）
PG_ADMIN_DSN = os.getenv(
    "PG_ADMIN_DSN",
    "postgresql://ceap:ceap_dev@localhost:5432/postgres"
)

# 新建的独立数据库名（独立于 ceap 业务库）
PG_DB_NAME = os.getenv("PG_DB_NAME", "agent_platform")

# 新库连接串（建表和插数据用）
PG_DSN = os.getenv(
    "PG_DSN",
    f"postgresql://ceap:ceap_dev@localhost:5432/{PG_DB_NAME}"
)

# Redis db=1（与 ceap/Dify 的 db=0 完全隔离）
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")

# ClickHouse（复用 langfuse-clickhouse）
CH_HOST     = os.getenv("CH_HOST", "localhost")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")


# ============================================================
# 1. 创建独立数据库
# ============================================================
def create_database():
    """
    在 ceap-postgres 里创建独立的 agent_platform 数据库
    不污染 ceap 业务库，职责分离
    幂等：数据库已存在则跳过
    """
    print(f">>> 检查并创建数据库 {PG_DB_NAME}...")

    # 必须连 postgres 默认库才能建新库
    conn = psycopg2.connect(PG_ADMIN_DSN)
    conn.autocommit = True  # CREATE DATABASE 不能在事务块里
    cur = conn.cursor()

    cur.execute(
        "SELECT 1 FROM pg_database WHERE datname = %s",
        (PG_DB_NAME,)
    )

    if not cur.fetchone():
        cur.execute(f'CREATE DATABASE "{PG_DB_NAME}"')
        print(f"    ✅ 数据库 {PG_DB_NAME} 创建完成")
    else:
        print(f"    ✅ 数据库 {PG_DB_NAME} 已存在，跳过")

    cur.close()
    conn.close()
    print(">>> 数据库就绪 ✅\n")


# ============================================================
# 2. PostgreSQL 建表 + 插初始数据
# ============================================================
def migrate_postgres():
    """
    在 agent_platform 库里建表并插入初始数据
    ON CONFLICT DO NOTHING 保证幂等
    """
    print(">>> 开始 PostgreSQL Migration...")
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor()

    # --------------------------------------------------------
    # 建表：question_types（问题类型定义）
    # 运营人员维护，新增类型只需插一条记录，不改代码
    # --------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS question_types (
            type_id     VARCHAR PRIMARY KEY,        -- 类型唯一标识，如 "buy_advice"
            name        VARCHAR NOT NULL,            -- 中文名，如 "买入建议"
            description VARCHAR,                     -- 类型说明
            examples    TEXT[],                      -- 示例问法，喂给 LLM prompt
            priority    INTEGER DEFAULT 0,           -- 规则匹配优先级，越大越先匹配
            enabled     BOOLEAN DEFAULT true,        -- 是否启用
            created_at  TIMESTAMP DEFAULT NOW()
        );
    """)
    print("    ✅ question_types 表就绪")

    # --------------------------------------------------------
    # 建表：question_rules（关键词规则）
    # 运营人员维护关键词，新增规则只需插一条记录，不改代码
    # --------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS question_rules (
            id          SERIAL PRIMARY KEY,
            type_id     VARCHAR REFERENCES question_types(type_id) ON DELETE CASCADE,
            keyword     VARCHAR NOT NULL,            -- 关键词，如 "值得买"
            weight      FLOAT DEFAULT 1.0,           -- 权重，多关键词命中时加权
            language    VARCHAR DEFAULT 'zh',        -- zh / en / all
            enabled     BOOLEAN DEFAULT true,
            created_at  TIMESTAMP DEFAULT NOW()
        );
    """)

    # 关键词唯一索引，防止重复插入
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_rules_type_keyword
        ON question_rules(type_id, keyword);
    """)

    # 查询加速索引（只索引启用的规则）
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_rules_enabled
        ON question_rules(type_id) WHERE enabled = true;
    """)
    print("    ✅ question_rules 表就绪")

    # --------------------------------------------------------
    # 插入初始问题类型（5类覆盖投研主要场景）
    # --------------------------------------------------------
    question_types = [
        (
            "buy_advice",
            "买入建议",
            "用户询问是否值得买入或卖出",
            ["值得买吗", "适合入手吗", "应该买吗", "可以买吗"],
            10,  # 最高优先级
        ),
        (
            "financial",
            "财务分析",
            "用户询问财报、收入、利润等财务数据",
            ["财报怎么样", "收入如何", "利润多少"],
            9,
        ),
        (
            "risk",
            "风险评估",
            "用户询问投资风险、需要注意的问题",
            ["有什么风险", "需要注意什么", "会不会跌"],
            8,
        ),
        (
            "price_analysis",
            "价格走势",
            "用户询问股价走势、行情分析",
            ["股价走势如何", "行情分析", "涨跌预测"],
            7,
        ),
        (
            "general",
            "通用分析",
            "其他综合性投研问题，LLM 兜底分类",
            ["综合分析", "整体评估"],
            0,  # 最低优先级，兜底用
        ),
    ]

    for type_id, name, description, examples, priority in question_types:
        cur.execute("""
            INSERT INTO question_types
                (type_id, name, description, examples, priority)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (type_id) DO NOTHING
        """, (type_id, name, description, examples, priority))

    print("    ✅ question_types 初始数据就绪（5条）")

    # --------------------------------------------------------
    # 插入初始关键词规则（中英文双语覆盖）
    # --------------------------------------------------------
    rules = [
        # ---- buy_advice 买入建议 ----
        ("buy_advice", "值得买",       1.0, "zh"),
        ("buy_advice", "适合入手",     1.0, "zh"),
        ("buy_advice", "应该买",       1.0, "zh"),
        ("buy_advice", "建议买",       1.0, "zh"),
        ("buy_advice", "可以买",       1.0, "zh"),
        ("buy_advice", "能买吗",       1.0, "zh"),
        ("buy_advice", "该不该买",     1.0, "zh"),
        ("buy_advice", "worth buying", 1.0, "en"),
        ("buy_advice", "should buy",   1.0, "en"),
        ("buy_advice", "recommend",    0.8, "en"),

        # ---- financial 财务分析 ----
        ("financial", "财报",      1.0, "zh"),
        ("financial", "收入",      1.0, "zh"),
        ("financial", "利润",      1.0, "zh"),
        ("financial", "营收",      1.0, "zh"),
        ("financial", "净利",      1.0, "zh"),
        ("financial", "毛利",      1.0, "zh"),
        ("financial", "业绩",      1.0, "zh"),
        ("financial", "revenue",   1.0, "en"),
        ("financial", "profit",    1.0, "en"),
        ("financial", "earnings",  1.0, "en"),
        ("financial", "financial", 1.0, "en"),

        # ---- risk 风险评估 ----
        ("risk", "风险",      1.0, "zh"),
        ("risk", "危险",      1.0, "zh"),
        ("risk", "注意",      0.8, "zh"),
        ("risk", "警惕",      1.0, "zh"),
        ("risk", "会不会跌",  1.0, "zh"),
        ("risk", "亏损",      1.0, "zh"),
        ("risk", "risk",     1.0, "en"),
        ("risk", "danger",   1.0, "en"),
        ("risk", "warning",  1.0, "en"),

        # ---- price_analysis 价格走势 ----
        ("price_analysis", "股价",  1.0, "zh"),
        ("price_analysis", "走势",  1.0, "zh"),
        ("price_analysis", "行情",  1.0, "zh"),
        ("price_analysis", "涨跌",  1.0, "zh"),
        ("price_analysis", "K线",   1.0, "zh"),
        ("price_analysis", "价格",  0.8, "zh"),
        ("price_analysis", "price", 1.0, "en"),
        ("price_analysis", "trend", 1.0, "en"),
        ("price_analysis", "chart", 1.0, "en"),
    ]

    for type_id, keyword, weight, language in rules:
        cur.execute("""
            INSERT INTO question_rules
                (type_id, keyword, weight, language)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (type_id, keyword) DO NOTHING
        """, (type_id, keyword, weight, language))

    print(f"    ✅ question_rules 初始数据就绪（{len(rules)}条）")

    conn.commit()
    cur.close()
    conn.close()
    print(">>> PostgreSQL Migration 完成 ✅\n")


# ============================================================
# 3. ClickHouse 建表
# ============================================================
def migrate_clickhouse():
    """
    建 ClickHouse 表：classification_logs（分类日志）
    用于分析：哪类问题最多、规则覆盖率、LLM 分类准确率趋势
    失败直接退出——Migration 是初始化脚本，必须确认所有组件就绪
    """
    print(">>> 开始 ClickHouse Migration...")
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
        )

        client.command("""
            CREATE TABLE IF NOT EXISTS classification_logs (
                id              UUID DEFAULT generateUUIDv4(),
                question        String,                 -- 用户原始问题
                question_hash   String,                 -- 问题 hash（关联 Redis 缓存 key）
                question_type   String,                 -- 分类结果，如 "buy_advice"
                method          String,                 -- 分类方法：cache / rule / llm
                confidence      Float32 DEFAULT 0.0,   -- LLM 分类置信度（0~1）
                client_id       String DEFAULT '',      -- 客户ID，用于多租户分析
                symbol          String DEFAULT '',      -- 标的代码，如 "09992HK"
                latency_ms      Int32 DEFAULT 0,        -- 分类耗时（毫秒）
                created_at      DateTime DEFAULT now()
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(created_at)    -- 按月分区，方便清理历史数据
            ORDER BY (created_at, question_type) -- 排序键，加速按类型+时间查询
            TTL created_at + INTERVAL 90 DAY     -- 90天自动过期，控制存储成本
        """)

        print("    ✅ classification_logs 表就绪")
        print(">>> ClickHouse Migration 完成 ✅\n")

    except Exception as e:
        print(f"❌ ClickHouse Migration 失败：{e}")
        sys.exit(1)


# ============================================================
# 4. Redis 连通性验证
# ============================================================
def check_redis():
    """
    验证 Redis db=1 连通性
    db=1 专用于 agent_platform，与 ceap/Dify 的 db=0 完全隔离
    失败直接退出——必须确认缓存层就绪
    """
    print(f">>> 检查 Redis 连接（{REDIS_URL}）...")
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()

        # 写读测试，验证读写权限正常
        # set 替代废弃的 setex，ex 参数指定过期秒数
        # r.setex("agent:migrate:health_check", 60, "ok")
        r.set("agent:migrate:health_check", "ok", ex=60)
        assert r.get("agent:migrate:health_check") == "ok"
        r.delete("agent:migrate:health_check")

        print("    ✅ Redis db=1 连接正常，读写验证通过")
        print(">>> Redis 检查完成 ✅\n")

    except Exception as e:
        print(f"❌ Redis 连接失败：{e}")
        sys.exit(1)


# ============================================================
# 主入口
# ============================================================
def main():
    print("=" * 50)
    print("开始数据库 Migration")
    print(f"目标库：{PG_DB_NAME}（独立于 ceap 业务库）")
    print(f"Redis：db=1（独立于 ceap/Dify 的 db=0）")
    print("=" * 50)
    print()

    # 所有步骤失败均退出
    # Migration 是上线前的初始化脚本，必须确认所有组件就绪

    try:
        create_database()
    except Exception as e:
        print(f"❌ 建库失败：{e}")
        sys.exit(1)

    try:
        migrate_postgres()
    except Exception as e:
        print(f"❌ PostgreSQL Migration 失败：{e}")
        sys.exit(1)

    migrate_clickhouse()  # 内部已处理退出
    check_redis()         # 内部已处理退出

    print("=" * 50)
    print("所有 Migration 完成 ✅")
    print("=" * 50)


if __name__ == "__main__":
    main()