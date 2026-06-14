"""
tools 调试脚本：逐个调用四个 @tool，打印结果。
在项目根目录运行：python test_tools.py
"""
import json
from src.tools import (
    get_stock_price,
    get_finance_data,
    get_sentiment,
    get_product_cycle,
)


def show(name: str, result):
    print(f"\n{'='*50}\n  {name}\n{'='*50}")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if isinstance(result, dict) and "error" in result:
        print(f"  ⚠️  返回 error：{result['error']}")


if __name__ == "__main__":
    show("get_stock_price（默认）", get_stock_price.invoke({}))
    show("get_finance_data（最近4期）", get_finance_data.invoke({"periods": 4}))
    show("get_sentiment（keyword=popmart）", get_sentiment.invoke({"keyword": "popmart"}))
    show("get_product_cycle（category=ecommerce）", get_product_cycle.invoke({"category": "ecommerce"}))

    print(f"\n{'='*50}\n  四个工具调用完毕\n{'='*50}")
