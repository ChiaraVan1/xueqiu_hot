#!/usr/bin/env python3
"""
雪球热股榜 每日抓取脚本
=====================================================
策略：从环境变量 XUEQIU_TOKEN 读取 xq_a_token Cookie
      → requests 调用雪球内部 API
      → 批量查询股票详情补充行业信息
      → 结果写入带日期的 CSV + 追加至 master.csv

Token 获取（一次性操作）：
  浏览器登录 xueqiu.com → F12 → Application → Cookies
  → 复制 xq_a_token 的值 → 存入 GitHub Secret: XUEQIU_TOKEN

用法：
  XUEQIU_TOKEN=xxx python xueqiu_scraper.py
  python xueqiu_scraper.py --debug   # 打印原始字段，用于排查空值
=====================================================
"""

import argparse
import csv
import os
import time
import schedule
import requests
from datetime import datetime
from pathlib import Path

# ─────────────────────────── 配置 ───────────────────────────
OUTPUT_DIR = Path("xueqiu_data")

# 四个榜单对应的 type 参数（雪球内部 API）
MARKET_TYPES = {
    "全球": 10,
    "沪深": 12,
    "港股": 11,
    "美股": 13,
}

HOT_LIST_API = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
QUOTE_API    = "https://stock.xueqiu.com/v5/stock/quote.json"

HOT_LIST_PARAMS = dict(size=30, _type=10)   # _type=10 固定值；type 按榜单变

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://xueqiu.com/",
    "Origin":  "https://xueqiu.com",
    "Accept":  "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# CSV 列顺序
CSV_FIELDS = [
    "日期", "榜单", "排名", "排名变化",
    "股票代码", "股票名称", "行业",
    "当前价格", "涨跌幅(%)", "涨跌额",
    "成交额(亿)", "总市值(亿)", "换手率(%)",
    "雪球关注人数",
]

# ─────────────────────────── Cookie 获取 ───────────────────────────

def get_xueqiu_cookies() -> dict:
    """
    访问雪球首页，从响应 Cookie 中自动获取 xq_a_token。
    雪球对所有访客（不含登录）都会下发这个 token，每次运行拿新的，不存在过期问题。
    如果请求被 block（GitHub Actions IP 被封），脚本会报错提示。
    """
    print("[1/3] 获取雪球访客 Token...")
    try:
        resp = requests.get(
            "https://xueqiu.com",
            headers=HEADERS,
            timeout=15,
            allow_redirects=True,
        )
        cookies = dict(resp.cookies)
        token = cookies.get("xq_a_token", "")
        if not token:
            raise RuntimeError("响应中未找到 xq_a_token，雪球可能封锁了当前 IP")
        print(f"   → Token 获取成功（前8位）: {token[:8]}...")
        return cookies
    except requests.exceptions.Timeout:
        raise RuntimeError("访问 xueqiu.com 超时，当前网络环境可能被封锁")


# ─────────────────────────── 热榜抓取 ───────────────────────────

def fetch_hot_list(session: requests.Session, market_name: str, type_id: int,
                   debug: bool = False) -> list[dict]:
    """调用内部 API 获取单个榜单数据"""
    params = {**HOT_LIST_PARAMS, "type": type_id}
    resp = session.get(HOT_LIST_API, params=params, timeout=15)
    resp.raise_for_status()

    data = resp.json()
    items = data.get("data", {}).get("items", [])

    if debug and items:
        print("\n[DEBUG] 热榜 API 第一条原始字段：")
        for k, v in items[0].items():
            print(f"   {k}: {v}")
        print()

    result = []
    for item in items:
        hot_rank      = item.get("hot_rank") or item.get("rank") or 0
        hot_rank_prev = item.get("hot_rank_last_time") or item.get("rank_last_time") or hot_rank
        rank_change   = hot_rank_prev - hot_rank   # 正数=上升，负数=下降

        amount_raw  = item.get("amount", 0) or 0
        mktcap_raw  = item.get("market_capital", 0) or 0

        result.append({
            "排名":       hot_rank,
            "排名变化":   rank_change if hot_rank_prev != hot_rank else 0,
            "股票代码":   item.get("symbol", ""),
            "股票名称":   item.get("name", ""),
            "行业":       "",    # 下一步补充
            "当前价格":   item.get("current", ""),
            "涨跌幅(%)":  item.get("percent", ""),
            "涨跌额":     item.get("chg", ""),
            "成交额(亿)": round(amount_raw / 1e8, 2) if amount_raw else "",
            "总市值(亿)": round(mktcap_raw / 1e8, 2) if mktcap_raw else "",
            "换手率(%)":  item.get("turnover_rate", ""),
            "雪球关注人数": item.get("followers_count", ""),
        })

    return result


# ─────────────────────────── 行业信息补充 ───────────────────────────

def enrich_industry(session: requests.Session, rows: list[dict],
                    debug: bool = False) -> None:
    """
    批量查股票详情，补充行业字段。
    雪球 quote 接口：/v5/stock/quote.json?symbol=SH601318,SZ000001&extend=detail
    行业字段候选：industry_name / classify_name / industry
    换手率候选（如热榜 API 没有）：turnover_rate
    """
    symbols = list({r["股票代码"] for r in rows if r["股票代码"]})
    if not symbols:
        return

    industry_map:  dict[str, str] = {}
    turnover_map:  dict[str, str] = {}   # 热榜没有换手率时的备用来源

    batch_size = 20
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i : i + batch_size]
        try:
            params = {"symbol": ",".join(batch), "extend": "detail"}
            resp = session.get(QUOTE_API, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {})

            if debug and i == 0:
                print("\n[DEBUG] Quote API 第一批原始结构 keys：", list(data.keys()))

            # 接口可能返回 {"items": [...]} 或 {"quote": {...}, "others": [...]}
            quotes = data.get("items") or []
            if not quotes:
                # 单股或结构不同时 fallback
                q = data.get("quote")
                if q:
                    quotes = [q]

            for q in quotes:
                if debug and not industry_map:
                    print("[DEBUG] Quote API 单条字段：")
                    for k, v in q.items():
                        print(f"   {k}: {v}")
                    print()

                sym = q.get("symbol", "")
                ind = (
                    q.get("industry_name")
                    or q.get("classify_name")
                    or q.get("industry")
                    or ""
                )
                tr = q.get("turnover_rate", "")
                if sym:
                    industry_map[sym] = ind
                    if tr:
                        turnover_map[sym] = tr

        except Exception as e:
            print(f"   ⚠ 行业查询批次失败（跳过）: {e}")

        time.sleep(0.4)

    for row in rows:
        sym = row["股票代码"]
        row["行业"] = industry_map.get(sym, "")
        # 如果热榜 API 没有换手率，从 quote 补充
        if not row.get("换手率(%)") and sym in turnover_map:
            row["换手率(%)"] = turnover_map[sym]


# ─────────────────────────── 主流程 ───────────────────────────

def scrape_and_save(debug: bool = False):
    today = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"\n{'='*55}")
    print(f"  雪球热股榜抓取  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    # Step 1: 拿 Cookie
    cookies = get_xueqiu_cookies()

    # Step 2: 建立带 Cookie 的 session
    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookies)

    print("[2/3] 抓取四个榜单...")
    all_rows: list[dict] = []
    first = True

    for market_name, type_id in MARKET_TYPES.items():
        try:
            # debug 只在第一个榜单打印原始字段，避免刷屏
            rows = fetch_hot_list(session, market_name, type_id, debug=debug and first)
            first = False
            for r in rows:
                r["日期"]  = today
                r["榜单"] = market_name
            all_rows.extend(rows)
            print(f"   → {market_name}: {len(rows)} 条")
        except Exception as e:
            print(f"   ✗ {market_name} 失败: {e}")
        time.sleep(0.6)

    if not all_rows:
        print("❌ 未抓到任何数据，请检查网络或 Cookie")
        return

    # Step 3: 补充行业信息（同时兜底换手率）
    print("[3/3] 补充行业信息...")
    enrich_industry(session, all_rows, debug=debug)

    # Step 4a: 每日独立文件（便于单日查看/备份）
    outfile = OUTPUT_DIR / f"xueqiu_hot_{today}.csv"
    with open(outfile, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    # Step 4b: 追加写入 master 文件（便于跨日分析霸榜/趋势）
    master = OUTPUT_DIR / "xueqiu_hot_master.csv"
    write_header = not master.exists()
    with open(master, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(all_rows)

    print(f"\n✅ 完成！共 {len(all_rows)} 条记录")
    print(f"   每日文件 → {outfile}")
    print(f"   历史汇总 → {master}")
    _print_preview(all_rows[:5])


def _print_preview(rows: list[dict]):
    print("\n--- 数据预览（前5条） ---")
    for r in rows:
        print(
            f"  [{r['榜单']}] #{r['排名']} {r['股票名称']}({r['股票代码']})  "
            f"涨跌幅={r['涨跌幅(%)']}%  行业={r['行业'] or '—'}"
        )


# ─────────────────────────── 定时模式 ───────────────────────────

def run_daemon(run_time: str = "09:35"):
    """
    每天指定时间运行一次。
    A股收盘时间为 15:00，建议在 15:05 后抓取确保数据稳定；
    如需盘中抓多次可修改 schedule 逻辑。
    默认 09:35 可抓到美股收盘后 + A股开盘前的快照。
    """
    print(f"定时模式启动，每天 {run_time} 运行...")
    schedule.every().day.at(run_time).do(scrape_and_save)

    # 启动时立即运行一次
    scrape_and_save()

    while True:
        schedule.run_pending()
        time.sleep(30)


# ─────────────────────────── 入口 ───────────────────────────

def main():
    parser = argparse.ArgumentParser(description="雪球热股榜每日抓取")
    parser.add_argument(
        "--daemon", action="store_true",
        help="持续运行，每天定时抓取"
    )
    parser.add_argument(
        "--time", default="15:10",
        help="定时运行时间，格式 HH:MM（默认 15:10，A股收盘后稳定）"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="打印 API 原始字段，用于确认成交额/换手率/行业等字段名"
    )
    args = parser.parse_args()

    if args.daemon:
        run_daemon(args.time)
    else:
        scrape_and_save(debug=args.debug)


if __name__ == "__main__":
    main()
