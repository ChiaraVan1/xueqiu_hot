#!/usr/bin/env python3
"""
雪球热股榜 每日抓取脚本
=====================================================
策略：从环境变量 XUEQIU_TOKEN 读取 xq_a_token Cookie
      → requests 调用雪球内部 API
      → 逐个查询 quote 接口补充成交额/市值/换手率
      → 调用 Claude API 批量判断行业
      → 结果写入带日期的 CSV + 追加至 master.csv

实测 API 结构（2026-06-24）：
  热榜 API：返回顺序即排名，rank_change = 排名位次变化
            无 amount/market_capital/turnover_rate，需 quote API 补充
  Quote API：批量查询返回全 null，必须单股逐个查
             行业/关注人数字段不存在，由 Claude 补充
  Claude：七牛云代理，一次调用批量判断所有股票行业

用法：
  XUEQIU_TOKEN=xxx CLAUDE_API_KEY=xxx python xueqiu_scraper.py
  python xueqiu_scraper.py --debug
=====================================================
"""

import argparse
import csv
import json
import os
import sys
import time
import schedule
import requests
from datetime import datetime
from pathlib import Path
from openai import OpenAI

# ─────────────────────────── 配置 ───────────────────────────
OUTPUT_DIR = Path("xueqiu_data")

MARKET_TYPES = {
    "全球": 10,
    "沪深": 12,
    "港股": 11,
    "美股": 13,
}

HOT_LIST_API = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
QUOTE_API    = "https://stock.xueqiu.com/v5/stock/quote.json"

TOP_N = 9  # 每个榜单取前 N 名（API 返回顺序即排名）

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

CSV_FIELDS = [
    "日期", "榜单", "排名", "排名变化",
    "股票代码", "股票名称", "行业",
    "当前价格", "涨跌幅(%)", "涨跌额",
    "成交额(亿)", "总市值(亿)", "换手率(%)",
]

# ─────────────────────────── Cookie 获取 ───────────────────────────

def get_xueqiu_cookies() -> dict:
    token = os.environ.get("XUEQIU_TOKEN", "").strip()
    if token:
        print(f"[1/4] 使用环境变量 Token（前8位）: {token[:8]}...")
        return {"xq_a_token": token, "xqat": token}

    print("[1/4] 未检测到 XUEQIU_TOKEN，尝试用 Playwright 获取（仅限本地）...")
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "本地运行需要安装 Playwright：pip install playwright && playwright install chromium\n"
            "或设置环境变量 XUEQIU_TOKEN"
        )

    stealth_js = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN','zh','en']});
        window.chrome = {runtime: {}};
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"], locale="zh-CN", timezone_id="Asia/Shanghai"
        )
        ctx.add_init_script(stealth_js)
        page = ctx.new_page()
        page.goto("https://xueqiu.com", wait_until="networkidle", timeout=30_000)
        page.wait_for_timeout(2_000)
        raw = ctx.cookies()
        browser.close()

    cookies = {c["name"]: c["value"] for c in raw}
    if not cookies.get("xq_a_token"):
        raise RuntimeError("Playwright 未能获取 xq_a_token，请手动设置 XUEQIU_TOKEN")
    print("   → Playwright Token 获取成功")
    return cookies


# ─────────────────────────── 热榜抓取 ───────────────────────────

def fetch_hot_list(session: requests.Session, market_name: str, type_id: int,
                   debug: bool = False) -> list[dict]:
    """
    获取单个榜单数据。
    实测：API 返回顺序即排名，rank_change = 排名位次变化（正=上升，负=下降）。
    成交额/市值/换手率 热榜 API 不返回，由 enrich_quote 补充。
    """
    params = {"size": 30, "_type": 10, "type": type_id}
    resp = session.get(HOT_LIST_API, params=params, timeout=15)
    resp.raise_for_status()

    items = resp.json().get("data", {}).get("items", [])

    if debug and items:
        print(f"\n[DEBUG] 热榜({market_name}) 第一条原始字段：")
        for k, v in items[0].items():
            print(f"   {k}: {v!r}")
        print()

    result = []
    for rank, item in enumerate(items[:TOP_N], start=1):
        result.append({
            "排名":       rank,
            "排名变化":   item.get("rank_change", 0),
            "股票代码":   item.get("symbol", ""),
            "股票名称":   item.get("name", ""),
            "行业":       "",   # 由 enrich_industry_ai 补充
            "当前价格":   item.get("current", ""),
            "涨跌幅(%)":  item.get("percent", ""),
            "涨跌额":     item.get("chg", ""),
            "成交额(亿)": "",   # 由 enrich_quote 补充
            "总市值(亿)": "",   # 由 enrich_quote 补充
            "换手率(%)":  "",   # 由 enrich_quote 补充
        })

    return result


# ─────────────────────────── Quote 补充 ───────────────────────────

def enrich_quote(session: requests.Session, rows: list[dict],
                 debug: bool = False) -> None:
    """
    逐个查询 quote 接口，补充成交额、总市值、换手率。
    实测：批量查询返回全 null，只能单股查询。
    """
    for i, row in enumerate(rows):
        sym = row["股票代码"]
        if not sym:
            continue
        try:
            resp = session.get(QUOTE_API,
                               params={"symbol": sym, "extend": "detail"},
                               timeout=15)
            resp.raise_for_status()
            q = resp.json().get("data", {}).get("quote") or {}

            if debug and i == 0:
                print(f"\n[DEBUG] Quote API({sym}) 关键字段：")
                for k in ["amount", "market_capital", "turnover_rate"]:
                    print(f"   {k}: {q.get(k)!r}")
                print()

            amt = q.get("amount") or 0
            mc  = q.get("market_capital") or 0
            tr  = q.get("turnover_rate", "")

            row["成交额(亿)"] = round(amt / 1e8, 2) if amt else ""
            row["总市值(亿)"] = round(mc  / 1e8, 2) if mc  else ""
            row["换手率(%)"]  = tr if tr != "" else ""

        except Exception as e:
            print(f"   ⚠ quote 查询失败 {sym}: {e}")

        time.sleep(0.3)


# ─────────────────────────── AI 行业判断 ───────────────────────────

def enrich_industry_ai(rows: list[dict], debug: bool = False) -> None:
    """
    一次 Claude API 调用，批量判断所有股票的行业。
    使用七牛云代理接口（与 stock_Valuation_bot 同一套）。
    Claude 对主流 A股/港股/美股 行业归属准确率很高；
    不确定时返回"其他"而非瞎猜。
    """
    api_key = os.environ.get("CLAUDE_API_KEY", "").strip()
    if not api_key:
        print("   ⚠ CLAUDE_API_KEY 未设置，跳过行业判断")
        return

    # 去重，避免重复查同一只股票
    seen: dict[str, str] = {}  # symbol -> industry
    unique = []
    for row in rows:
        sym = row["股票代码"]
        if sym and sym not in seen:
            seen[sym] = ""
            unique.append({"symbol": sym, "name": row["股票名称"]})

    if not unique:
        return

    stock_list = "\n".join(f'{s["symbol"]} {s["name"]}' for s in unique)
    prompt = f"""以下是一组股票代码和名称，请判断每只股票所属的行业（使用中文，简洁精准，如：半导体、白酒、新能源汽车、互联网、医药、银行、消费电子、石油、航运等）。

股票列表：
{stock_list}

要求：
1. 严格按 JSON 格式返回，key 为股票代码，value 为行业名称
2. 不确定的返回"其他"
3. 只返回 JSON，不要任何解释

示例格式：
{{"SH603986": "半导体", "SZ000858": "白酒"}}"""

    try:
        client = OpenAI(
            api_key=api_key,
            base_url="https://openai.qiniu.com/v1"
        )
        resp = client.chat.completions.create(
            model="claude-4.5-sonnet",
            max_tokens=500,
            temperature=0,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = resp.choices[0].message.content.strip()

        if debug:
            print(f"\n[DEBUG] Claude 行业判断原始返回：\n{raw}\n")

        # 清理可能的 markdown 代码块
        raw = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        industry_map: dict[str, str] = json.loads(raw)

        # 写回
        for row in rows:
            sym = row["股票代码"]
            if sym in industry_map:
                row["行业"] = industry_map[sym]

        print(f"   → Claude 行业判断完成（{len(industry_map)} 只股票）")

    except Exception as e:
        print(f"   ⚠ Claude 行业判断失败: {e}")


# ─────────────────────────── 主流程 ───────────────────────────

def scrape_and_save(debug: bool = False):
    today = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(exist_ok=True)

    print(f"\n{'='*55}")
    print(f"  雪球热股榜抓取  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*55}")

    cookies = get_xueqiu_cookies()

    session = requests.Session()
    session.headers.update(HEADERS)
    session.cookies.update(cookies)

    print(f"[2/4] 抓取四个榜单（每榜取前 {TOP_N} 名）...")
    all_rows: list[dict] = []
    first = True

    for market_name, type_id in MARKET_TYPES.items():
        try:
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
        print("❌ 未抓到任何数据，Token 可能已过期，请更新 XUEQIU_TOKEN")
        sys.exit(1)

    print(f"[3/4] 逐股查询 quote 补充成交额/市值/换手率（共 {len(all_rows)} 条）...")
    enrich_quote(session, all_rows, debug=debug)

    print("[4/4] Claude 批量判断行业...")
    enrich_industry_ai(all_rows, debug=debug)

    outfile = OUTPUT_DIR / f"xueqiu_hot_{today}.csv"
    with open(outfile, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

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
            f"  [{r['榜单']}] #{r['排名']}(变化:{r['排名变化']:+d}) "
            f"{r['股票名称']}({r['股票代码']})  "
            f"涨跌幅={r['涨跌幅(%)']}%  行业={r['行业'] or '—'}  "
            f"成交额={r['成交额(亿)']}亿"
        )


# ─────────────────────────── 定时模式 ───────────────────────────

def run_daemon(run_time: str = "15:10"):
    print(f"定时模式启动，每天 {run_time} 运行...")
    schedule.every().day.at(run_time).do(scrape_and_save)
    scrape_and_save()
    while True:
        schedule.run_pending()
        time.sleep(30)


# ─────────────────────────── 入口 ───────────────────────────

def main():
    parser = argparse.ArgumentParser(description="雪球热股榜每日抓取")
    parser.add_argument("--daemon", action="store_true", help="持续运行，每天定时抓取")
    parser.add_argument("--time", default="15:10", help="定时运行时间 HH:MM（默认 15:10）")
    parser.add_argument("--debug", action="store_true", help="打印 API 原始字段")
    args = parser.parse_args()

    if args.daemon:
        run_daemon(args.time)
    else:
        scrape_and_save(debug=args.debug)


if __name__ == "__main__":
    main()
