#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股巴菲特指标 · 实时数据服务（SQLite 版）

数据口径：
  A股总市值 = 上证指数 + 深证综指总市值（东方财富 f20），北交所单列
  证券化率 = 总市值 / GDP
  880001 等价值 = 总市值（万亿元）× 100

功能：
  · 交易时段每 30 秒抓取，非交易时段每 5 分钟
  · SQLite 落库：盘中快照、日线、预警记录
  · 估值组合（沪深300 / 中证500 的 PE、PB、分位）、ERP
  · 市场确认（成交额、涨停/跌停、前十大集中度、北交所占比）
  · 阈值预警与自定义配置
"""

import csv
import datetime
import io
import json
import os
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import indices_engine
import runtime_lock

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "buffett.db")
DATA_JS = os.path.join(BASE, "data.js")
HIST_JSON = os.path.join(BASE, "buffett_history.json")
CONFIG_JSON = os.path.join(BASE, "config.json")
STATE_JSON = os.path.join(BASE, "state.json")
BREADTH_HIST = os.path.join(BASE, "breadth_history.json")
REFRESH_LOCK = os.path.join(BASE, "refresh.lock")
INSTANCE_LOCK = os.path.join(BASE, "server.lock")
PAGE_NAME = "A股巴菲特指标仪表盘.html"
PORT = 8787
CST = datetime.timezone(datetime.timedelta(hours=8))

EM_HOSTS = [
    "https://1.push2.eastmoney.com",
    "https://2.push2.eastmoney.com",
    "https://3.push2.eastmoney.com",
    "https://push2delay.eastmoney.com",
    "https://push2.eastmoney.com",
]
CAP_PATH = ("/api/qt/ulist.np/get?fltt=2&secids=1.000001,0.399106,0.899050"
            "&fields=f12,f14,f2,f3,f20,f104,f105,f106,f124")
TOP10_PATH = ("/api/qt/clist/get?pn=1&pz=10&po=1&np=1&fltt=2&invt=2&fid=f20"
              "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23&fields=f12,f14,f20")
EM_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
              "Referer": "https://quote.eastmoney.com/"}
SINA_URL = "https://hq.sinajs.cn/list=s_sh000001,s_sz399106"
SINA_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"}
ZT_URL = ("https://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989"
          "&dpt=wz.ztzt&Pageindex=0&pagesize=1&sort=fbt:asc&date={date}")
DT_URL = ("https://push2ex.eastmoney.com/getTopicDTPool?ut=7eea3edcaed734bea9cbfc24409ed989"
          "&dpt=wz.ztzt&Pageindex=0&pagesize=1&sort=fund:asc&date={date}")
DANJUAN = {
    "hs300": ("https://danjuanfunds.com/djapi/index_eva/detail/SH000300", "沪深300"),
    "zz500": ("https://danjuanfunds.com/djapi/index_eva/detail/SH000905", "中证500"),
}
DANJUAN_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://danjuanfunds.com/"}
RF_SECID = "171.CN10Y"  # 东方财富：中国10年期国债收益率（收盘价为收益率%）

SSE_URL = ("https://query.sse.com.cn/commonQuery.do?sqlId=COMMON_SSE_SJ_GPSJ_CJGK_MRGK_C"
           "&isPagination=false&tradeDate={date}&PRODUCT_CODE=17")
SSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.sse.com.cn/"}
SZSE_URL = ("https://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON"
            "&CATALOGID=1803_sczm&TABKEY=tab1&txtQueryDate={date}")
SZSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.szse.cn/"}

DEFAULT_CONFIG = {
    "rf": 1.80,
    "gdp_ttm": 141.76,
    "mode": "fixed",
    "low": 51.44,
    "mid": 64.30,
    "high": 77.16,
    "rolling_months": 60,
}

state = {"ready": False}
state_lock = threading.Lock()
valuation_cache = {"ts": 0, "data": None}
top10_cache = {"ts": 0, "data": None}
rf_cache = {"ts": 0, "spot": None, "hist": None}


# ---------- 基础工具 ----------
def http_json(url, headers, timeout=10):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_text(url, headers, timeout=10):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "ignore")


def em_json(path, retries=3):
    last = None
    for host in EM_HOSTS:
        for _ in range(retries):
            try:
                return http_json(host + path, EM_HEADERS, timeout=10)
            except Exception as exc:  # noqa: BLE001
                last = exc
                time.sleep(1.2)
    raise RuntimeError("行情源全部失败：%s" % last)


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(CONFIG_JSON):
        try:
            with open(CONFIG_JSON, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:  # noqa: BLE001
            pass
    return cfg


def save_config(cfg):
    with open(CONFIG_JSON, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS snapshots(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL, date TEXT NOT NULL,
              total_mv REAL, ratio REAL, idx REAL,
              sh_mv REAL, sz_mv REAL, bj_mv REAL,
              amount REAL, zt INTEGER, dt INTEGER,
              pe REAL, pb REAL, erp REAL, top10 REAL
            );
            CREATE INDEX IF NOT EXISTS idx_snap_date ON snapshots(date);
            CREATE TABLE IF NOT EXISTS daily(
              date TEXT PRIMARY KEY, ratio REAL, idx REAL,
              high REAL, low REAL, amount REAL, zt INTEGER, dt INTEGER
            );
            CREATE TABLE IF NOT EXISTS alerts(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT, date TEXT, level TEXT, ratio REAL, message TEXT
            );
            CREATE TABLE IF NOT EXISTS meta(
              key TEXT PRIMARY KEY, value TEXT
            );
            """
        )
        # 老库平滑迁移：补齐涨跌家数列
        cols = [r[1] for r in conn.execute("PRAGMA table_info(snapshots)")]
        for col in ("up_cnt", "down_cnt", "flat_cnt"):
            if col not in cols:
                conn.execute("ALTER TABLE snapshots ADD COLUMN %s INTEGER" % col)
        # 清理历史上周末写入的伪数据（周末行情源返回的是上一交易日快照）
        conn.execute("DELETE FROM snapshots WHERE strftime('%w', date) IN ('0', '6')")
        conn.execute("DELETE FROM daily WHERE strftime('%w', date) IN ('0', '6')")


def load_history():
    with open(HIST_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def load_breadth_hist():
    """历史情绪序列：rows = [[date, up, total, flat], ...]。

    种子为通达信 880005（涨跌家数）日线，之后由本地每日收盘快照追加。
    """
    try:
        with open(BREADTH_HIST, "r", encoding="utf-8") as f:
            obj = json.load(f)
        rows = [r for r in obj.get("rows", []) if isinstance(r, list) and len(r) >= 3]
        return {"source": obj.get("source", ""), "rows": rows[-560:]}
    except Exception:  # noqa: BLE001
        return {"source": "", "rows": []}


def record_breadth(asof, now, b):
    """把涨跌家数并入历史文件。按行情实际交易日归属，与上一交易日完全相同的读数目视为陈旧跳过。"""
    if not b or not b.get("total"):
        return
    if asof.weekday() >= 5:
        return
    hm = now.hour * 60 + now.minute
    # 当天盘中/收盘后才记录；补录上一交易日（如周末启动）时直接写入
    if asof == now.date() and not (in_trading_hours(now) or hm >= 15 * 60 + 10):
        return
    date = asof.strftime("%Y-%m-%d")
    hist = load_breadth_hist()
    rows = list(hist["rows"])
    idx = next((i for i, r in enumerate(rows) if r[0] == date), None)
    if idx is None and rows:
        last = rows[-1]
        if last[1] == b["up"] and last[3] == b.get("flat", 0):
            # 节假日/休市时行情源返还上一交易日数据，避免记成新的一天
            return
    row = [date, int(b["up"]), int(b["total"]), int(b.get("flat") or 0)]
    if idx is None:
        rows.append(row)
    else:
        rows[idx] = row
    rows.sort(key=lambda r: r[0])
    with open(BREADTH_HIST, "w", encoding="utf-8") as f:
        json.dump({"source": hist.get("source") or "本地每日收盘快照", "rows": rows},
                  f, ensure_ascii=False)


def in_trading_hours(now=None):
    now = now or datetime.datetime.now(CST)
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    return (9 * 60 + 15 <= hm <= 11 * 60 + 35) or (12 * 60 + 55 <= hm <= 15 * 60 + 5)


# ---------- 抓取 ----------
def fetch_caps_em():
    """东方财富：总市值 + 涨跌家数（f104/f105/f106 为指数行情的涨/跌/平家数）。"""
    payload = em_json(CAP_PATH)
    rows = payload["data"]["diff"]
    caps = {"sh": 0.0, "sz": 0.0, "bj": 0.0}
    breadth = {"up": 0, "down": 0, "flat": 0}
    found_breadth = False
    asof = None
    for row in rows:
        name = row.get("f14", "")
        mv = float(row.get("f20") or 0) / 1e12
        if "上证" in name:
            caps["sh"] = mv
            ts = row.get("f124")
            if ts:
                try:
                    asof = datetime.datetime.fromtimestamp(int(ts), CST).date()
                except (TypeError, ValueError, OSError):
                    pass
        elif "深证" in name:
            caps["sz"] = mv
        elif "北证" in name or "北交" in name:
            caps["bj"] = mv
        else:
            continue
        if "北证" in name or "北交" in name:
            continue  # 北交所不计入主口径涨跌家数
        try:
            breadth["up"] += int(row.get("f104") or 0)
            breadth["down"] += int(row.get("f105") or 0)
            breadth["flat"] += int(row.get("f106") or 0)
            found_breadth = True
        except (TypeError, ValueError):
            pass
    if not caps["sh"] and not caps["sz"]:
        raise RuntimeError("总市值字段为空")
    return caps, (breadth if found_breadth else None), asof


def fetch_caps_official():
    """交易所官方口径兜底：上交所（股票）+ 深交所（股票），返回总市值与成交额（万亿元）。"""
    date = datetime.datetime.now(CST).strftime("%Y-%m-%d")
    sse = http_json(SSE_URL.format(date=date), SSE_HEADERS, timeout=10)
    sse_rows = sse.get("result") or []
    if not sse_rows:
        raise RuntimeError("上交所接口无当日数据")
    sse_row = sse_rows[0]
    szse = http_json(SZSE_URL.format(date=date), SZSE_HEADERS, timeout=10)
    szse_rows = (szse[0].get("data") if isinstance(szse, list) and szse else None) or []
    if not szse_rows:
        raise RuntimeError("深交所接口无当日数据")
    szse_row = szse_rows[0]
    sh = float(sse_row["TOTAL_VALUE"]) / 10000.0
    sz = float(str(szse_row["sjzz"]).replace(",", "")) / 10000.0
    # 交易所金额字段单位均为万元，除以 1e8 换算为万亿元（与新浪口径一致）
    sse_amt = float(sse_row.get("TRADE_AMT") or 0) / 1e8
    szse_amt = float(str(szse_row.get("cjje") or "0").replace(",", "")) / 1e8
    if sh <= 0 or sz <= 0:
        raise RuntimeError("交易所总市值为空")
    return {"sh": sh, "sz": sz, "bj": 0.0}, sse_amt + szse_amt


def fetch_caps():
    """优先东方财富；失败则切换交易所官方。返回 (caps, source, official_amount, breadth, asof)。"""
    try:
        caps, breadth, asof = fetch_caps_em()
        return caps, "东方财富", None, breadth, asof
    except Exception as exc:  # noqa: BLE001
        print("东方财富总市值失败，切换交易所官方：%s" % exc, flush=True)
        caps, amount = fetch_caps_official()
        return caps, "交易所官方", amount, None, None


def fetch_amount():
    text = http_text(SINA_URL, SINA_HEADERS, timeout=6)
    total = 0.0
    for line in text.strip().splitlines():
        if '"' not in line:
            continue
        parts = line.split('"')[1].split(",")
        if len(parts) >= 6:
            total += float(parts[5])  # 成交额：万元
    return total / 1e8  # 万元 -> 万亿元


def fetch_pool(url):
    try:
        today = datetime.datetime.now(CST).strftime("%Y%m%d")
        data = http_json(url.format(date=today), EM_HEADERS, timeout=8)
        return int(data.get("data", {}).get("tc") or 0)
    except Exception:  # noqa: BLE001
        return None


def fetch_valuation(force=False):
    now = time.time()
    if not force and valuation_cache["data"] and now - valuation_cache["ts"] < 300:
        return valuation_cache["data"]
    out = {}
    for key, (url, name) in DANJUAN.items():
        try:
            data = http_json(url, DANJUAN_HEADERS, timeout=8)["data"]
            out[key] = {
                "name": name,
                "pe": round(float(data["pe"]), 2),
                "pb": round(float(data["pb"]), 2),
                "pe_pct": round(float(data["pe_percentile"]) * 100, 1),
                "pb_pct": round(float(data["pb_percentile"]) * 100, 1),
                "dy": round(float(data["yeild"]) * 100, 2),
            }
        except Exception:  # noqa: BLE001
            out[key] = None
    valuation_cache.update({"ts": now, "data": out})
    return out


def fetch_top10(total_mv, force=False):
    now = time.time()
    if not force and top10_cache["data"] and now - top10_cache["ts"] < 300:
        return top10_cache["data"]
    try:
        payload = em_json(TOP10_PATH)
        rows = payload["data"]["diff"]
        top = sum(float(r.get("f20") or 0) / 1e12 for r in rows)
        result = {
            "ratio": round(top / total_mv * 100, 2) if total_mv else 0,
            "names": [r.get("f14", "") for r in rows[:5]],
        }
    except Exception:  # noqa: BLE001
        result = {"ratio": 0, "names": []}
    top10_cache.update({"ts": now, "data": result})
    return result


# ---------- 计算 ----------
def pct_rank(values, value):
    vals = [v for v in values if v is not None]
    if not vals or value is None:
        return None
    return round(sum(1 for v in vals if v <= value) / len(vals) * 100, 1)


def fetch_rf(force=False):
    """10年期国债收益率（%）：现值 + 日频历史 [(date, yld)]。缓存 1 小时，失败用上次缓存。"""
    now = time.time()
    if not force and rf_cache["spot"] and now - rf_cache["ts"] < 3600:
        return rf_cache["spot"], rf_cache["hist"]
    spot, hist = None, None
    try:
        data = em_json("/api/qt/stock/get?secid=%s&fields=f43&fltt=2" % RF_SECID)
        v = float((data.get("data") or {}).get("f43") or 0)
        if 0 < v < 10:
            spot = round(v, 3)
    except Exception:  # noqa: BLE001
        pass
    for host in ("https://push2his.eastmoney.com", "https://1.push2his.eastmoney.com"):
        try:
            payload = http_json(
                host + "/api/qt/stock/kline/get?secid=%s"
                "&fields1=f1,f2,f3&fields2=f51,f53&klt=101&fqt=0&end=20500101&lmt=2600" % RF_SECID,
                EM_HEADERS, timeout=10)
            rows = []
            for line in (payload.get("data") or {}).get("klines") or []:
                d, y = line.split(",")[:2]
                rows.append((d, float(y)))
            if len(rows) > 100:
                hist = rows
                break
        except Exception:  # noqa: BLE001
            continue
    if spot:
        rf_cache["spot"] = spot
    if hist:
        rf_cache["hist"] = hist
    rf_cache["ts"] = now
    return rf_cache["spot"], rf_cache["hist"]


def percentile_of(series, value):
    values = [p[1] for p in series]
    if not values:
        return 0
    below = sum(1 for v in values if v <= value)
    return round(below / len(values) * 100, 1)


def rolling_stats(series, months):
    values = [p[1] for p in series[-months:]]
    if not values:
        return 0, 0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return round(mean, 2), round(var ** 0.5, 2)


def pct_windows(series, ratio):
    """当前读数在不同时间窗口内的历史分位（%）。"""
    out = {}
    for key, months in (("m36", 36), ("m60", 60), ("m120", 120), ("all", None)):
        vals = [p[1] for p in (series[-months:] if months else series)]
        out[key] = pct_rank(vals, ratio) if vals else None
    return out


def hist_band(series, months=120):
    """近 N 个月读数分布：最低 / 20分位 / 中位 / 80分位 / 最高，供“历史温度计”用。"""
    vals = sorted(p[1] for p in series[-months:])
    if not vals:
        return None
    n = len(vals)
    return {"min": vals[0], "p20": vals[max(0, int(n * 0.2))], "p50": vals[n // 2],
            "p80": vals[min(n - 1, int(n * 0.8))], "max": vals[-1], "n": n}


def threshold_backtest(series, cfg):
    """阈值触发历史复盘（事件研究，非交易策略）。

    按当前阈值规则逐月还原历史区间：固定模式用配置阈值；滚动模式用该月
    往前 rolling_months 个月的均值 ±1σ（不足 24 个月跳过）。
    事件 = 读数“首次”进入高估区 / 低估区（连续多月只记首次），统计事件月
    之后 3 / 6 / 12 个月 880001 等价值（= 总市值 × 100）的涨跌幅。
    """
    mode = cfg.get("mode", "fixed")
    win = max(12, int(cfg.get("rolling_months", 60)))
    fixed_low, fixed_high = float(cfg["low"]), float(cfg["high"])

    ths = []  # 每月 (low, high)，None = 滚动窗口不足
    for i in range(len(series)):
        if mode == "rolling":
            vals = [p[1] for p in series[max(0, i - win + 1): i + 1]]
            if len(vals) < 24:
                ths.append(None)
                continue
            m = sum(vals) / len(vals)
            sd = (sum((v - m) ** 2 for v in vals) / len(vals)) ** 0.5
            ths.append((m - sd, m + sd))
        else:
            ths.append((fixed_low, fixed_high))

    def episodes(hit_fn):
        events, prev = [], False
        for i, p in enumerate(series):
            th = ths[i]
            hit = bool(th) and hit_fn(p[1], th[0], th[1])
            if hit and not prev:
                events.append((i, p[0], p[1], p[2]))
            prev = hit
        return events

    def summarize(events):
        rows, stats = [], {}
        for h in (3, 6, 12):
            rets = []
            for i, month, ratio, idx in events:
                j = i + h
                if j < len(series) and idx:
                    rets.append(round((series[j][2] / idx - 1) * 100, 2))
            rets_sorted = sorted(rets)
            stats[str(h)] = {
                "n": len(rets),
                "avg": round(sum(rets) / len(rets), 2) if rets else None,
                "med": rets_sorted[len(rets) // 2] if rets else None,
                "win": round(sum(1 for r in rets if r > 0) / len(rets) * 100, 1) if rets else None,
            }
        for i, month, ratio, idx in events:
            row = [month, ratio]
            for h in (3, 6, 12):
                j = i + h
                row.append(round((series[j][2] / idx - 1) * 100, 2)
                           if (j < len(series) and idx) else None)
            rows.append(row)
        rows.reverse()  # 最近的触发排前面
        return {"events": rows, "stats": stats}

    cur_th = ths[-1] if ths and ths[-1] else (fixed_low, fixed_high)
    return {
        "rule": ("滚动 %d 月均值±1σ" % win) if mode == "rolling" else "固定阈值",
        "th_low": round(cur_th[0], 2),
        "th_high": round(cur_th[1], 2),
        "high": summarize(episodes(lambda r, lo, hi: r >= hi)),
        "deep": summarize(episodes(lambda r, lo, hi: r <= lo)),
        "note": "月度级事件研究：触发=读数首次进入该区间，表内为事件月后 3/6/12 个月"
                " 880001 等价值（总市值）涨跌幅，非交易策略回测。",
    }


def breadth_of(breadth):
    """涨跌家数 -> 市场宽度指标。adv_ratio=涨/跌，breadth=涨家数占比。"""
    if not breadth:
        return None
    up, down, flat = breadth["up"], breadth["down"], breadth["flat"]
    total = up + down + flat
    if total <= 0:
        return None
    pct = round(up / total * 100, 1)
    if pct >= 70:
        mood, level = "情绪火热", "high"
    elif pct >= 50:
        mood, level = "情绪偏暖", "mid"
    elif pct >= 20:
        mood, level = "情绪偏冷", "low"
    else:
        mood, level = "情绪冰点", "deep"
    return {
        "up": up, "down": down, "flat": flat, "total": total,
        "pct": pct,
        "adv_ratio": round(up / down, 2) if down else None,
        "mood": mood, "level": level,
    }


def build_state(caps, amount, zt, dt, valuation, top10, hist, cfg, now, sources,
                rf_auto=None, rf_hist=None, pe_hist=None, breadth=None, asof_date=None):
    market_date = asof_date or now.date()
    # 主口径：沪深京全 A 总市值，与历史 880001 序列保持一致
    total = caps["sh"] + caps["sz"] + caps["bj"]
    gdp_yi = hist["gdp"].get(str(market_date.year - 1)) or hist["gdp"].get(str(market_date.year - 2))
    gdp_mv = gdp_yi / 10000.0
    ratio = round(total / gdp_mv * 100, 2)
    idx = round(total * 100, 2)
    month = market_date.strftime("%Y-%m")
    series = hist["series"]
    if series and series[-1][0] == month:
        series[-1] = [month, ratio, idx]
    else:
        series.append([month, ratio, idx])

    percentile = percentile_of(series, ratio)
    pctw = pct_windows(series, ratio)
    band = hist_band(series)
    backtest = threshold_backtest(series, cfg)
    mean5, sigma5 = rolling_stats(series, int(cfg.get("rolling_months", 60)))
    if cfg.get("mode") == "rolling" and sigma5:
        low, mid, high = round(mean5 - sigma5, 2), round(mean5, 2), round(mean5 + sigma5, 2)
    else:
        low, mid, high = float(cfg["low"]), float(cfg["mid"]), float(cfg["high"])

    hs300 = valuation.get("hs300") or {}
    rf_cfg = float(cfg["rf"])
    rf_used = rf_auto if rf_auto else rf_cfg
    erp = round(100.0 / hs300["pe"] - rf_used, 2) if hs300.get("pe") else None
    ttm_gdp = float(cfg.get("gdp_ttm") or 0)
    ttm_ratio = round(total / ttm_gdp * 100, 2) if ttm_gdp else None

    # ERP 历史分位：沪深300 周频 PE 历史 × 按日期对齐的 10Y 国债收益率
    erp_pct = None
    erp_band = None
    if pe_hist and pe_hist.get("pe") and len(pe_hist["pe"]) > 50:
        rf_map = dict(rf_hist) if rf_hist else {}
        erps = []
        for d, p in zip(pe_hist["dates"], pe_hist["pe"]):
            try:
                pf = float(p)
            except (TypeError, ValueError):
                continue
            if pf <= 0:
                continue
            erps.append(round(100.0 / pf - rf_map.get(d, rf_used), 2))
        if len(erps) > 50:
            erp_pct = pct_rank(erps, erp)
            sv = sorted(erps)
            n = len(sv)
            erp_band = {"low": sv[max(0, int(n * 0.2))], "mid": sv[n // 2],
                        "high": sv[min(n - 1, int(n * 0.8))]}

    prev_ratio = series[-2][1] if len(series) > 1 else ratio
    return {
        "ready": True,
        "updated": now.strftime("%Y-%m-%d %H:%M:%S"),
        "asof": market_date.strftime("%Y-%m-%d"),
        "series": series,
        "gdp": hist["gdp"],
        "cur": idx,
        "ratio": ratio,
        "prev_ratio": prev_ratio,
        "delta": round(ratio - prev_ratio, 2),
        "percentile": percentile,
        "pct_windows": pctw,
        "hist_band": band,
        "backtest": backtest,
        "mean5": mean5,
        "sigma5": sigma5,
        "thresholds": {"low": low, "mid": mid, "high": high,
                       "mode": cfg.get("mode", "fixed")},
        "market": {
            "sh_mv": round(caps["sh"], 2),
            "sz_mv": round(caps["sz"], 2),
            "bj_mv": round(caps["bj"], 3),
            "total_mv": round(total, 2),
            "amount": round(amount, 2) if amount is not None else None,
            "zt": zt,
            "dt": dt,
            "top10": top10.get("ratio", 0),
            "top10_names": top10.get("names", []),
            "bj_share": round(caps["bj"] / total * 100, 2) if total else 0,
        },
        "valuation": valuation,
        "erp": erp,
        "rf": round(rf_used, 3),
        "rf_source": "10Y国债·东方财富" if rf_auto else "配置值",
        "erp_pct": erp_pct,
        "erp_band": erp_band,
        "breadth": breadth_of(breadth),
        "breadth_hist": load_breadth_hist(),
        "ttm": {"gdp": ttm_gdp, "ratio": ttm_ratio},
        "config": cfg,
        "sources": sources,
    }


def level_of(ratio, th):
    if ratio >= th["high"]:
        return "high", "进入高估区"
    if ratio >= th["mid"]:
        return "mid", "处于正常区上沿"
    if ratio >= th["low"]:
        return "low", "处于偏低区"
    return "deep", "进入低估区"


def refresh(force_valuation=False):
    """串行刷新入口：其他进程正在刷新时直接跳过，避免重复写库/写文件。"""
    try:
        with runtime_lock.file_lock(REFRESH_LOCK):
            _refresh(force_valuation)
    except runtime_lock.LockBusy:
        print("[%s] 另一进程正在刷新，跳过本轮" %
              datetime.datetime.now(CST).strftime("%H:%M:%S"), flush=True)


def _refresh(force_valuation=False):
    now = datetime.datetime.now(CST)
    hist = load_history()
    cfg = load_config()
    caps, caps_source, official_amount, breadth, asof = fetch_caps()
    market_date = asof or now.date()
    amount_source = "新浪"
    try:
        amount = fetch_amount()
    except Exception:  # noqa: BLE001
        amount = None
    if amount is None:
        if official_amount is None:
            try:
                _, official_amount = fetch_caps_official()
            except Exception:  # noqa: BLE001
                official_amount = None
        amount = official_amount
        amount_source = "交易所官方" if amount is not None else "不可用"
    zt = fetch_pool(ZT_URL)
    dt = fetch_pool(DT_URL)
    valuation = fetch_valuation(force_valuation)
    rf_auto, rf_hist = fetch_rf()
    pe_hist = None
    try:
        pe_hist = indices_engine.fetch_pe_history("SH000300")
    except Exception:  # noqa: BLE001
        pe_hist = None
    total_all = caps["sh"] + caps["sz"] + caps["bj"]
    top10 = fetch_top10(total_all, force_valuation)

    sources = {
        "caps": caps_source,
        "amount": amount_source,
        "valuation": bool(valuation.get("hs300") and valuation.get("zz500")),
        "pools": zt is not None and dt is not None,
    }
    record_breadth(market_date, now, breadth_of(breadth))
    snap = build_state(caps, amount, zt, dt, valuation, top10, hist, cfg, now, sources,
                       rf_auto=rf_auto, rf_hist=rf_hist, pe_hist=pe_hist, breadth=breadth,
                       asof_date=market_date)
    date = market_date.strftime("%Y-%m-%d")

    # 写入历史文件
    hist["series"] = snap["series"]
    hist["cur"] = snap["cur"]
    hist["updated"] = snap["updated"]
    with open(HIST_JSON, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=1)

    # 落库：只在实际交易日写快照/日线/预警；周末与节假日只更新页面数据
    b = snap.get("breadth") or {}
    is_current = market_date == now.date()
    with db() as conn:
        if is_current:
            conn.execute(
                "INSERT INTO snapshots(ts,date,total_mv,ratio,idx,sh_mv,sz_mv,bj_mv,"
                "amount,zt,dt,pe,pb,erp,top10,up_cnt,down_cnt,flat_cnt) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (snap["updated"], date, snap["market"]["total_mv"], snap["ratio"], snap["cur"],
                 snap["market"]["sh_mv"], snap["market"]["sz_mv"], snap["market"]["bj_mv"],
                 amount, zt, dt, (valuation.get("hs300") or {}).get("pe"),
                 (valuation.get("hs300") or {}).get("pb"), snap["erp"], top10.get("ratio", 0),
                 b.get("up"), b.get("down"), b.get("flat")),
            )
            row = conn.execute("SELECT high, low, amount FROM daily WHERE date=?", (date,)).fetchone()
            hi = max(row[0], snap["ratio"]) if row else snap["ratio"]
            lo = min(row[1], snap["ratio"]) if row else snap["ratio"]
            conn.execute(
                "INSERT INTO daily(date,ratio,idx,high,low,amount,zt,dt) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(date) DO UPDATE SET ratio=excluded.ratio, idx=excluded.idx,"
                "high=excluded.high, low=excluded.low, amount=excluded.amount,"
                "zt=excluded.zt, dt=excluded.dt",
                (date, snap["ratio"], snap["cur"], hi, lo, amount, zt, dt),
            )
            prev = conn.execute("SELECT value FROM meta WHERE key='last_level'").fetchone()
            level, msg = level_of(snap["ratio"], snap["thresholds"])
            if prev and prev[0] != level:
                conn.execute("INSERT INTO alerts(ts,date,level,ratio,message) VALUES(?,?,?,?,?)",
                             (snap["updated"], date, level, snap["ratio"],
                              "读数 %.2f%%，%s" % (snap["ratio"], msg)))
            conn.execute("INSERT INTO meta(key,value) VALUES('last_level',?) "
                         "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (level,))

        conn.execute("DELETE FROM snapshots WHERE date < ?",
                     ((now - datetime.timedelta(days=30)).strftime("%Y-%m-%d"),))
        recent = conn.execute("SELECT id,ts,level,ratio,message FROM alerts ORDER BY id DESC LIMIT 8").fetchall()
        rows = conn.execute(
            "SELECT ts,ratio FROM snapshots WHERE date=? "
            "AND (substr(ts,12,5) BETWEEN '09:15' AND '11:35' "
            "OR substr(ts,12,5) BETWEEN '12:55' AND '15:05') ORDER BY id", (date,)).fetchall()
        # 同分钟多进程重复写入的历史样本按分钟去重，避免盘中曲线出现锯齿/重复点
        minute = {}
        for ts, ratio in rows:
            minute[ts[:16]] = ratio
        intraday = sorted(minute.items())

    with state_lock:
        state.clear()
        state.update(snap)
        state["alerts"] = [{"id": r[0], "ts": r[1], "level": r[2],
                            "ratio": r[3], "message": r[4]} for r in recent]
        state["intraday"] = [[r[0][11:16], r[1]] for r in intraday]
        state_text = json.dumps(state, ensure_ascii=False)
        data_text = "window.BUFFETT_DATA = " + json.dumps(
            state, ensure_ascii=False, separators=(",", ":")) + ";\n"
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        f.write(state_text)
    # data.js 输出完整快照，公网静态页在无本地 API 时也能展示估值/市场/预警等模块
    with open(DATA_JS, "w", encoding="utf-8") as f:
        f.write(data_text)
    print("[%s] %.4f万亿 | %.2f%% | 分位%.1f%% | 涨停%s 跌停%s | 源:%s/%s%s" %
          (snap["updated"], snap["market"]["total_mv"], snap["ratio"],
           snap["percentile"], zt, dt, caps_source, amount_source,
           "" if is_current else " | 非交易日，仅更新展示"), flush=True)


def loop():
    while True:
        ok = True
        try:
            refresh()
        except Exception as exc:  # noqa: BLE001
            ok = False
            print("[%s] 刷新失败：%s" % (datetime.datetime.now(CST).strftime("%H:%M:%S"), exc), flush=True)
        if not ok:
            time.sleep(15)
        else:
            time.sleep(30 if in_trading_hours() else 300)


def indices_loop():
    while True:
        try:
            payload = indices_engine.get_payload(force=True)
            with open(os.path.join(BASE, "indices.js"), "w", encoding="utf-8") as f:
                f.write("window.INDEX_DATA = " +
                        json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + ";\n")
            print("[%s] 指数全景已更新：%d 个指数" %
                  (datetime.datetime.now(CST).strftime("%H:%M:%S"), len(payload["indices"])))
        except Exception as exc:  # noqa: BLE001
            print("指数全景刷新失败：%s" % exc, flush=True)
        time.sleep(600 if in_trading_hours() else 1800)


# ---------- HTTP ----------
DISCONNECT_ERRORS = (ConnectionAbortedError, ConnectionResetError, BrokenPipeError)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE, **kwargs)

    def handle_one_request(self):
        # 浏览器刷新/关闭标签会打断进行中的响应，属正常现象，不应刷堆栈日志
        try:
            super().handle_one_request()
        except DISCONNECT_ERRORS:
            self.close_connection = True

    def end_headers(self):
        self.send_header("Cache-Control", "no-store, max-age=0")
        super().end_headers()

    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path in ("/", "/index.html"):
            self.send_response(302)
            self.send_header("Location", "/" + urllib.parse.quote(PAGE_NAME))
            self.end_headers()
            return
        if path == "/api/data":
            with state_lock:
                self._json(state)
            return
        if path == "/api/config":
            self._json(load_config())
            return
        if path == "/api/indices":
            try:
                self._json(indices_engine.get_payload())
            except Exception as exc:  # noqa: BLE001
                self._json({"error": str(exc)}, 503)
            return
        if path == "/api/export.csv":
            with state_lock:
                series = list(state.get("series", []))
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(["month", "ratio_pct", "index_880001"])
            for row in series:
                writer.writerow(row)
            body = buf.getvalue().encode("utf-8-sig")
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="buffett_history.csv"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def do_POST(self):
        path = self.path.split("?")[0]
        if path != "/api/config":
            self._json({"ok": False, "error": "not found"}, 404)
            return
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length).decode("utf-8"))
        except Exception:  # noqa: BLE001
            self._json({"ok": False, "error": "invalid json"}, 400)
            return
        cfg = load_config()
        allowed = {"rf", "gdp_ttm", "mode", "low", "mid", "high", "rolling_months"}
        num_keys = {"rf", "gdp_ttm", "low", "mid", "high", "rolling_months"}
        for key, value in body.items():
            if key not in allowed:
                continue
            if key in num_keys:
                try:
                    value = float(value)
                except (TypeError, ValueError):
                    self._json({"ok": False, "error": "参数 %s 必须是数字" % key}, 400)
                    return
                if value != value or value in (float("inf"), float("-inf")):
                    self._json({"ok": False, "error": "参数 %s 非法" % key}, 400)
                    return
            cfg[key] = value
        lo, mi, hi = float(cfg["low"]), float(cfg["mid"]), float(cfg["high"])
        if not (0 < lo <= mi <= hi):
            self._json({"ok": False, "error": "阈值需满足 0 < 低估线 ≤ 中轴 ≤ 高估线"}, 400)
            return
        if not (0 < float(cfg["rf"]) < 20 and float(cfg["gdp_ttm"]) > 0
                and 12 <= float(cfg["rolling_months"]) <= 600):
            self._json({"ok": False, "error": "rf/gdp_ttm/rolling_months 取值超出合理范围"}, 400)
            return
        if cfg.get("mode") not in ("fixed", "rolling"):
            self._json({"ok": False, "error": "mode 仅支持 fixed/rolling"}, 400)
            return
        save_config(cfg)
        self._json({"ok": True, "config": cfg})
        try:
            refresh(force_valuation=True)
        except Exception:  # noqa: BLE001
            pass

    def log_message(self, fmt, *args):
        pass


class QuietServer(ThreadingHTTPServer):
    """连接被客户端中断时静默处理，不打印堆栈。"""

    def handle_error(self, request, client_address):
        if isinstance(sys.exc_info()[1], DISCONNECT_ERRORS):
            return
        super().handle_error(request, client_address)


def main():
    init_db()

    cached = None
    if os.path.exists(STATE_JSON):
        try:
            with open(STATE_JSON, "r", encoding="utf-8") as f:
                cached = json.load(f)
        except Exception:  # noqa: BLE001
            cached = None

    if cached:
        with state_lock:
            state.clear()
            state.update(cached)
        print("已载入上次快照：%s" % cached.get("updated", ""), flush=True)
    else:
        try:
            hist = load_history()
            with state_lock:
                state.update({"series": hist.get("series", []), "gdp": hist.get("gdp", {}),
                              "cur": hist.get("cur", 0), "ready": False})
        except Exception as exc:  # noqa: BLE001
            print("历史数据读取失败：%s" % exc)

    # 单实例锁：Windows 下 SO_REUSEADDR 允许多个进程绑定同一端口，必须用文件锁兜底
    try:
        with runtime_lock.file_lock(INSTANCE_LOCK):
            _serve()
    except runtime_lock.LockBusy:
        print("已有实例在运行，本次启动退出：http://127.0.0.1:%d/" % PORT, flush=True)


def _serve():
    # 先绑定端口：占用时直接退出，避免抓取线程空跑
    try:
        server = QuietServer(("127.0.0.1", PORT), Handler)
    except OSError:
        print("端口 %d 已被占用，看板可能已在运行：http://127.0.0.1:%d/" % (PORT, PORT), flush=True)
        return

    threading.Thread(target=loop, daemon=True).start()
    threading.Thread(target=indices_loop, daemon=True).start()
    url = "http://127.0.0.1:%d/" % PORT
    if not os.environ.get("BUFFETT_NO_BROWSER"):
        threading.Timer(1.0, lambda: __import__("webbrowser").open(url)).start()
    print("看板已启动：%s（关闭窗口即停止）" % url, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
