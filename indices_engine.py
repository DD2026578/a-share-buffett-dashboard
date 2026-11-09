#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A股主流指数多维分析引擎。

覆盖：上证、深证成指、创业板、科创50、沪深300、中证500、中证1000、
中证红利、主要消费、全指医药、中证白酒、中证银行、证券公司、北证50。

指标：区间收益、均线、RSI、MACD、BOLL、波动率、最大回撤、52周位置、
相对强弱、Beta、相关性、估值分位、综合评分与信号。
"""

import datetime
import json
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import urllib.request

import runtime_lock

BASE = os.path.dirname(os.path.abspath(__file__))
CACHE_SECONDS = 600
CST = datetime.timezone(datetime.timedelta(hours=8))

INDICES = [
    {"code": "sh000001", "name": "上证指数", "secid": "1.000001", "dj": None},
    {"code": "sz399001", "name": "深证成指", "secid": "0.399001", "dj": "SZ399001"},
    {"code": "sz399006", "name": "创业板指", "secid": "0.399006", "dj": "SZ399006"},
    {"code": "sh000688", "name": "科创50", "secid": "1.000688", "dj": "SH000688"},
    {"code": "sh000300", "name": "沪深300", "secid": "1.000300", "dj": "SH000300"},
    {"code": "sh000905", "name": "中证500", "secid": "1.000905", "dj": "SH000905"},
    {"code": "sh000852", "name": "中证1000", "secid": "1.000852", "dj": "SH000852"},
    {"code": "sh000922", "name": "中证红利", "secid": "1.000922", "dj": "SH000922"},
    {"code": "sh000932", "name": "主要消费", "secid": "1.000932", "dj": "SH000932"},
    {"code": "sh000991", "name": "全指医药", "secid": "1.000991", "dj": "SH000991"},
    {"code": "sz399997", "name": "中证白酒", "secid": "0.399997", "dj": "SZ399997"},
    {"code": "sz399986", "name": "中证银行", "secid": "0.399986", "dj": "SZ399986"},
    {"code": "sz399975", "name": "证券公司", "secid": "0.399975", "dj": "SZ399975"},
    {"code": "bj899050", "name": "北证50", "secid": "0.899050", "dj": None},
    {"code": "hkHSI", "name": "恒生指数", "secid": "100.HSI", "dj": "HKHSI"},
    {"code": "hkHSTECH", "name": "恒生科技", "secid": "124.HSTECH", "dj": "HKHSTECH"},
]

EM_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
              "Referer": "https://quote.eastmoney.com/"}
DJ_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://danjuanfunds.com/"}

CACHE = {"ts": 0, "data": None}
MARKET_VAL = {"ts": 0, "data": None}
PE_HIST_CACHE = {}
LOCK = threading.Lock()
KLINE_HOSTS = [
    "https://push2his.eastmoney.com",
    "https://1.push2his.eastmoney.com",
    "https://2.push2his.eastmoney.com",
]
EM_KLINE_DOWN_UNTIL = 0.0


def _log(msg):
    """控制台输出失败（如管道断开）时静默，避免中断指数抓取流程。"""
    try:
        print(msg, flush=True)
    except Exception:  # noqa: BLE001
        pass


def http_json(url, headers, timeout=10):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_kline_tencent(secid, limit=3000):
    market, code = secid.split(".")
    if market == "1":
        sym = "sh" + code
    elif market == "0":
        sym = ("bj" if code.startswith("899") else "sz") + code
    elif market in ("100", "124"):
        sym = "hk" + code  # 港股：恒生指数 hkHSI、恒生科技 hkHSTECH
    else:
        raise RuntimeError("未知市场 %s" % secid)
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"}
    merged = {}
    end = None
    for _ in range(6):
        # 腾讯 fqkline 接口会返回 501，改用稳定的 kline 接口（指数无需复权）
        if end is None:
            url = ("https://web.ifzq.gtimg.cn/appstock/app/kline/kline?param=%s,day,,,640"
                   % sym)
        else:
            url = ("https://web.ifzq.gtimg.cn/appstock/app/kline/kline?param=%s,day,,%s,640"
                   % (sym, end.isoformat()))
        bars = []
        for _ in range(3):
            try:
                payload = http_json(url, headers, timeout=12)
                data = payload.get("data")
                if isinstance(data, dict) and isinstance(data.get(sym), dict):
                    bars = data[sym].get("day") or data[sym].get("qfqday") or []
                    if bars:
                        break
            except Exception:  # noqa: BLE001
                bars = []
            time.sleep(1.2)
        if not bars:
            break
        for bar in bars:
            merged[str(bar[0])] = bar
        first = datetime.date.fromisoformat(str(bars[0][0]))
        if first <= datetime.date(2010, 1, 1) or len(merged) >= limit:
            break
        end = first - datetime.timedelta(days=1)
        time.sleep(0.3)
    bars = [merged[d] for d in sorted(merged)][-limit:]
    if len(bars) < 30:
        raise RuntimeError("腾讯K线不足：%s" % sym)
    out = []
    prev = None
    for bar in bars:
        date, op, close, high, low, vol = bar[:6]
        pct = 0.0 if prev is None else (float(close) / prev - 1) * 100
        amp = 0.0 if prev is None else (float(high) - float(low)) / prev * 100
        out.append("%s,%s,%s,%s,%s,%s,0,%.2f,%.2f,0,0" %
                   (date, op, close, high, low, vol, amp, pct))
        prev = float(close)
    return out


def fetch_kline_sina(secid, limit=2000):
    """新浪日线兜底：北证50 等腾讯只给 1 根 K 线的标的可用。"""
    market, code = secid.split(".")
    if market == "1":
        sym = "sh" + code
    elif market == "0":
        sym = ("bj" if code.startswith("899") else "sz") + code
    else:
        raise RuntimeError("新浪不支持该市场 %s" % secid)
    url = ("https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/"
           "CN_MarketData.getKLineData?symbol=%s&scale=240&ma=no&datalen=%d"
           % (sym, min(limit, 2000)))
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"}
    rows = http_json(url, headers, timeout=12)
    if not isinstance(rows, list) or len(rows) < 30:
        raise RuntimeError("新浪K线不足：%s" % sym)
    out = []
    prev = None
    for row in rows:
        date = str(row.get("day") or "")[:10]
        op = row.get("open")
        close = row.get("close")
        high = row.get("high")
        low = row.get("low")
        vol = row.get("volume") or 0
        if not date or close is None:
            continue
        pct = 0.0 if prev is None else (float(close) / prev - 1) * 100
        amp = 0.0 if prev is None else (float(high) - float(low)) / prev * 100
        out.append("%s,%s,%s,%s,%s,%s,0,%.2f,%.2f,0,0" %
                   (date, op, close, high, low, vol, amp, pct))
        prev = float(close)
    return out


def fetch_kline(secid, limit=3000):
    global EM_KLINE_DOWN_UNTIL
    last = None
    if time.time() >= EM_KLINE_DOWN_UNTIL:
        for host in KLINE_HOSTS:
            for _ in range(2):
                try:
                    url = (host + "/api/qt/stock/kline/get?secid=%s"
                           "&fields1=f1,f2,f3,f4,f5,f6"
                           "&fields2=f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
                           "&klt=101&fqt=1&end=20500101&lmt=%d" % (secid, limit))
                    payload = http_json(url, EM_HEADERS, timeout=10)
                    bars = payload["data"]["klines"]
                    if bars:
                        return bars
                except Exception as exc:  # noqa: BLE001
                    last = exc
                    time.sleep(1.5)
        EM_KLINE_DOWN_UNTIL = time.time() + 600
    try:
        return fetch_kline_tencent(secid, limit)
    except Exception as exc:  # noqa: BLE001
        try:
            return fetch_kline_sina(secid, limit)
        except Exception as exc2:  # noqa: BLE001
            raise RuntimeError("K线抓取失败 %s: %s / %s / %s" % (secid, last, exc, exc2))


SH_FS = "m:1+t:2,m:1+t:23"
BJ_FS = "m:0+t:81+s:2048"
SSE_URL = ("https://query.sse.com.cn/commonQuery.do?sqlId=COMMON_SSE_SJ_GPSJ_CJGK_MRGK_C"
           "&isPagination=false&tradeDate={date}&PRODUCT_CODE=17")
SSE_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.sse.com.cn/"}


CLIST_HOSTS = [
    "https://1.push2.eastmoney.com",
    "https://2.push2.eastmoney.com",
    "https://3.push2.eastmoney.com",
    "https://push2delay.eastmoney.com",
]


def fetch_stock_pages(fs, max_pages=30):
    rows = []
    total = None
    for pn in range(1, max_pages + 1):
        path = ("/api/qt/clist/get?pn=%d&pz=100&po=1&np=1"
                "&fltt=2&invt=2&fid=f20&fs=%s&fields=f12,f14,f20,f9,f23" % (pn, fs))
        payload = None
        for host in CLIST_HOSTS:
            for _ in range(2):
                try:
                    payload = http_json(host + path, EM_HEADERS, timeout=12)
                    break
                except Exception:
                    time.sleep(0.8)
            if payload:
                break
        if not payload:
            break
        data = payload.get("data") or {}
        diff = data.get("diff") or []
        if total is None:
            total = int(data.get("total") or 0)
        rows.extend(diff)
        if not diff or len(rows) >= total:
            break
        time.sleep(0.2)
    return rows


def sse_market_pe():
    try:
        date = datetime.datetime.now(CST).strftime("%Y-%m-%d")
        data = http_json(SSE_URL.format(date=date), SSE_HEADERS, timeout=10)
        row = (data.get("result") or [])[0]
        return {"pe": round(float(row["AVG_PE_RATE"]), 2), "pb": None,
                "pe_pct": None, "pb_pct": None, "dy": None,
                "source": "上交所官方平均（非市值加权）"}
    except Exception:  # noqa: BLE001
        return None


def to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def aggregate_val(rows, top_n=None, source=""):
    valid = [r for r in rows if to_float(r.get("f20")) > 0]
    valid.sort(key=lambda r: to_float(r["f20"]), reverse=True)
    if top_n:
        valid = valid[:top_n]
    cap = sum(to_float(r["f20"]) for r in valid)
    earn = sum(to_float(r["f20"]) / to_float(r["f9"]) for r in valid if to_float(r.get("f9")) > 0)
    book = sum(to_float(r["f20"]) / to_float(r["f23"]) for r in valid if to_float(r.get("f23")) > 0)
    return {
        "pe": round(cap / earn, 2) if earn else None,
        "pb": round(cap / book, 2) if book else None,
        "pe_pct": None,
        "pb_pct": None,
        "dy": None,
        "source": source,
    }


def sh_valuation():
    """上证：PE 用上交所官方平均（稳定），PB 用沪市前300只市值加权估算。"""
    pe = sse_market_pe()
    rows = fetch_stock_pages(SH_FS, max_pages=3)
    agg = aggregate_val(rows) if rows else {}
    return {
        "pe": (pe or {}).get("pe") or agg.get("pe"),
        "pb": agg.get("pb"),
        "pe_pct": None,
        "pb_pct": None,
        "dy": None,
        "source": "PE 上交所官方 / PB 沪市前300估算",
    }


def market_valuation():
    """沪市 + 北证市值前50 的 PE/PB，缓存 30 分钟。"""
    now = time.time()
    if MARKET_VAL["data"] and now - MARKET_VAL["ts"] < 1800:
        return MARKET_VAL["data"]
    bj_rows = fetch_stock_pages(BJ_FS, max_pages=5)
    bj = aggregate_val(bj_rows, top_n=50, source="北证市值前50估算") if bj_rows else \
        {"pe": None, "pb": None, "pe_pct": None, "pb_pct": None, "dy": None, "source": "北证数据暂缺"}
    sh = sh_valuation()
    data = {"sh": sh, "bj": bj}
    MARKET_VAL.update({"ts": now, "data": data})
    return data


def quantile(values, q):
    if not values:
        return None
    vals = sorted(values)
    k = (len(vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(vals) - 1)
    return round(vals[lo] + (vals[hi] - vals[lo]) * (k - lo), 2)


def fetch_pe_history(dj_code):
    """蛋卷 10 年 PE 周频序列，缓存 1 小时。"""
    if not dj_code:
        return None
    now = time.time()
    cached = PE_HIST_CACHE.get(dj_code)
    if cached and now - cached["ts"] < 3600:
        return cached["data"]
    data = None
    try:
        payload = http_json(
            "https://danjuanfunds.com/djapi/index_eva/pe_history/%s?day=all" % dj_code,
            DJ_HEADERS, timeout=10)
        items = payload["data"]["index_eva_pe_growths"]
        dates = [datetime.datetime.fromtimestamp(float(i["ts"]) / 1000, CST).strftime("%Y-%m-%d")
                 for i in items]
        pes = [float(i["pe"]) for i in items]
        if len(pes) > 30:
            data = {"dates": dates, "pe": pes}
    except Exception:  # noqa: BLE001
        data = None
    PE_HIST_CACHE[dj_code] = {"ts": now, "data": data}
    return data


def attach_pe_channel(val, dj_code):
    if not val or not dj_code:
        return val
    hist = fetch_pe_history(dj_code)
    if not hist:
        return val
    pes = hist["pe"]
    val["pe_low"] = quantile(pes, 0.2)
    val["pe_mid"] = quantile(pes, 0.5)
    val["pe_high"] = quantile(pes, 0.8)
    pe = val.get("pe")
    if pe is not None and val["pe_low"] is not None:
        if pe < val["pe_low"]:
            val["pe_zone"] = "低估"
        elif pe < val["pe_mid"]:
            val["pe_zone"] = "偏低"
        elif pe < val["pe_high"]:
            val["pe_zone"] = "正常"
        else:
            val["pe_zone"] = "高估"
    step = max(1, len(hist["dates"]) // 120)
    idxs = list(range(0, len(hist["dates"]), step))
    if idxs[-1] != len(hist["dates"]) - 1:
        idxs.append(len(hist["dates"]) - 1)
    val["pe_hist"] = {"dates": [hist["dates"][i] for i in idxs],
                      "pe": [round(pes[i], 2) for i in idxs]}
    return val


def fetch_valuation(dj_code):
    if not dj_code:
        return None
    try:
        data = http_json("https://danjuanfunds.com/djapi/index_eva/detail/" + dj_code,
                         DJ_HEADERS, timeout=8)["data"]
        if not data.get("pe"):
            return None
        return {
            "pe": round(float(data["pe"]), 2),
            "pb": round(float(data["pb"]), 2),
            "pe_pct": round(float(data["pe_percentile"]) * 100, 1),
            "pb_pct": round(float(data["pb_percentile"]) * 100, 1),
            "dy": round(float(data.get("yeild") or 0) * 100, 2),
            "source": "蛋卷指数估值",
        }
    except Exception:  # noqa: BLE001
        return None


def ema(values, n):
    alpha = 2.0 / (n + 1)
    out = []
    for i, v in enumerate(values):
        out.append(v if i == 0 else alpha * v + (1 - alpha) * out[-1])
    return out


def rsi14(closes):
    if len(closes) < 15:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:14]) / 14
    avg_loss = sum(losses[:14]) / 14
    for i in range(14, len(gains)):
        avg_gain = (avg_gain * 13 + gains[i]) / 14
        avg_loss = (avg_loss * 13 + losses[i]) / 14
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def stdev(values):
    """样本标准差（n-1），与通达信 BOLL/主流行情软件口径一致。"""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / (len(values) - 1))


def pct_change(closes, n):
    if len(closes) <= n:
        return None
    return round((closes[-1] / closes[-1 - n] - 1) * 100, 2)


def max_drawdown(closes, n=250):
    window = closes[-n:]
    peak = window[0]
    mdd = 0.0
    for v in window:
        peak = max(peak, v)
        mdd = min(mdd, (v - peak) / peak)
    return round(mdd * 100, 2)


def signal_list(m, prev_macd_hist):
    out = []
    if m["close"] > m["ma"]["20"] > m["ma"]["60"]:
        out.append("多头排列")
    elif m["close"] < m["ma"]["20"] < m["ma"]["60"]:
        out.append("空头排列")
    if m["macd"]["hist"] > 0 >= prev_macd_hist:
        out.append("MACD 金叉")
    elif m["macd"]["hist"] < 0 <= prev_macd_hist:
        out.append("MACD 死叉")
    if m["rsi"] is not None:
        if m["rsi"] >= 70:
            out.append("RSI 超买")
        elif m["rsi"] <= 30:
            out.append("RSI 超卖")
    if m["boll"]["pct_b"] is not None:
        if m["boll"]["pct_b"] > 1:
            out.append("突破布林上轨")
        elif m["boll"]["pct_b"] < 0:
            out.append("跌破布林下轨")
    return out


def fetch_latest_amount(secid):
    """腾讯实时行情：返回 (行情日期, 成交额(元))，K线兜底源不含成交额时补齐。"""
    market, code = secid.split(".")
    if market == "1":
        sym = "sh" + code
    elif market == "0":
        sym = ("bj" if code.startswith("899") else "sz") + code
    elif market in ("100", "124"):
        sym = "hk" + code
    else:
        return None, None
    try:
        req = urllib.request.Request("https://qt.gtimg.cn/q=%s" % sym,
                                     headers={"User-Agent": "Mozilla/5.0",
                                              "Referer": "https://gu.qq.com/"})
        with urllib.request.urlopen(req, timeout=6) as resp:
            text = resp.read().decode("gbk", "ignore")
        parts = text.split('"')[1].split("~")
        d = str(parts[30])[:8]
        date = "%s-%s-%s" % (d[:4], d[4:6], d[6:8])
        return date, float(parts[37]) * 1e4  # 万元 -> 元
    except Exception:  # noqa: BLE001
        return None, None


def compute_index(item, bars, valuation):
    parsed = []
    for bar in bars:
        parts = bar.split(",")
        parsed.append({"date": parts[0], "open": float(parts[1]), "close": float(parts[2]),
                       "high": float(parts[3]), "low": float(parts[4]),
                       "volume": float(parts[5]), "amount": float(parts[6]),
                       "pct": float(parts[8])})
    closes = [p["close"] for p in parsed]
    last = parsed[-1]
    if not last["amount"]:
        qdate, qamt = fetch_latest_amount(item["secid"])
        if qamt and qdate == last["date"]:
            last["amount"] = qamt
    ma = {}
    for n in (5, 20, 60, 120, 250):
        ma[str(n)] = round(sum(closes[-n:]) / min(n, len(closes)), 2) if len(closes) >= n else None
    dif = [a - b for a, b in zip(ema(closes, 12), ema(closes, 26))]
    dea = ema(dif, 9)
    hist = 2 * (dif[-1] - dea[-1])
    prev_hist = 2 * (dif[-2] - dea[-2]) if len(dif) > 1 else hist
    mid = ma["20"]
    sd = stdev(closes[-20:]) if len(closes) >= 20 else 0
    up = round(mid + 2 * sd, 2) if mid else None
    low = round(mid - 2 * sd, 2) if mid else None
    pct_b = round((last["close"] - low) / (up - low), 3) if up and low and up != low else None

    recent = closes[-21:]
    vol20 = round(stdev([recent[i] / recent[i - 1] - 1 for i in range(1, len(recent))]) * math.sqrt(252) * 100, 2) if len(recent) > 2 else 0
    vol60 = round(stdev([closes[i] / closes[i - 1] - 1 for i in range(len(closes) - 60, len(closes))]) * math.sqrt(252) * 100, 2) if len(closes) > 60 else vol20

    high250 = max(closes[-250:])
    low250 = min(closes[-250:])
    pos250 = round((last["close"] - low250) / (high250 - low250) * 100, 1) if high250 != low250 else 50

    def ma_full(n):
        out = []
        s = 0.0
        for i, v in enumerate(closes):
            s += v
            if i >= n:
                s -= closes[i - n]
            out.append(round(s / n, 2) if i >= n - 1 else None)
        return out

    ma20f = ma_full(20)
    ma60f = ma_full(60)
    step = max(1, len(parsed) // 600)
    idxs = list(range(0, len(parsed), step))
    if idxs[-1] != len(parsed) - 1:
        idxs.append(len(parsed) - 1)

    metrics = {
        "code": item["code"],
        "name": item["name"],
        "secid": item["secid"],
        "date": last["date"],
        "close": last["close"],
        "pct": last["pct"],
        "amount": round(last["amount"] / 1e8, 1) if last["amount"] else None,
        "ret": {str(n): pct_change(closes, n) for n in (1, 5, 20, 60, 120, 250)},
        "ma": ma,
        "rsi": rsi14(closes),
        "macd": {"dif": round(dif[-1], 3), "dea": round(dea[-1], 3), "hist": round(hist, 3)},
        "boll": {"mid": mid, "up": up, "low": low, "pct_b": pct_b},
        "vol20": vol20,
        "vol60": vol60,
        "mdd": max_drawdown(closes, 250),
        "high250": round(high250, 2),
        "low250": round(low250, 2),
        "pos250": pos250,
        "valuation": valuation,
        "series": [parsed[i]["close"] for i in idxs],
        "dates": [parsed[i]["date"] for i in idxs],
        "ma20s": [ma20f[i] for i in idxs],
        "ma60s": [ma60f[i] for i in idxs],
        "ret10y": (round((closes[-1] / closes[-1 - min(2500, len(closes) - 1)] - 1) * 100, 2)
                   if len(closes) > 2 else None),
        "high10y": round(max(closes[-2500:]), 2),
        "low10y": round(min(closes[-2500:]), 2),
        # 内部字段：供统一日期轴计算相关性 / Beta / 归一化走势，序列化前移除
        "_daily_dates": [p["date"] for p in parsed],
        "_daily_closes": closes,
    }
    metrics["signals"] = signal_list(metrics, prev_hist)
    return metrics


def percentile_rank(values, value):
    if not values or value is None:
        return 50.0
    return round(sum(1 for v in values if v <= value) / len(values) * 100, 1)


def _close_map(it):
    """日期 -> 收盘价。优先用日线全量序列；缓存复用的指数退回抽样序列。"""
    dates = it.get("_daily_dates")
    if dates:
        return dict(zip(dates, it.get("_daily_closes") or []))
    return dict(zip(it.get("dates") or [], it.get("series") or []))


def add_cross_metrics(items):
    """Beta / 相关系数：按同一批交易日对齐后计算，避免各指数抽样步长不同导致错配。"""
    hs = next((x for x in items if x["code"] == "sh000300"), None)
    if not hs:
        return
    hs_map = _close_map(hs)
    for it in items:
        m = _close_map(it)
        common = sorted(set(m) & set(hs_map))[-121:]
        x, y = [], []
        for i in range(1, len(common)):
            d0, d1 = common[i - 1], common[i]
            v0, v1 = m.get(d0), m.get(d1)
            h0, h1 = hs_map.get(d0), hs_map.get(d1)
            if not (v0 and v1 and h0 and h1):
                continue
            x.append(v1 / v0 - 1)
            y.append(h1 / h0 - 1)
        n = len(x)
        if n >= 30:
            mx, my = sum(x) / n, sum(y) / n
            cov = sum((x[i] - mx) * (y[i] - my) for i in range(n)) / n
            vx = sum((v - mx) ** 2 for v in x) / n
            vy = sum((v - my) ** 2 for v in y) / n
            beta = round(cov / vy, 2) if vy else None
            corr = round(cov / math.sqrt(vx * vy), 2) if vx and vy else None
        else:
            beta = corr = None
        it["beta"] = beta
        it["corr"] = corr
        it["rs20"] = round((it["ret"]["20"] or 0) - (hs["ret"]["20"] or 0), 2)
        it["rs60"] = round((it["ret"]["60"] or 0) - (hs["ret"]["60"] or 0), 2)


def add_scores(items):
    r20 = [x["ret"]["20"] for x in items if x["ret"]["20"] is not None]
    r60 = [x["ret"]["60"] for x in items if x["ret"]["60"] is not None]
    vols = [x["vol60"] for x in items]
    mdds = [abs(x["mdd"]) for x in items]

    for it in items:
        trend = (
            percentile_rank(r20, it["ret"]["20"]) / 100 * 0.35
            + percentile_rank(r60, it["ret"]["60"]) / 100 * 0.35
            + (1 if it["close"] > (it["ma"]["20"] or 0) else 0) * 0.15
            + (1 if (it["ma"]["20"] or 0) > (it["ma"]["60"] or 0) else 0) * 0.15
        )
        value = (100 - it["valuation"]["pe_pct"]) / 100 if (it["valuation"] and it["valuation"].get("pe_pct") is not None) else 0.5
        risk = (1 - percentile_rank(vols, it["vol60"]) / 100) * 0.5 + \
               (1 - percentile_rank(mdds, abs(it["mdd"])) / 100) * 0.5
        it["score"] = round((0.4 * trend + 0.3 * value + 0.3 * risk) * 100, 1)
        it["score_parts"] = {"trend": round(trend * 100, 1), "value": round(value * 100, 1),
                             "risk": round(risk * 100, 1)}


def add_combo_signal(items):
    """左侧转强 = 估值便宜（PE分位<30 或 通道低估/偏低）且 趋势转强（收盘>MA20 且 20日跑赢沪深300）。"""
    for it in items:
        v = it.get("valuation") or {}
        pe_pct = v.get("pe_pct")
        zone = v.get("pe_zone")
        cheap = (pe_pct is not None and pe_pct < 30) or (zone in ("低估", "偏低"))
        strong = it["close"] > (it["ma"]["20"] or 0) and (it.get("rs20") or 0) > 0
        it["combo"] = bool(cheap and strong)


def correlation_matrix(items, days=60):
    """近 days 个交易日的收益率相关性矩阵，按参考指数的日期轴逐日对齐。"""
    ref_map = _close_map(items[0])
    axis = sorted(ref_map)[-(days + 1):]
    maps = {x["code"]: _close_map(x) for x in items}
    matrix = []
    for a in items:
        row = []
        for b in items:
            ma, mb = maps[a["code"]], maps[b["code"]]
            common = [d for d in axis if d in ma and d in mb]
            xa, xb = [], []
            for i in range(1, len(common)):
                d0, d1 = common[i - 1], common[i]
                a0, a1 = ma.get(d0), ma.get(d1)
                b0, b1 = mb.get(d0), mb.get(d1)
                if not (a0 and a1 and b0 and b1):
                    continue
                xa.append(a1 / a0 - 1)
                xb.append(b1 / b0 - 1)
            n = len(xa)
            if n < 20:
                row.append(0)
                continue
            mxa, mxb = sum(xa) / n, sum(xb) / n
            cov = sum((xa[i] - mxa) * (xb[i] - mxb) for i in range(n)) / n
            va = sum((v - mxa) ** 2 for v in xa) / n
            vb = sum((v - mxb) ** 2 for v in xb) / n
            row.append(round(cov / math.sqrt(va * vb), 2) if va and vb else 0)
        matrix.append(row)
    return matrix


def _fetch_one(it):
    """单个指数的 K 线抓取，失败重试 3 次，返回 (item, bars, error)。"""
    last = None
    for attempt in range(3):
        try:
            return it, fetch_kline(it["secid"], 3000), None
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 + attempt * 2)
    return it, None, last


def _load_cached_items():
    """读取上次成功生成的 indices.js，供个别指数抓取失败时兜底。"""
    path = os.path.join(BASE, "indices.js")
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        data = json.loads(text[text.index("{"):text.rindex("}") + 1])
        return {x["code"]: x for x in data.get("indices", [])}
    except Exception:  # noqa: BLE001
        return {}


def build_payload():
    cached = _load_cached_items()
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(_fetch_one, INDICES))
    valuations = {}
    for it in INDICES:
        val = fetch_valuation(it["dj"])
        if not val and cached.get(it["code"]):
            # 蛋卷临时失败时沿用上次成功的估值，避免 PE/PB/分位突然消失
            val = cached[it["code"]].get("valuation")
            if val:
                _log("reuse valuation %s" % it["code"])
        valuations[it["code"]] = attach_pe_channel(val, it["dj"])
    mkt = market_valuation()
    if not valuations.get("sh000001"):
        valuations["sh000001"] = mkt.get("sh")
    if not valuations.get("bj899050"):
        valuations["bj899050"] = mkt.get("bj")
    items, reused, fresh = [], [], 0
    failed = []
    for it, bars, err in results:
        if bars:
            items.append(compute_index(it, bars, valuations.get(it["code"])))
            fresh += 1
        elif cached.get(it["code"]):
            items.append(cached[it["code"]])
            reused.append(it["code"])
            _log("reuse cached %s: %s" % (it["code"], err))
        else:
            # 单个指数失败且有缓存兜底时可以跳过，不再让整轮重建全部作废
            failed.append(it["code"])
            _log("skip %s: %s" % (it["code"], err))
    if fresh < 5:
        raise RuntimeError("仅 %d 个指数抓取成功，保留旧数据" % fresh)
    # 缓存复用的指数没有日线序列，用近一年抽样收盘近似回撤，避免沿用旧的失效值
    for it in items:
        if not it.get("_daily_dates"):
            cutoff = (datetime.date.fromisoformat(it["date"])
                      - datetime.timedelta(days=365)).isoformat()
            closes = [c for d, c in zip(it.get("dates") or [], it.get("series") or []) if d >= cutoff]
            if len(closes) >= 30:
                it["mdd"] = max_drawdown(closes)
    add_cross_metrics(items)
    add_scores(items)
    add_combo_signal(items)

    # 归一化走势：统一以参考指数（上证）的日期轴抽样，其余指数按日期取收盘，历史不足处留空
    base_dates = items[0].get("_daily_dates") or items[0]["dates"]
    step = max(1, len(base_dates) // 600)
    idxs = list(range(0, len(base_dates), step))
    if idxs[-1] != len(base_dates) - 1:
        idxs.append(len(base_dates) - 1)
    sampled_dates = [base_dates[i] for i in idxs]
    date_index = {d: i for i, d in enumerate(sampled_dates)}
    norm = {}
    for it in items:
        m = _close_map(it)
        row = [None] * len(sampled_dates)
        pts = [(date_index[d], m[d]) for d in sampled_dates if d in m and m[d]]
        if not pts:
            norm[it["code"]] = row
            continue
        v0 = pts[0][1]
        for j, v in pts:
            row[j] = round(v / v0 * 100, 2) if v0 else None
        norm[it["code"]] = row

    # 北证50 与港股属独立市场，不参与 A 股相关性矩阵
    corr_items = [x for x in items if x["code"] not in ("bj899050", "hkHSI", "hkHSTECH")]
    corr = {"labels": [x["name"] for x in corr_items],
            "matrix": correlation_matrix(corr_items)}
    for it in items:
        it.pop("_daily_dates", None)
        it.pop("_daily_closes", None)

    payload = {
        "updated": datetime.datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        "indices": items,
        "corr": corr,
        "norm": {"dates": sampled_dates, "series": norm},
        "reused": reused,
        "failed": failed,
        "score_formula": "综合评分 = 40% 趋势 + 30% 估值分位 + 30% 风险（波动/回撤）",
        "combo_formula": "左侧转强 = 估值便宜（PE分位<30 或 通道低估/偏低）且 收盘>MA20 且 20日相对沪深300超额>0",
    }
    return payload


def _load_static_payload():
    """启动后首次请求先用磁盘上的 indices.js 兜底，避免被全量重建阻塞。"""
    path = os.path.join(BASE, "indices.js")
    try:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        return json.loads(text[text.index("{"):text.rindex("}") + 1])
    except Exception:  # noqa: BLE001
        return None


def get_payload(force=False):
    # 非强制请求只读缓存/磁盘，绝不等待网络抓取，避免接口被后台重建阻塞
    if not force:
        data = CACHE["data"]
        if data:
            return data
        disk = _load_static_payload()
        if disk:
            CACHE.update({"ts": time.time(), "data": disk})
            return disk
    with LOCK:
        if not force and CACHE["data"]:
            return CACHE["data"]
        data = build_payload()
        CACHE.update({"ts": time.time(), "data": data})
        return data


def _atomic_write(path, text, retries=3):
    """先写临时文件再原子替换，避免浏览器读到写了一半的 indices.js。"""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    for i in range(retries):
        try:
            os.replace(tmp, path)
            return
        except OSError:
            if i == retries - 1:
                raise
            time.sleep(0.25)


def write_static(path):
    data = get_payload(force=True)
    text = "window.INDEX_DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + ";\n"
    with runtime_lock.file_lock(os.path.join(BASE, "indices_write.lock")):
        _atomic_write(path, text)
    return data


if __name__ == "__main__":
    d = build_payload()
    print("indices:", len(d["indices"]), "updated:", d["updated"])
    for x in d["indices"]:
        print("%-8s %9.2f %6.2f%% 20D=%6.2f%% 60D=%6.2f%% PE分位=%s 评分=%s 信号=%s" % (
            x["name"], x["close"], x["pct"], x["ret"]["20"] or 0, x["ret"]["60"] or 0,
            (x["valuation"]["pe_pct"] if x["valuation"] else "—"), x["score"], "/".join(x["signals"]) or "—"))
