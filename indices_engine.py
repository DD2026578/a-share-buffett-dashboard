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
]

EM_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
              "Referer": "https://quote.eastmoney.com/"}
DJ_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://danjuanfunds.com/"}

CACHE = {"ts": 0, "data": None}
LOCK = threading.Lock()
KLINE_HOSTS = [
    "https://push2his.eastmoney.com",
    "https://1.push2his.eastmoney.com",
    "https://2.push2his.eastmoney.com",
]


def http_json(url, headers, timeout=10):
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_kline_tencent(secid, limit=250):
    market, code = secid.split(".")
    if market == "1":
        sym = "sh" + code
    elif market == "0":
        sym = ("bj" if code.startswith("899") else "sz") + code
    else:
        raise RuntimeError("未知市场 %s" % secid)
    url = ("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=%s,day,,,%d,qfq"
           % (sym, limit))
    payload = http_json(url, {"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"},
                        timeout=10)
    data = payload["data"][sym]
    bars = data.get("day") or data.get("qfqday") or []
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


def fetch_kline(secid, limit=250):
    last = None
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
    try:
        return fetch_kline_tencent(secid, limit)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("K线抓取失败 %s: %s / %s" % (secid, last, exc))


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
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((v - mean) ** 2 for v in values) / len(values))


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
        mdd = max(mdd, (v - peak) / peak)
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

    metrics = {
        "code": item["code"],
        "name": item["name"],
        "secid": item["secid"],
        "date": last["date"],
        "close": last["close"],
        "pct": last["pct"],
        "amount": round(last["amount"] / 1e8, 1),
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
        "series": closes[-250:],
        "dates": [p["date"] for p in parsed][-250:],
    }
    metrics["signals"] = signal_list(metrics, prev_hist)
    return metrics


def percentile_rank(values, value):
    if not values or value is None:
        return 50.0
    return round(sum(1 for v in values if v <= value) / len(values) * 100, 1)


def add_cross_metrics(items):
    hs = next((x for x in items if x["code"] == "sh000300"), None)
    if not hs:
        return
    hs_dates = hs["dates"]
    hs_closes = hs["series"]
    hs_ret = {}
    for i in range(1, len(hs_closes)):
        hs_ret[hs_dates[i]] = hs_closes[i] / hs_closes[i - 1] - 1

    for it in items:
        dates, closes = it["dates"], it["series"]
        common = [d for d in dates if d in hs_ret]
        idx_ret = []
        hs_pair = []
        for i in range(1, len(closes)):
            d = dates[i]
            if d in hs_ret:
                idx_ret.append(closes[i] / closes[i - 1] - 1)
                hs_pair.append(hs_ret[d])
        idx_ret = idx_ret[-120:]
        hs_pair = hs_pair[-120:]
        n = min(len(idx_ret), len(hs_pair))
        if n >= 30:
            x, y = idx_ret[-n:], hs_pair[-n:]
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
    mdds = [x["mdd"] for x in items]
    pcts = [x["valuation"]["pe_pct"] for x in items if x["valuation"]]

    for it in items:
        trend = (
            percentile_rank(r20, it["ret"]["20"]) / 100 * 0.35
            + percentile_rank(r60, it["ret"]["60"]) / 100 * 0.35
            + (1 if it["close"] > (it["ma"]["20"] or 0) else 0) * 0.15
            + (1 if (it["ma"]["20"] or 0) > (it["ma"]["60"] or 0) else 0) * 0.15
        )
        value = (100 - it["valuation"]["pe_pct"]) / 100 if it["valuation"] else 0.5
        risk = (1 - percentile_rank(vols, it["vol60"]) / 100) * 0.5 + \
               (1 - percentile_rank(mdds, it["mdd"]) / 100) * 0.5
        it["score"] = round((0.4 * trend + 0.3 * value + 0.3 * risk) * 100, 1)
        it["score_parts"] = {"trend": round(trend * 100, 1), "value": round(value * 100, 1),
                             "risk": round(risk * 100, 1)}


def correlation_matrix(items, days=60):
    matrix = []
    for a in items:
        row = []
        for b in items:
            n = min(days, len(a["series"]) - 1, len(b["series"]) - 1)
            xa = [a["series"][i] / a["series"][i - 1] - 1 for i in range(len(a["series"]) - n, len(a["series"]))]
            xb = [b["series"][i] / b["series"][i - 1] - 1 for i in range(len(b["series"]) - n, len(b["series"]))]
            ma, mb = sum(xa) / n, sum(xb) / n
            cov = sum((xa[i] - ma) * (xb[i] - mb) for i in range(n)) / n
            va = sum((v - ma) ** 2 for v in xa) / n
            vb = sum((v - mb) ** 2 for v in xb) / n
            row.append(round(cov / math.sqrt(va * vb), 2) if va and vb else 0)
        matrix.append(row)
    return matrix


def build_payload():
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda it: (it, fetch_kline(it["secid"])), INDICES))
    valuations = {it["code"]: fetch_valuation(it["dj"]) for it in INDICES}
    items = [compute_index(it, bars, valuations.get(it["code"])) for it, bars in results]
    add_cross_metrics(items)
    add_scores(items)

    # 归一化走势（按周采样，基准 100）
    base = items[0]
    dates = base["dates"]
    step = max(1, len(dates) // 52)
    sampled_dates = dates[::step]
    norm = {}
    for it in items:
        base_value = it["series"][0]
        norm[it["code"]] = [round(v / base_value * 100, 2) for v in it["series"][::step]][:len(sampled_dates)]

    corr_items = items[:13]  # 北证50 独立市场，不参与相关性
    payload = {
        "updated": datetime.datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"),
        "indices": items,
        "corr": {"labels": [x["name"] for x in corr_items],
                 "matrix": correlation_matrix(corr_items)},
        "norm": {"dates": sampled_dates, "series": norm},
        "score_formula": "综合评分 = 40% 趋势 + 30% 估值分位 + 30% 风险（波动/回撤）",
    }
    return payload


def get_payload(force=False):
    with LOCK:
        now = time.time()
        if not force and CACHE["data"] and now - CACHE["ts"] < CACHE_SECONDS:
            return CACHE["data"]
        data = build_payload()
        CACHE.update({"ts": now, "data": data})
        return data


def write_static(path):
    data = get_payload(force=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("window.INDEX_DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + ";\n")
    return data


if __name__ == "__main__":
    d = build_payload()
    print("indices:", len(d["indices"]), "updated:", d["updated"])
    for x in d["indices"]:
        print("%-8s %9.2f %6.2f%% 20D=%6.2f%% 60D=%6.2f%% PE分位=%s 评分=%s 信号=%s" % (
            x["name"], x["close"], x["pct"], x["ret"]["20"] or 0, x["ret"]["60"] or 0,
            (x["valuation"]["pe_pct"] if x["valuation"] else "—"), x["score"], "/".join(x["signals"]) or "—"))