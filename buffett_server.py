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
import threading
import time
import urllib.parse
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import indices_engine

BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "buffett.db")
DATA_JS = os.path.join(BASE, "data.js")
HIST_JSON = os.path.join(BASE, "buffett_history.json")
CONFIG_JSON = os.path.join(BASE, "config.json")
STATE_JSON = os.path.join(BASE, "state.json")
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
CAP_PATH = "/api/qt/ulist.np/get?fltt=2&secids=1.000001,0.399106,0.899050&fields=f12,f14,f2,f3,f20"
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


def load_history():
    with open(HIST_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


def in_trading_hours(now=None):
    now = now or datetime.datetime.now(CST)
    if now.weekday() >= 5:
        return False
    hm = now.hour * 60 + now.minute
    return (9 * 60 + 15 <= hm <= 11 * 60 + 35) or (12 * 60 + 55 <= hm <= 15 * 60 + 5)


# ---------- 抓取 ----------
def fetch_caps_em():
    payload = em_json(CAP_PATH)
    rows = payload["data"]["diff"]
    caps = {"sh": 0.0, "sz": 0.0, "bj": 0.0}
    for row in rows:
        name = row.get("f14", "")
        mv = float(row.get("f20") or 0) / 1e12
        if "上证" in name:
            caps["sh"] = mv
        elif "深证" in name:
            caps["sz"] = mv
        elif "北证" in name or "北交" in name:
            caps["bj"] = mv
    if not caps["sh"] and not caps["sz"]:
        raise RuntimeError("总市值字段为空")
    return caps


def fetch_caps_official():
    """交易所官方口径兜底：上交所（股票）+ 深交所（股票），返回总市值与成交额（万亿元）。"""
    date = datetime.datetime.now(CST).strftime("%Y-%m-%d")
    sse = http_json(SSE_URL.format(date=date), SSE_HEADERS, timeout=10)
    sse_row = (sse.get("result") or [])[0]
    szse = http_json(SZSE_URL.format(date=date), SZSE_HEADERS, timeout=10)
    szse_row = szse[0]["data"][0]
    sh = float(sse_row["TOTAL_VALUE"]) / 10000.0
    sz = float(str(szse_row["sjzz"]).replace(",", "")) / 10000.0
    sse_amt = float(sse_row.get("TRADE_AMT") or 0) / 10000.0
    szse_amt = float(str(szse_row.get("cjje") or "0").replace(",", "")) / 10000.0
    if sh <= 0 or sz <= 0:
        raise RuntimeError("交易所总市值为空")
    return {"sh": sh, "sz": sz, "bj": 0.0}, sse_amt + szse_amt


def fetch_caps():
    """优先东方财富；失败则切换交易所官方。返回 (caps, source, official_amount)。"""
    try:
        return fetch_caps_em(), "东方财富", None
    except Exception as exc:  # noqa: BLE001
        print("东方财富总市值失败，切换交易所官方：%s" % exc, flush=True)
        caps, amount = fetch_caps_official()
        return caps, "交易所官方", amount


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


def build_state(caps, amount, zt, dt, valuation, top10, hist, cfg, now, sources):
    total = caps["sh"] + caps["sz"]
    gdp_yi = hist["gdp"].get(str(now.year - 1)) or hist["gdp"].get(str(now.year - 2))
    gdp_mv = gdp_yi / 10000.0
    ratio = round(total / gdp_mv * 100, 2)
    idx = round(total * 100, 2)
    month = now.strftime("%Y-%m")
    series = hist["series"]
    if series and series[-1][0] == month:
        series[-1] = [month, ratio, idx]
    else:
        series.append([month, ratio, idx])

    percentile = percentile_of(series, ratio)
    mean5, sigma5 = rolling_stats(series, int(cfg.get("rolling_months", 60)))
    if cfg.get("mode") == "rolling" and sigma5:
        low, mid, high = round(mean5 - sigma5, 2), round(mean5, 2), round(mean5 + sigma5, 2)
    else:
        low, mid, high = float(cfg["low"]), float(cfg["mid"]), float(cfg["high"])

    hs300 = valuation.get("hs300") or {}
    erp = round(100.0 / hs300["pe"] - float(cfg["rf"]), 2) if hs300.get("pe") else None
    ttm_gdp = float(cfg.get("gdp_ttm") or 0)
    ttm_ratio = round(total / ttm_gdp * 100, 2) if ttm_gdp else None

    prev_ratio = series[-2][1] if len(series) > 1 else ratio
    return {
        "ready": True,
        "updated": now.strftime("%Y-%m-%d %H:%M:%S"),
        "asof": now.strftime("%Y-%m-%d"),
        "series": series,
        "gdp": hist["gdp"],
        "cur": idx,
        "ratio": ratio,
        "prev_ratio": prev_ratio,
        "delta": round(ratio - prev_ratio, 2),
        "percentile": percentile,
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
        "rf": float(cfg["rf"]),
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
    now = datetime.datetime.now(CST)
    hist = load_history()
    cfg = load_config()
    caps, caps_source, official_amount = fetch_caps()
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
    total = caps["sh"] + caps["sz"]
    top10 = fetch_top10(total, force_valuation)

    sources = {
        "caps": caps_source,
        "amount": amount_source,
        "valuation": bool(valuation.get("hs300") and valuation.get("zz500")),
        "pools": zt is not None and dt is not None,
    }
    snap = build_state(caps, amount, zt, dt, valuation, top10, hist, cfg, now, sources)
    date = now.strftime("%Y-%m-%d")
    month = now.strftime("%Y-%m")

    # 写入历史文件与 data.js
    hist["series"] = snap["series"]
    hist["cur"] = snap["cur"]
    hist["updated"] = snap["updated"]
    with open(HIST_JSON, "w", encoding="utf-8") as f:
        json.dump(hist, f, ensure_ascii=False, indent=1)
    payload = {"updated": snap["updated"], "asof": snap["asof"],
               "series": snap["series"], "gdp": snap["gdp"],
               "cur": snap["cur"], "ratio": snap["ratio"]}
    with open(DATA_JS, "w", encoding="utf-8") as f:
        f.write("window.BUFFETT_DATA = " + json.dumps(payload, ensure_ascii=False,
                                                      separators=(",", ":")) + ";\n")

    # 落库
    with db() as conn:
        conn.execute(
            "INSERT INTO snapshots(ts,date,total_mv,ratio,idx,sh_mv,sz_mv,bj_mv,"
            "amount,zt,dt,pe,pb,erp,top10) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (snap["updated"], date, snap["market"]["total_mv"], snap["ratio"], snap["cur"],
             snap["market"]["sh_mv"], snap["market"]["sz_mv"], snap["market"]["bj_mv"],
             amount, zt, dt, (valuation.get("hs300") or {}).get("pe"),
             (valuation.get("hs300") or {}).get("pb"), snap["erp"], top10.get("ratio", 0)),
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
        conn.execute("DELETE FROM snapshots WHERE date < ?",
                     ((now - datetime.timedelta(days=30)).strftime("%Y-%m-%d"),))

        prev = conn.execute("SELECT value FROM meta WHERE key='last_level'").fetchone()
        level, msg = level_of(snap["ratio"], snap["thresholds"])
        if prev and prev[0] != level:
            conn.execute("INSERT INTO alerts(ts,date,level,ratio,message) VALUES(?,?,?,?,?)",
                         (snap["updated"], date, level, snap["ratio"],
                          "读数 %.2f%%，%s" % (snap["ratio"], msg)))
        conn.execute("INSERT INTO meta(key,value) VALUES('last_level',?) "
                     "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (level,))
        recent = conn.execute("SELECT id,ts,level,ratio,message FROM alerts ORDER BY id DESC LIMIT 8").fetchall()
        intraday = conn.execute(
            "SELECT ts,ratio FROM snapshots WHERE date=? ORDER BY id", (date,)).fetchall()

    with state_lock:
        state.clear()
        state.update(snap)
        state["alerts"] = [{"id": r[0], "ts": r[1], "level": r[2],
                            "ratio": r[3], "message": r[4]} for r in recent]
        state["intraday"] = [[r[0][11:16], r[1]] for r in intraday]
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)
    print("[%s] %.4f万亿 | %.2f%% | 分位%.1f%% | 涨停%s 跌停%s | 源:%s/%s" %
          (snap["updated"], snap["market"]["total_mv"], snap["ratio"],
           snap["percentile"], zt, dt, caps_source, amount_source), flush=True)


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
                  (datetime.datetime.now(CST).strftime("%H:%M:%S"), len(payload["indices"]))),
        except Exception as exc:  # noqa: BLE001
            print("指数全景刷新失败：%s" % exc, flush=True)
        time.sleep(600 if in_trading_hours() else 1800)


# ---------- HTTP ----------
class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=BASE, **kwargs)

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
        for key, value in body.items():
            if key in allowed:
                cfg[key] = value
        save_config(cfg)
        self._json({"ok": True, "config": cfg})
        try:
            refresh(force_valuation=True)
        except Exception:  # noqa: BLE001
            pass

    def log_message(self, fmt, *args):
        pass


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

    threading.Thread(target=loop, daemon=True).start()
    threading.Thread(target=indices_loop, daemon=True).start()
    try:
        server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    except OSError:
        url = "http://127.0.0.1:%d/" % PORT
        print("端口 %d 已被占用，看板可能已在运行：%s" % (PORT, url), flush=True)
        return
    url = "http://127.0.0.1:%d/" % PORT
    if not os.environ.get("BUFFETT_NO_BROWSER"):
        threading.Timer(1.0, lambda: __import__("webbrowser").open(url)).start()
    print("看板已启动：%s（关闭窗口即停止）" % url, flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()