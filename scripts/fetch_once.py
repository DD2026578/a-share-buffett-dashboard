#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CI 单次抓数：更新 data.js / indices.js / buffett_history.json 后退出。"""

import datetime
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

STATUS = {"time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

try:
    import buffett_server as server

    server.init_db()
    server.refresh(force_valuation=True)
    STATUS["buffett"] = {"ok": True, "ratio": round(server.state.get("ratio", 0), 2)}
    print("fetch ok: %.2f%%" % server.state.get("ratio", 0))
except Exception as exc:  # noqa: BLE001
    STATUS["buffett"] = {"ok": False, "error": str(exc)[:300]}
    print("fetch failed, keep previous data: %s" % exc)

try:
    import indices_engine

    payload = indices_engine.write_static(os.path.join(ROOT, "indices.js"))
    STATUS["indices"] = {"ok": True, "count": len(payload["indices"]),
                         "updated": payload["updated"]}
    print("indices ok: %d" % len(payload["indices"]))
except Exception as exc:  # noqa: BLE001
    STATUS["indices"] = {"ok": False, "error": str(exc)[:300]}
    print("indices failed, keep previous data: %s" % exc)

try:
    with open(os.path.join(ROOT, "docs", "fetch_status.json"), "w", encoding="utf-8") as f:
        json.dump(STATUS, f, ensure_ascii=False, indent=2)
except Exception as exc:  # noqa: BLE001
    print("status write failed: %s" % exc)

sys.exit(0)