#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""CI 单次抓数：更新 data.js / buffett_history.json 后退出。"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

try:
    import buffett_server as server

    server.init_db()
    server.refresh(force_valuation=True)
    print("fetch ok: %.2f%%" % server.state.get("ratio", 0))
except Exception as exc:  # noqa: BLE001
    print("fetch failed, keep previous data: %s" % exc)
    sys.exit(0)