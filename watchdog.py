#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看板守护进程：服务异常退出后 5 秒自动重启。"""

import os
import subprocess
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(BASE, "buffett_server.py")


def main():
    env = os.environ.copy()
    if "--no-browser" in sys.argv:
        env["BUFFETT_NO_BROWSER"] = "1"
    while True:
        try:
            subprocess.call([sys.executable, SERVER], env=env)
        except KeyboardInterrupt:
            break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(5)


if __name__ == "__main__":
    main()