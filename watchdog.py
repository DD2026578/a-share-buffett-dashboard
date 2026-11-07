#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看板守护进程：服务异常退出后 5 秒自动重启。"""

import os
import subprocess
import sys
import time

import runtime_lock

BASE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(BASE, "buffett_server.py")
WATCHDOG_LOCK = os.path.join(BASE, "watchdog.lock")


def main():
    env = os.environ.copy()
    if "--no-browser" in sys.argv:
        env["BUFFETT_NO_BROWSER"] = "1"
    # 只允许一个守护进程，避免重复双击启动后互相拉起多个服务
    try:
        with runtime_lock.file_lock(WATCHDOG_LOCK):
            while True:
                try:
                    subprocess.call([sys.executable, SERVER], env=env)
                except KeyboardInterrupt:
                    break
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(5)
    except runtime_lock.LockBusy:
        print("守护进程已在运行，本次启动退出", flush=True)


if __name__ == "__main__":
    main()
