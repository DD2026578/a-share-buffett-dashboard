#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""跨进程非阻塞文件锁。

服务进程、watchdog、scripts/fetch_once.py 可能同时触发刷新；
用锁文件串行化，避免同一秒重复写快照或写坏 JSON/JS 文件。
"""

import contextlib
import os


class LockBusy(Exception):
    """锁已被其他进程占用。"""


def _try_lock(fd):
    try:
        import msvcrt
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        return "msvcrt"
    except ImportError:
        import fcntl
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return "fcntl"
        except OSError:
            raise LockBusy()
    except OSError:
        raise LockBusy()


def _unlock(fd, kind):
    if kind == "msvcrt":
        import msvcrt
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(fd, fcntl.LOCK_UN)


@contextlib.contextmanager
def file_lock(path):
    f = open(path, "a+")
    try:
        f.seek(0, os.SEEK_END)
        if f.tell() == 0:
            f.write("0")
            f.flush()
        f.seek(0)
        kind = _try_lock(f.fileno())
        try:
            yield
        finally:
            f.seek(0)
            _unlock(f.fileno(), kind)
    finally:
        f.close()
