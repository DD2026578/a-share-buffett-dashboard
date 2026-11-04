#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把看板构建成 GitHub Pages 可用的静态目录 docs/。"""

import os
import shutil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")
PAGE = "A股巴菲特指标仪表盘.html"

os.makedirs(DOCS, exist_ok=True)

shutil.copyfile(os.path.join(ROOT, PAGE), os.path.join(DOCS, "index.html"))
for name in ("data.js", "indices.js"):
    src = os.path.join(ROOT, name)
    if os.path.exists(src):
        shutil.copyfile(src, os.path.join(DOCS, name))

with open(os.path.join(DOCS, ".nojekyll"), "w", encoding="utf-8") as f:
    f.write("")

print("built docs/ with", len(os.listdir(DOCS)), "files")