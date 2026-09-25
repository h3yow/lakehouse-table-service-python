"""本地湖仓表服务入口。

环境变量 LAKEHOUSE_ROOT 指定数据根目录（默认 ./lakehouse_data）。
"""

from __future__ import annotations

import os

import uvicorn

from lakehouse.api import create_app

app = create_app(os.environ.get("LAKEHOUSE_ROOT", "./lakehouse_data"))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(os.environ.get("PORT", "8000")))
