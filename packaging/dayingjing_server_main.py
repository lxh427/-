from __future__ import annotations

import os

from server import main


def run() -> None:
    # 安装版默认只在本机监听，避免误暴露到局域网。
    os.environ.setdefault("HOST", "127.0.0.1")
    os.environ.setdefault("PORT", "8787")
    os.environ.setdefault("WAITRESS_THREADS", "8")
    main()


if __name__ == "__main__":
    run()
