from __future__ import annotations

import os

from waitress import serve

from dayingjing import create_app


def main() -> None:
    # 本地默认监听 127.0.0.1，部署到容器或服务器时改为 0.0.0.0。
    host = os.environ.get("HOST", "127.0.0.1")
    port = int(os.environ.get("PORT", "8787"))
    threads = int(os.environ.get("WAITRESS_THREADS", "8"))
    app = create_app()
    serve(app, host=host, port=port, threads=threads)


if __name__ == "__main__":
    main()
