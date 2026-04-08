from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path


HOST = "127.0.0.1"
PORT = int(os.environ.get("PORT", "8787"))
URL = f"http://{HOST}:{PORT}/"
HEALTH_URL = f"{URL}api/health"
APP_TITLE = "达盈镜v1.0"


def _app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _server_command() -> list[str]:
    app_dir = _app_dir()
    if getattr(sys, "frozen", False):
        candidates = [
            app_dir / "DaYingJingServer.exe",
            app_dir / "DaYingJingServer",
            app_dir.parent / "Resources" / "DaYingJingServer.exe",
            app_dir.parent / "Resources" / "DaYingJingServer",
        ]
        for server_exe in candidates:
            if server_exe.exists():
                return [str(server_exe)]
        raise FileNotFoundError("server executable not found in app directory or bundle resources")
    return [sys.executable, str(app_dir / "dayingjing_server_main.py")]


def _health_ok() -> bool:
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=2) as response:
            return response.status == 200
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def _show_error(message: str) -> None:
    try:
        import tkinter
        from tkinter import messagebox

        root = tkinter.Tk()
        root.withdraw()
        messagebox.showerror(APP_TITLE, message)
        root.destroy()
    except Exception:
        print(message)


def _start_server() -> None:
    env = os.environ.copy()
    env.setdefault("HOST", HOST)
    env.setdefault("PORT", str(PORT))
    env.setdefault("WAITRESS_THREADS", "8")

    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_CONSOLE
    else:
        start_new_session = True

    subprocess.Popen(
        _server_command(),
        cwd=str(_app_dir()),
        env=env,
        creationflags=creationflags,
        start_new_session=start_new_session,
    )


def _wait_until_ready(timeout_seconds: int = 35) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _health_ok():
            return True
        time.sleep(0.5)
    return False


def main() -> None:
    if not _health_ok():
        try:
            _start_server()
        except Exception as exc:
            _show_error(f"启动达盈镜服务失败：{exc}")
            raise SystemExit(1) from exc

        if not _wait_until_ready():
            _show_error("达盈镜服务启动超时，请检查网络或重新打开程序。")
            raise SystemExit(1)

    webbrowser.open(URL)


if __name__ == "__main__":
    main()
