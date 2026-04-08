from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import json
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter

from .config import WEB_HEADERS


class HttpClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.headers.update(WEB_HEADERS)
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self._curl_available = shutil.which("curl.exe") is not None

    def should_prefer_curl(self, url: str) -> bool:
        url = (url or "").lower()
        return any(
            host in url
            for host in (
                "nmpa.gov.cn",
                "nhsa.gov.cn",
                "miit.gov.cn",
                "gov.cn",
                "csrc.gov.cn",
                "pbc.gov.cn",
            )
        )

    def get_text(
        self,
        url: str,
        referer: str = "",
        timeout: int = 10,
        retries: int = 2,
        allow_curl_fallback: bool = True,
        prefer_curl: bool | None = None,
    ) -> str | None:
        should_use_curl = self.should_prefer_curl(url) if prefer_curl is None else prefer_curl
        if should_use_curl:
            text = self._curl_text(url, referer=referer, connect_timeout=min(6, timeout), max_time=max(timeout, timeout + 2))
            if text is not None:
                return text

        for _ in range(max(1, retries)):
            try:
                response = self.session.get(
                    url,
                    headers={"Referer": referer} if referer else None,
                    timeout=timeout,
                )
                response.raise_for_status()
                encoding = (response.encoding or "").lower()
                apparent = response.apparent_encoding or ""
                if apparent and (not encoding or encoding in {"iso-8859-1", "latin-1", "ascii"}):
                    response.encoding = apparent
                elif not response.encoding:
                    response.encoding = apparent or "utf-8"
                return response.text
            except Exception:
                continue

        env_text = self._requests_env_text(url, referer=referer, timeout=timeout)
        if env_text is not None:
            return env_text

        if not allow_curl_fallback:
            return None
        return self._curl_text(url, referer=referer, connect_timeout=min(6, timeout), max_time=max(timeout, timeout + 2))

    def get_bytes(
        self,
        url: str,
        referer: str = "",
        timeout: int = 12,
        retries: int = 2,
        allow_curl_fallback: bool = True,
    ) -> bytes | None:
        for _ in range(max(1, retries)):
            try:
                response = self.session.get(
                    url,
                    headers={"Referer": referer} if referer else None,
                    timeout=timeout,
                )
                response.raise_for_status()
                return response.content
            except Exception:
                continue
        env_bytes = self._requests_env_bytes(url, referer=referer, timeout=timeout)
        if env_bytes is not None:
            return env_bytes
        if not allow_curl_fallback:
            return None
        return self._curl_bytes(url, referer=referer, connect_timeout=min(6, timeout), max_time=max(timeout, timeout + 2))

    def get_json(
        self,
        url: str,
        referer: str = "",
        timeout: int = 10,
        retries: int = 2,
        allow_curl_fallback: bool = True,
        prefer_curl: bool | None = None,
    ) -> Any:
        text = self.get_text(
            url,
            referer=referer,
            timeout=timeout,
            retries=retries,
            allow_curl_fallback=allow_curl_fallback,
            prefer_curl=prefer_curl,
        )
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {}

    def post_form_text(self, url: str, form: dict[str, Any], referer: str = "", timeout: int = 12, retries: int = 1) -> str | None:
        headers = {"X-Requested-With": "XMLHttpRequest"}
        if referer:
            headers["Referer"] = referer
        for _ in range(max(1, retries)):
            try:
                response = self.session.post(url, data=form, headers=headers, timeout=timeout)
                response.raise_for_status()
                encoding = (response.encoding or "").lower()
                apparent = response.apparent_encoding or ""
                if apparent and (not encoding or encoding in {"iso-8859-1", "latin-1", "ascii"}):
                    response.encoding = apparent
                elif not response.encoding:
                    response.encoding = apparent or "utf-8"
                return response.text
            except Exception:
                continue
        return None

    def post_form_json(self, url: str, form: dict[str, Any], referer: str = "", timeout: int = 12, retries: int = 1) -> Any:
        text = self.post_form_text(url, form=form, referer=referer, timeout=timeout, retries=retries)
        if not text:
            return {}
        try:
            return json.loads(text)
        except Exception:
            return {}

    def _curl_text(self, url: str, referer: str = "", connect_timeout: int = 6, max_time: int = 12) -> str | None:
        result = self._curl_fetch(url, referer=referer, connect_timeout=connect_timeout, max_time=max_time)
        if result is None:
            return None
        data, _ = result
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            for encoding in ("gb18030", "gbk", "utf-8", "latin-1"):
                try:
                    return data.decode(encoding)
                except UnicodeDecodeError:
                    continue
        return data.decode("utf-8", errors="ignore")

    def _requests_env_text(self, url: str, referer: str = "", timeout: int = 10) -> str | None:
        try:
            response = requests.get(
                url,
                headers={"Referer": referer, **WEB_HEADERS} if referer else dict(WEB_HEADERS),
                timeout=timeout,
            )
            response.raise_for_status()
            encoding = (response.encoding or "").lower()
            apparent = response.apparent_encoding or ""
            if apparent and (not encoding or encoding in {"iso-8859-1", "latin-1", "ascii"}):
                response.encoding = apparent
            elif not response.encoding:
                response.encoding = apparent or "utf-8"
            return response.text
        except Exception:
            return None

    def _requests_env_bytes(self, url: str, referer: str = "", timeout: int = 12) -> bytes | None:
        try:
            response = requests.get(
                url,
                headers={"Referer": referer, **WEB_HEADERS} if referer else dict(WEB_HEADERS),
                timeout=timeout,
            )
            response.raise_for_status()
            return response.content
        except Exception:
            return None

    def _curl_bytes(self, url: str, referer: str = "", connect_timeout: int = 6, max_time: int = 12) -> bytes | None:
        result = self._curl_fetch(url, referer=referer, connect_timeout=connect_timeout, max_time=max_time)
        if result is None:
            return None
        return result[0]

    def _curl_fetch(self, url: str, referer: str = "", connect_timeout: int = 6, max_time: int = 12) -> tuple[bytes, str] | None:
        if not self._curl_available:
            return None

        body_fd, body_path = tempfile.mkstemp(prefix="dayingjing-curl-", suffix=".bin")
        headers_fd, headers_path = tempfile.mkstemp(prefix="dayingjing-curl-", suffix=".headers")
        os.close(body_fd)
        os.close(headers_fd)
        tmp_body = Path(body_path)
        tmp_headers = Path(headers_path)
        try:
            args = [
                "curl.exe",
                "--http1.1",
                "--tlsv1.2",
                "-L",
                "--compressed",
                "-A",
                WEB_HEADERS["User-Agent"],
                "-H",
                f"Accept-Language: {WEB_HEADERS['Accept-Language']}",
                "--connect-timeout",
                str(max(1, int(connect_timeout))),
                "--max-time",
                str(max(2, int(max_time))),
                "-s",
                "-S",
                "-D",
                str(tmp_headers),
                "-o",
                str(tmp_body),
            ]
            if referer:
                args.extend(["-e", referer])
            args.append(url)
            curl_env = {
                key: value
                for key, value in os.environ.items()
                if key.upper() not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"}
            }
            curl_env["NO_PROXY"] = "*"
            completed = subprocess.run(args, capture_output=True, check=False, env=curl_env)
            if completed.returncode != 0 or not tmp_body.exists():
                return None
            data = tmp_body.read_bytes()
            headers = tmp_headers.read_text("utf-8", errors="ignore")
            return data, headers
        except Exception:
            return None
        finally:
            tmp_body.unlink(missing_ok=True)
            tmp_headers.unlink(missing_ok=True)
