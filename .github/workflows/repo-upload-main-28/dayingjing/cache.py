from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class CacheEntry:
    expires_at: datetime
    payload: Any


class CacheStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._memory: dict[tuple[str, str], CacheEntry] = {}

    def _namespace_dir(self, namespace: str) -> Path:
        path = self.root / namespace
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _file_path(self, namespace: str, key: str) -> Path:
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self._namespace_dir(namespace) / f"{digest}.json"

    def get(self, namespace: str, key: str) -> Any | None:
        cache_key = (namespace, key)
        now = datetime.now()
        entry = self._memory.get(cache_key)
        if entry and entry.expires_at > now:
            return copy.deepcopy(entry.payload)

        path = self._file_path(namespace, key)
        if not path.exists():
            return None

        try:
            raw = json.loads(path.read_text("utf-8"))
            expires_at = datetime.fromisoformat(raw["expiresAt"])
            if expires_at <= now:
                path.unlink(missing_ok=True)
                return None
            payload = raw["payload"]
        except Exception:
            path.unlink(missing_ok=True)
            return None

        self._memory[cache_key] = CacheEntry(expires_at=expires_at, payload=payload)
        return copy.deepcopy(payload)

    def set(self, namespace: str, key: str, payload: Any, expires_at: datetime) -> None:
        safe_payload = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
        cache_key = (namespace, key)
        self._memory[cache_key] = CacheEntry(expires_at=expires_at, payload=safe_payload)
        path = self._file_path(namespace, key)
        entry = {
            "expiresAt": expires_at.isoformat(),
            "payload": safe_payload,
        }
        path.write_text(json.dumps(entry, ensure_ascii=False, indent=2), "utf-8")
