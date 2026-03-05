from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json


STATE_VERSION = 1


@dataclass
class State:
    path: Path
    data: dict[str, Any]

    @property
    def authors(self) -> dict[str, Any]:
        return self.data.setdefault("authors", {})

    def is_uploaded(self, author_key: str, aweme_id: str) -> bool:
        author = self.authors.get(author_key, {})
        uploaded = author.get("uploaded", {})
        return str(aweme_id) in uploaded

    def mark_uploaded(
        self,
        author_key: str,
        aweme_id: str,
        *,
        drive_path: str,
        video_url: str,
        local_dir: str,
        uploaded_at: str,
    ) -> None:
        author = self.authors.setdefault(author_key, {})
        uploaded = author.setdefault("uploaded", {})
        uploaded[str(aweme_id)] = {
            "drive_path": drive_path,
            "video_url": video_url,
            "local_dir": local_dir,
            "uploaded_at": uploaded_at,
        }

    def remove_author(self, author_key: str) -> bool:
        if author_key not in self.authors:
            return False
        del self.authors[author_key]
        return True


def load_state(path: str | Path) -> State:
    path = Path(path)
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            data = {}
    else:
        data = {}

    version = int(data.get("version", 0) or 0)
    if version not in (0, STATE_VERSION):
        raise RuntimeError(f"不支持的 state 版本: {version}（期望 {STATE_VERSION}）")

    if version == 0:
        data["version"] = STATE_VERSION
        data.setdefault("authors", {})

    return State(path=path, data=data)


def save_state(state: State) -> None:
    state.path.parent.mkdir(parents=True, exist_ok=True)
    state.path.write_text(
        json.dumps(state.data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
