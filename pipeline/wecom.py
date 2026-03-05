from __future__ import annotations

from dataclasses import dataclass
import textwrap
from typing import Iterable

import requests


@dataclass(frozen=True)
class WecomMessage:
    title: str
    lines: list[str]

    def to_markdown(self) -> str:
        # WeCom group bot supports a limited Markdown subset.
        body = "\n".join(self.lines).strip()
        if self.title:
            return f"## {self.title}\n{body}".strip()
        return body


def _chunks(s: str, *, max_len: int) -> Iterable[str]:
    if len(s) <= max_len:
        yield s
        return
    for part in textwrap.wrap(s, width=max_len, break_long_words=False, break_on_hyphens=False):
        if part.strip():
            yield part


def send_markdown(webhook_url: str, content: str, *, timeout_seconds: int = 10) -> None:
    if not webhook_url:
        return

    for chunk in _chunks(content, max_len=3500):
        payload = {"msgtype": "markdown", "markdown": {"content": chunk}}
        resp = requests.post(webhook_url, json=payload, timeout=timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        # {"errcode":0,"errmsg":"ok"} means success.
        errcode = int(data.get("errcode", -1))
        if errcode != 0:
            raise RuntimeError(f"企业微信 webhook 推送失败: {data}")
