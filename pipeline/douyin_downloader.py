from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

import yaml

from .config import Author, DownloaderConfig


class DouyinDownloaderError(RuntimeError):
    pass


@dataclass(frozen=True)
class VideoItem:
    aweme_id: str
    title: str
    date: str | None
    video_dir: Path

    @property
    def video_url(self) -> str:
        return f"https://www.douyin.com/video/{self.aweme_id}"


def _load_yaml_file(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise DouyinDownloaderError(f"YAML 格式不正确: {path}")
    return data


def ensure_repo(downloader_dir: Path) -> Path:
    run_py = downloader_dir / "run.py"
    if not run_py.exists():
        raise DouyinDownloaderError(
            "未找到 douyin-downloader。\n"
            f"- 期望路径: {downloader_dir}\n"
            "- 解决: git clone https://github.com/jiji262/douyin-downloader .tools/douyin-downloader\n"
            "- 然后安装依赖: pip install -r .tools/douyin-downloader/requirements.txt\n"
        )
    return run_py


def load_cookies(cookies_file: Path | None) -> dict[str, Any]:
    if not cookies_file:
        return {}
    if not cookies_file.exists():
        raise DouyinDownloaderError(f"cookies_file 不存在: {cookies_file}")
    data = _load_yaml_file(cookies_file)
    cookies = data.get("cookies", data)
    if not isinstance(cookies, dict):
        raise DouyinDownloaderError(f"cookies_file 内容不正确（需要 cookies mapping）: {cookies_file}")
    return cookies


def build_config(
    *,
    author: Author,
    output_dir: Path,
    downloader_cfg: DownloaderConfig,
    cookies: dict[str, Any],
) -> dict[str, Any]:
    # Minimal stable config for V2.0 main branch.
    number = {"post": 0, "like": 0, "allmix": 0, "mix": 0, "music": 0}
    increase = {"post": True, "like": False, "allmix": False, "mix": False, "music": False}

    browser_fallback = {
        "enabled": True,
        "headless": False,
        "max_scrolls": 240,
        "idle_rounds": 8,
        "wait_timeout_seconds": 600,
    }
    # Allow user overrides from pipeline.yml
    browser_fallback.update(downloader_cfg.browser_fallback or {})

    cfg: dict[str, Any] = {
        "link": [author.profile_url],
        "path": str(output_dir),
        # Keep only the essentials by default: mp4 + metadata.
        "music": False,
        "cover": False,
        "avatar": False,
        "json": True,
        "start_time": "",
        "end_time": "",
        "folderstyle": True,
        "mode": ["post"],
        "number": number,
        "increase": increase,
        "thread": downloader_cfg.thread,
        "retry_times": downloader_cfg.retry_times,
        "database": True,
        "progress": {"quiet_logs": downloader_cfg.quiet_logs},
        "browser_fallback": browser_fallback,
        "cookies": cookies,
    }
    return cfg


def write_config(config: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, allow_unicode=True, sort_keys=False), encoding="utf-8")


def run_downloader(run_py: Path, config_path: Path, *, python: str | None = None) -> None:
    python_bin = python or sys.executable
    cmd = [python_bin, str(run_py), "-c", str(config_path)]
    # douyin-downloader 的 run.py 末尾会询问是否转录；自动化场景下关闭 stdin 避免阻塞。
    proc = subprocess.run(cmd, cwd=str(run_py.parent), text=True, stdin=subprocess.DEVNULL)
    if proc.returncode != 0:
        raise DouyinDownloaderError(f"douyin-downloader 执行失败（exit={proc.returncode}）: {' '.join(cmd)}")


_VIDEO_DIR_RE = re.compile(r"_(\d{6,})$")
_DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_video_dir_name(name: str) -> tuple[str, str, str | None] | None:
    # Expected: YYYY-MM-DD_<title>_<aweme_id>
    m = _VIDEO_DIR_RE.search(name)
    if not m:
        return None
    aweme_id = m.group(1)
    left = name[: m.start()]
    date: str | None = None
    title = left
    parts = left.split("_", 1)
    if len(parts) == 2 and _DATE_PREFIX_RE.match(parts[0]):
        date = parts[0]
        title = parts[1]
    title = title.strip() or aweme_id
    return aweme_id, title, date


def discover_video_items(output_dir: Path) -> list[VideoItem]:
    items: list[VideoItem] = []
    if not output_dir.exists():
        return items

    # Layout: <output_dir>/download_manifest.jsonl + <author_name>/post/<video_dir>/
    author_dirs = [p for p in output_dir.iterdir() if p.is_dir()]
    for author_dir in author_dirs:
        post_dir = author_dir / "post"
        if not post_dir.is_dir():
            continue
        for video_dir in post_dir.iterdir():
            if not video_dir.is_dir():
                continue
            parsed = _parse_video_dir_name(video_dir.name)
            if not parsed:
                continue
            aweme_id, title, date = parsed
            items.append(VideoItem(aweme_id=aweme_id, title=title, date=date, video_dir=video_dir))

    # Stable order: older first by directory name
    items.sort(key=lambda x: x.video_dir.name)
    return items


def find_mp4_files(video_dir: Path) -> list[Path]:
    return sorted([p for p in video_dir.iterdir() if p.is_file() and p.suffix.lower() == ".mp4"])
