from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

import yaml


class ConfigError(RuntimeError):
    pass


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"配置文件不存在: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"配置文件格式不正确（需要 YAML mapping）: {path}")
    return data


def _as_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", ""}:
            return False
    raise ConfigError(f"{field} 必须是布尔值（true/false）")


def sanitize_component(value: str) -> str:
    # Make a filesystem-safe, drive-path-safe component.
    value = value.strip()
    value = re.sub(r"[\\\\/]+", "-", value)
    value = re.sub(r"[:*?\"<>|]+", "-", value)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"\.+$", "", value)  # avoid trailing dots on Windows
    return value or "unknown"


@dataclass(frozen=True)
class Author:
    douyin_id: str
    name: str
    profile_url: str

    @property
    def key(self) -> str:
        return sanitize_component(f"{self.douyin_id}-{self.name}")


@dataclass(frozen=True)
class StorageConfig:
    rclone_remote: str
    base_dir: str


@dataclass(frozen=True)
class WecomConfig:
    webhook_url: str
    push_on_first_sync: bool
    max_list_items: int


@dataclass(frozen=True)
class DownloaderConfig:
    douyin_downloader_dir: Path
    cookies_file: Path | None
    thread: int
    retry_times: int
    quiet_logs: bool
    browser_fallback: dict[str, Any]


@dataclass(frozen=True)
class AppConfig:
    authors: list[Author]
    storage: StorageConfig
    wecom: WecomConfig
    downloader: DownloaderConfig
    data_dir: Path


@dataclass(frozen=True)
class AuthorMutationResult:
    author: Author
    created: bool


def _parse_author(item: dict[str, Any]) -> Author:
    douyin_id = str(item.get("douyin_id", "")).strip()
    name = str(item.get("name", "")).strip()
    profile_url = str(item.get("profile_url", "")).strip()
    sec_uid = str(item.get("sec_uid", "")).strip()
    if not profile_url and sec_uid:
        profile_url = f"https://www.douyin.com/user/{sec_uid}"
    if not douyin_id:
        raise ConfigError("authors[].douyin_id 不能为空")
    if not name:
        raise ConfigError("authors[].name 不能为空")
    if not profile_url:
        raise ConfigError("authors[].profile_url 或 authors[].sec_uid 不能为空")
    return Author(douyin_id=douyin_id, name=name, profile_url=profile_url)


def _author_to_dict(author: Author) -> dict[str, str]:
    return {
        "douyin_id": author.douyin_id,
        "name": author.name,
        "profile_url": author.profile_url,
    }


def _save_yaml(path: Path, data: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def list_authors(path: str | Path) -> list[Author]:
    cfg = _read_yaml(Path(path))
    authors_raw = cfg.get("authors", [])
    if authors_raw is None:
        return []
    if not isinstance(authors_raw, list):
        raise ConfigError("authors 必须是列表")

    authors: list[Author] = []
    for item in authors_raw:
        if not isinstance(item, dict):
            raise ConfigError("authors[] 必须是对象")
        authors.append(_parse_author(item))
    return authors


def add_or_update_author(
    path: str | Path,
    *,
    douyin_id: str,
    name: str,
    profile_url: str,
) -> AuthorMutationResult:
    cfg_path = Path(path)
    cfg = _read_yaml(cfg_path)
    authors_raw = cfg.get("authors", [])
    if authors_raw is None:
        authors_raw = []
    if not isinstance(authors_raw, list):
        raise ConfigError("authors 必须是列表")

    incoming = _parse_author(
        {
            "douyin_id": douyin_id,
            "name": name,
            "profile_url": profile_url,
        }
    )

    kept: list[dict[str, Any]] = []
    replaced = False
    for raw in authors_raw:
        if not isinstance(raw, dict):
            raise ConfigError("authors[] 必须是对象")
        author = _parse_author(raw)
        if author.key == incoming.key or author.douyin_id == incoming.douyin_id:
            replaced = True
            continue
        kept.append(raw)

    kept.append(_author_to_dict(incoming))
    cfg["authors"] = kept
    _save_yaml(cfg_path, cfg)
    return AuthorMutationResult(author=incoming, created=not replaced)


def remove_author(path: str | Path, *, author_key: str) -> bool:
    cfg_path = Path(path)
    cfg = _read_yaml(cfg_path)
    authors_raw = cfg.get("authors", [])
    if authors_raw is None:
        authors_raw = []
    if not isinstance(authors_raw, list):
        raise ConfigError("authors 必须是列表")

    kept: list[dict[str, Any]] = []
    removed = False
    for raw in authors_raw:
        if not isinstance(raw, dict):
            raise ConfigError("authors[] 必须是对象")
        author = _parse_author(raw)
        if author.key == author_key:
            removed = True
            continue
        kept.append(raw)

    if not removed:
        return False

    cfg["authors"] = kept
    _save_yaml(cfg_path, cfg)
    return True


def load_config(path: str | Path) -> AppConfig:
    path = Path(path)
    cfg = _read_yaml(path)

    data_dir = Path(str(cfg.get("data_dir", "data"))).resolve()

    authors_raw = cfg.get("authors", [])
    if authors_raw is None:
        authors_raw = []
    if not isinstance(authors_raw, list):
        raise ConfigError("authors 必须是列表")
    authors: list[Author] = []
    for item in authors_raw:
        if not isinstance(item, dict):
            raise ConfigError("authors[] 必须是对象")
        authors.append(_parse_author(item))

    storage_raw = cfg.get("storage", {}) or {}
    if not isinstance(storage_raw, dict):
        raise ConfigError("storage 必须是对象")
    rclone_remote = str(storage_raw.get("rclone_remote", "")).strip()
    base_dir = str(storage_raw.get("base_dir", "")).strip()
    if not rclone_remote:
        raise ConfigError("storage.rclone_remote 不能为空（例如: gdrive）")
    if not base_dir:
        raise ConfigError("storage.base_dir 不能为空（例如: DouyinArchive）")
    storage = StorageConfig(rclone_remote=rclone_remote, base_dir=base_dir)

    wecom_raw = cfg.get("wecom", {}) or {}
    if not isinstance(wecom_raw, dict):
        raise ConfigError("wecom 必须是对象")
    webhook_url = str(wecom_raw.get("webhook_url", "")).strip()
    push_on_first_sync = _as_bool(
        wecom_raw.get("push_on_first_sync", False),
        field="wecom.push_on_first_sync",
    )
    max_list_items = int(wecom_raw.get("max_list_items", 20))
    wecom = WecomConfig(
        webhook_url=webhook_url,
        push_on_first_sync=push_on_first_sync,
        max_list_items=max_list_items,
    )

    downloader_raw = cfg.get("downloader", {}) or {}
    if not isinstance(downloader_raw, dict):
        raise ConfigError("downloader 必须是对象")
    douyin_downloader_dir = Path(
        str(downloader_raw.get("douyin_downloader_dir", ".tools/douyin-downloader"))
    ).resolve()
    cookies_file_raw = str(downloader_raw.get("cookies_file", "")).strip()
    cookies_file = Path(cookies_file_raw).resolve() if cookies_file_raw else None
    thread = int(downloader_raw.get("thread", 5))
    retry_times = int(downloader_raw.get("retry_times", 3))
    progress_raw = downloader_raw.get("progress", {}) or {}
    if not isinstance(progress_raw, dict):
        raise ConfigError("downloader.progress 必须是对象")
    quiet_logs = _as_bool(
        progress_raw.get("quiet_logs", True),
        field="downloader.progress.quiet_logs",
    )
    browser_fallback = downloader_raw.get("browser_fallback", {}) or {}
    if not isinstance(browser_fallback, dict):
        raise ConfigError("downloader.browser_fallback 必须是对象")
    downloader = DownloaderConfig(
        douyin_downloader_dir=douyin_downloader_dir,
        cookies_file=cookies_file,
        thread=thread,
        retry_times=retry_times,
        quiet_logs=quiet_logs,
        browser_fallback=browser_fallback,
    )

    return AppConfig(
        authors=authors,
        storage=storage,
        wecom=wecom,
        downloader=downloader,
        data_dir=data_dir,
    )
