from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
import importlib
import re
import sys
from typing import Any
from urllib.parse import urlparse

import requests
import yaml

from .douyin_downloader import DouyinDownloaderError
from .douyin_downloader import ensure_repo, load_cookies

_USER_PATH_RE = re.compile(r"/user/([A-Za-z0-9_-]+)")
_URL_RE = re.compile(r"https?://[^\s]+")
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.douyin.com/",
}


class ProfileResolveError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResolvedAuthorProfile:
    profile_url: str
    sec_uid: str
    douyin_id: str
    name: str
    douyin_id_is_fallback: bool = False


def resolve_author_profile_from_config(
    config_path: str | Path,
    profile_input: str,
) -> ResolvedAuthorProfile:
    try:
        settings = _load_resolver_settings(Path(config_path))
        cookies = load_cookies(settings["cookies_file"])
        return resolve_author_profile(
            profile_input,
            downloader_dir=settings["downloader_dir"],
            cookies=cookies,
        )
    except (OSError, yaml.YAMLError, DouyinDownloaderError) as e:
        raise ProfileResolveError(str(e)) from e


def resolve_author_profile(
    profile_input: str,
    *,
    downloader_dir: str | Path,
    cookies: dict[str, Any] | None = None,
) -> ResolvedAuthorProfile:
    profile_url = _resolve_profile_url(profile_input)
    sec_uid = _extract_sec_uid(profile_url)
    user_info = _fetch_user_info(sec_uid, downloader_dir=Path(downloader_dir), cookies=cookies or {})

    name = str(user_info.get("nickname") or "").strip()
    if not name:
        raise ProfileResolveError("未获取到作者昵称，请手动填写")

    douyin_id, is_fallback = _pick_douyin_id(user_info, sec_uid=sec_uid)
    return ResolvedAuthorProfile(
        profile_url=_build_profile_url(sec_uid),
        sec_uid=sec_uid,
        douyin_id=douyin_id,
        name=name,
        douyin_id_is_fallback=is_fallback,
    )


def _load_resolver_settings(config_path: Path) -> dict[str, Path | None]:
    if not config_path.exists():
        raise ProfileResolveError(f"配置文件不存在: {config_path}")
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ProfileResolveError(f"配置文件格式不正确: {config_path}")

    downloader_raw = data.get("downloader", {}) or {}
    if not isinstance(downloader_raw, dict):
        raise ProfileResolveError("downloader 配置格式不正确")

    downloader_dir = Path(str(downloader_raw.get("douyin_downloader_dir", ".tools/douyin-downloader"))).resolve()
    cookies_file_raw = str(downloader_raw.get("cookies_file", "")).strip()
    cookies_file = Path(cookies_file_raw).resolve() if cookies_file_raw else None
    return {
        "downloader_dir": downloader_dir,
        "cookies_file": cookies_file,
    }


def _extract_first_url(profile_input: str) -> str:
    raw = str(profile_input or "").strip()
    if not raw:
        raise ProfileResolveError("抖音主页链接不能为空")
    match = _URL_RE.search(raw)
    if not match:
        raise ProfileResolveError("未识别到有效链接")
    return match.group(0).rstrip('，。；;）)]}>,')


def _resolve_profile_url(profile_input: str) -> str:
    url = _extract_first_url(profile_input)
    try:
        response = requests.get(
            url,
            headers=_REQUEST_HEADERS,
            timeout=20,
            allow_redirects=True,
        )
    except requests.RequestException as e:
        raise ProfileResolveError(f"解析主页链接失败: {e}") from e

    if response.status_code >= 400:
        raise ProfileResolveError(f"解析主页链接失败（HTTP {response.status_code}）")

    final_url = str(response.url or url).strip()
    if not final_url:
        raise ProfileResolveError("解析主页链接失败：未拿到最终跳转地址")
    return final_url


def _extract_sec_uid(profile_url: str) -> str:
    parsed = urlparse(profile_url)
    match = _USER_PATH_RE.search(parsed.path)
    if not match:
        raise ProfileResolveError("链接不是抖音作者主页，或未识别到 sec_uid")
    return match.group(1)


def _pick_douyin_id(user_info: dict[str, Any], *, sec_uid: str) -> tuple[str, bool]:
    for key in ("unique_id", "short_id", "custom_verify"):
        value = str(user_info.get(key) or "").strip()
        if value:
            return value, False
    return sec_uid, True


def _build_profile_url(sec_uid: str) -> str:
    return f"https://www.douyin.com/user/{sec_uid}"


def _fetch_user_info(
    sec_uid: str,
    *,
    downloader_dir: Path,
    cookies: dict[str, Any],
) -> dict[str, Any]:
    ensure_repo(downloader_dir)
    if str(downloader_dir) not in sys.path:
        sys.path.insert(0, str(downloader_dir))

    try:
        api_module = importlib.import_module("core.api_client")
        client_cls = getattr(api_module, "DouyinAPIClient")
    except Exception as e:
        raise ProfileResolveError(
            "自动获取依赖未安装，请先执行 `pip install -r .tools/douyin-downloader/requirements.txt`，或继续手动填写"
        ) from e

    async def _run() -> dict[str, Any]:
        async with client_cls(cookies or {}) as client:
            data = await client.get_user_info(sec_uid)
            return data or {}

    try:
        user_info = asyncio.run(_run())
    except Exception as e:
        raise ProfileResolveError(f"获取作者信息失败: {e}") from e

    if not user_info:
        raise ProfileResolveError("未获取到作者信息，请确认链接可访问、cookies 可用，或继续手动填写")
    if not isinstance(user_info, dict):
        raise ProfileResolveError("作者信息格式异常，请继续手动填写")
    return user_info
