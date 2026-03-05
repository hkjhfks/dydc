from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import argparse
import datetime as dt
import shutil
import sys
import time

from .config import AppConfig, ConfigError, load_config
from .douyin_downloader import (
    DouyinDownloaderError,
    discover_video_items,
    ensure_repo,
    find_mp4_files,
    load_cookies,
    build_config as build_downloader_config,
    run_downloader,
    write_config as write_downloader_config,
)
from .rclone import RcloneError, RcloneStorage
from .state import load_state, save_state
from .web_server import run_web_server
from .wecom import WecomMessage, send_markdown


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


@dataclass(frozen=True)
class RunContext:
    cfg: AppConfig
    state_path: Path


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="douyin_check_pipeline")
    p.add_argument("-c", "--config", default="pipeline.yml", help="配置文件路径（YAML）")
    p.add_argument("--doctor", action="store_true", help="检查依赖与配置并退出")
    p.add_argument("--web", action="store_true", help="启动前端控制台与作者管理 API")
    p.add_argument("--host", default="127.0.0.1", help="Web 服务监听地址（默认 127.0.0.1）")
    p.add_argument("--port", type=int, default=8765, help="Web 服务端口（默认 8765）")
    p.add_argument(
        "--web-interval-min",
        type=int,
        default=55 * 60,
        help="Web 后台监控最小间隔秒（默认 3300，即 55 分钟）",
    )
    p.add_argument(
        "--web-interval-max",
        type=int,
        default=65 * 60,
        help="Web 后台监控最大间隔秒（默认 3900，即 65 分钟）",
    )
    p.add_argument("--once", action="store_true", help="只执行一轮（默认）")
    p.add_argument("--loop", action="store_true", help="循环监控")
    p.add_argument(
        "--interval",
        type=int,
        default=600,
        help="CLI 循环模式间隔秒（默认 600）",
    )
    p.add_argument("--fail-fast", action="store_true", help="循环模式下遇到失败立即退出")
    p.add_argument("--dry-run", action="store_true", help="只打印，不下载/不上传/不推送")
    return p.parse_args(argv)


def _ensure_dirs(cfg: AppConfig) -> None:
    cfg.data_dir.mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "downloads").mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "tmp").mkdir(parents=True, exist_ok=True)
    (cfg.data_dir / "state").mkdir(parents=True, exist_ok=True)


def doctor(cfg: AppConfig) -> int:
    issues = 0
    print("== doctor ==")
    print(f"- data_dir: {cfg.data_dir}")
    print(f"- authors: {len(cfg.authors)}")
    if not cfg.wecom.webhook_url:
        print("- wecom.webhook_url: (empty) 推送将被跳过")

    try:
        run_py = ensure_repo(cfg.downloader.douyin_downloader_dir)
        print(f"- douyin-downloader: OK ({run_py})")
    except DouyinDownloaderError as e:
        issues += 1
        print(f"- douyin-downloader: FAIL ({e})")

    if cfg.downloader.cookies_file:
        try:
            _ = load_cookies(cfg.downloader.cookies_file)
            print(f"- cookies_file: OK ({cfg.downloader.cookies_file})")
        except DouyinDownloaderError as e:
            issues += 1
            print(f"- cookies_file: FAIL ({e})")
    else:
        print("- cookies_file: (not set) 可能只能抓到少量作品")

    try:
        storage = RcloneStorage(remote=cfg.storage.rclone_remote, base_dir=cfg.storage.base_dir)
        storage.check_remote()
        print("- rclone: OK")
    except Exception as e:
        issues += 1
        print(f"- rclone: FAIL ({e})")

    return 2 if issues else 0


def _remove_local_video_dir(video_dir: Path, *, stop_dir: Path) -> None:
    if video_dir.exists():
        shutil.rmtree(video_dir)

    stop_dir = stop_dir.resolve()
    cursor = video_dir.parent
    while True:
        if not cursor.exists():
            break
        if cursor == stop_dir:
            break
        if cursor == cursor.parent:
            break
        if any(cursor.iterdir()):
            break
        cursor.rmdir()
        cursor = cursor.parent


def run_once(ctx: RunContext, *, dry_run: bool) -> int:
    state = load_state(ctx.state_path)
    storage = RcloneStorage(
        remote=ctx.cfg.storage.rclone_remote,
        base_dir=ctx.cfg.storage.base_dir,
    )

    failures = 0
    try:
        run_py = ensure_repo(ctx.cfg.downloader.douyin_downloader_dir)
        cookies = load_cookies(ctx.cfg.downloader.cookies_file)
    except DouyinDownloaderError as e:
        print(str(e), file=sys.stderr)
        return 2

    for author in ctx.cfg.authors:
        author_is_new = author.key not in state.authors
        author_out_dir = ctx.cfg.data_dir / "downloads" / author.key
        author_out_dir.mkdir(parents=True, exist_ok=True)
        author_remote_root = storage.remote_path(author.key)

        try:
            storage.mkdir(author_remote_root, dry_run=dry_run)

            downloader_cfg = build_downloader_config(
                author=author,
                output_dir=author_out_dir,
                downloader_cfg=ctx.cfg.downloader,
                cookies=cookies,
            )
            cfg_path = ctx.cfg.data_dir / "tmp" / "downloader-configs" / f"{author.key}.yml"
            write_downloader_config(downloader_cfg, cfg_path)

            if dry_run:
                print(f"[dry-run] {author.key}: 将执行下载: {author.profile_url}")
            else:
                print(f"{author.key}: 下载检查中…")
                run_downloader(run_py, cfg_path)

            items = discover_video_items(author_out_dir)
            new_items = [it for it in items if not state.is_uploaded(author.key, it.aweme_id)]
            if not new_items:
                continue

            uploaded = []
            for it in new_items:
                mp4_files = find_mp4_files(it.video_dir)
                if not mp4_files:
                    continue
                dest_dir = author_remote_root
                if dry_run:
                    names = ", ".join([p.name for p in mp4_files])
                    print(f"[dry-run] {author.key}: 将上传 {names} -> {dest_dir}")
                else:
                    storage.copy_mp4_dir(it.video_dir, dest_dir, dry_run=False)
                    try:
                        _remove_local_video_dir(it.video_dir, stop_dir=author_out_dir)
                    except OSError as e:
                        print(f"{author.key}: 本地清理失败（{it.video_dir}）: {e}", file=sys.stderr)
                    state.mark_uploaded(
                        author.key,
                        it.aweme_id,
                        drive_path=dest_dir,
                        video_url=it.video_url,
                        local_dir=str(it.video_dir),
                        uploaded_at=utc_now_iso(),
                    )
                uploaded.append(it)

            if uploaded:
                if author_is_new and not ctx.cfg.wecom.push_on_first_sync:
                    msg = WecomMessage(
                        title="抖音作者已同步",
                        lines=[
                            f"作者：{author.douyin_id} - {author.name}",
                            f"已同步作品：{len(uploaded)}",
                            f"云盘目录：{author_remote_root}",
                            "",
                            "说明：首次同步不逐条推送作品链接；后续新作品会自动推送。",
                        ],
                    ).to_markdown()
                else:
                    max_items = max(1, int(ctx.cfg.wecom.max_list_items))
                    shown = uploaded[:max_items]
                    extra = len(uploaded) - len(shown)
                    lines = [
                        f"作者：{author.douyin_id} - {author.name}",
                        f"新增作品：{len(uploaded)}",
                        f"云盘目录：{author_remote_root}",
                        "",
                        *[f"- {it.title} {it.video_url}" for it in shown],
                    ]
                    if extra > 0:
                        lines.append(f"... 还有 {extra} 条未列出")
                    msg = WecomMessage(title="抖音作者更新", lines=lines).to_markdown()
                if dry_run:
                    print(f"[dry-run] {author.key}: 将推送企业微信通知（{len(msg)} chars）")
                else:
                    send_markdown(ctx.cfg.wecom.webhook_url, msg)

        except (RcloneError, DouyinDownloaderError, Exception) as e:
            failures += 1
            print(f"{author.key}: 失败: {e}", file=sys.stderr)

    if not dry_run:
        save_state(state)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        cfg = load_config(args.config)
    except ConfigError as e:
        print(f"配置错误: {e}", file=sys.stderr)
        return 2

    _ensure_dirs(cfg)
    if args.doctor:
        return doctor(cfg)
    state_path = cfg.data_dir / "state" / "state.json"
    ctx = RunContext(cfg=cfg, state_path=state_path)

    if args.web:
        return run_web_server(
            config_path=args.config,
            state_path=state_path,
            host=args.host,
            port=args.port,
            min_interval_seconds=max(1, int(args.web_interval_min)),
            max_interval_seconds=max(1, int(args.web_interval_max)),
        )

    if args.loop:
        while True:
            rc = run_once(ctx, dry_run=args.dry_run)
            if rc != 0 and args.fail_fast:
                return rc
            if rc != 0:
                print(
                    f"本轮执行失败（rc={rc}），{max(1, int(args.interval))} 秒后重试。"
                    "如需失败即退出，请使用 --fail-fast。",
                    file=sys.stderr,
                )
            time.sleep(max(1, int(args.interval)))

    return run_once(ctx, dry_run=args.dry_run)
