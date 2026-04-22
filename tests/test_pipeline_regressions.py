from __future__ import annotations

import json
from pathlib import Path
import subprocess
import tempfile
import textwrap
import time
import unittest
from unittest.mock import patch

from pipeline.config import (
    AppConfig,
    Author,
    add_or_update_author,
    ConfigError,
    DownloaderConfig,
    list_authors,
    remove_author,
    StorageConfig,
    WecomConfig,
    load_config,
    sanitize_component,
)
from pipeline.douyin_downloader import VideoItem, _parse_video_dir_name
from pipeline.profile_resolver import (
    ProfileResolveError,
    _extract_first_url,
    _extract_sec_uid,
    _pick_douyin_id,
    resolve_author_profile,
)
from pipeline.runner import RunContext, run_once
from pipeline.runner import doctor, main
from pipeline.web_server import MonitorScheduler


class ParsingRegressionTests(unittest.TestCase):
    def test_parse_video_dir_name_works_for_standard_format(self) -> None:
        parsed = _parse_video_dir_name("2024-02-07_作品标题_7600224486650121526")
        self.assertEqual(
            parsed,
            ("7600224486650121526", "作品标题", "2024-02-07"),
        )

    def test_sanitize_component_normalizes_spaces_and_trailing_dots(self) -> None:
        self.assertEqual(sanitize_component("  a   b...  "), "a b")

    def test_extract_first_url_supports_share_text(self) -> None:
        text = "7.82 复制此链接，打开抖音搜索，直接观看视频 https://v.douyin.com/abc123/ "
        self.assertEqual(_extract_first_url(text), "https://v.douyin.com/abc123/")

    def test_extract_sec_uid_reads_user_path(self) -> None:
        self.assertEqual(
            _extract_sec_uid("https://www.douyin.com/user/MS4wLjABAAAAxyz?from_tab_name=main"),
            "MS4wLjABAAAAxyz",
        )

    def test_pick_douyin_id_prefers_unique_id(self) -> None:
        douyin_id, is_fallback = _pick_douyin_id(
            {"unique_id": "tester001", "short_id": "123"},
            sec_uid="sec_uid_x",
        )
        self.assertEqual(douyin_id, "tester001")
        self.assertFalse(is_fallback)

    def test_pick_douyin_id_falls_back_to_sec_uid(self) -> None:
        douyin_id, is_fallback = _pick_douyin_id({}, sec_uid="sec_uid_x")
        self.assertEqual(douyin_id, "sec_uid_x")
        self.assertTrue(is_fallback)


class ProfileResolverTests(unittest.TestCase):
    def test_resolve_author_profile_uses_fetched_user_info(self) -> None:
        with (
            patch(
                "pipeline.profile_resolver._resolve_profile_url",
                return_value="https://www.douyin.com/user/sec_uid_x",
            ),
            patch(
                "pipeline.profile_resolver._fetch_user_info",
                return_value={"nickname": "测试作者", "unique_id": "tester001"},
            ),
        ):
            result = resolve_author_profile(
                "https://v.douyin.com/abc123/",
                downloader_dir=Path("/tmp/downloader"),
                cookies={},
            )

        self.assertEqual(result.profile_url, "https://www.douyin.com/user/sec_uid_x")
        self.assertEqual(result.sec_uid, "sec_uid_x")
        self.assertEqual(result.name, "测试作者")
        self.assertEqual(result.douyin_id, "tester001")
        self.assertFalse(result.douyin_id_is_fallback)

    def test_resolve_author_profile_requires_name(self) -> None:
        with (
            patch(
                "pipeline.profile_resolver._resolve_profile_url",
                return_value="https://www.douyin.com/user/sec_uid_x",
            ),
            patch("pipeline.profile_resolver._fetch_user_info", return_value={}),
            self.assertRaises(ProfileResolveError),
        ):
            _ = resolve_author_profile(
                "https://www.douyin.com/user/sec_uid_x",
                downloader_dir=Path("/tmp/downloader"),
                cookies={},
            )


class ConfigBoolParsingTests(unittest.TestCase):
    def test_load_config_accepts_string_false_for_bools(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            cfg_path.write_text(
                textwrap.dedent(
                    """
                    data_dir: data
                    authors:
                      - douyin_id: "exampleid"
                        name: "示例作者"
                        profile_url: "https://www.douyin.com/user/MS4wLjABAAAAxxxx"
                    storage:
                      rclone_remote: gdrive
                      base_dir: DouyinArchive
                    wecom:
                      webhook_url: ""
                      push_on_first_sync: "false"
                    downloader:
                      progress:
                        quiet_logs: "false"
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            cfg = load_config(cfg_path)
            self.assertFalse(cfg.wecom.push_on_first_sync)
            self.assertFalse(cfg.downloader.quiet_logs)

    def test_load_config_rejects_invalid_bool_string(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            cfg_path.write_text(
                textwrap.dedent(
                    """
                    data_dir: data
                    authors:
                      - douyin_id: "exampleid"
                        name: "示例作者"
                        profile_url: "https://www.douyin.com/user/MS4wLjABAAAAxxxx"
                    storage:
                      rclone_remote: gdrive
                      base_dir: DouyinArchive
                    wecom:
                      webhook_url: ""
                      push_on_first_sync: "maybe"
                    downloader:
                      progress:
                        quiet_logs: true
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaises(ConfigError):
                _ = load_config(cfg_path)


class AuthorConfigMutationTests(unittest.TestCase):
    def _write_base_config(self, cfg_path: Path) -> None:
        cfg_path.write_text(
            textwrap.dedent(
                """
                data_dir: data
                authors: []
                storage:
                  rclone_remote: gdrive
                  base_dir: DouyinArchive
                wecom:
                  webhook_url: ""
                downloader: {}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    def test_add_update_remove_author_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            self._write_base_config(cfg_path)

            created = add_or_update_author(
                cfg_path,
                douyin_id="exampleid",
                name="示例作者",
                profile_url="https://www.douyin.com/user/MS4wLjABAAAAxxxx",
            )
            self.assertTrue(created.created)

            updated = add_or_update_author(
                cfg_path,
                douyin_id="exampleid",
                name="示例作者2",
                profile_url="https://www.douyin.com/user/MS4wLjABAAAAyyyy",
            )
            self.assertFalse(updated.created)

            authors = list_authors(cfg_path)
            self.assertEqual(len(authors), 1)
            self.assertEqual(authors[0].name, "示例作者2")
            self.assertEqual(authors[0].profile_url, "https://www.douyin.com/user/MS4wLjABAAAAyyyy")

            removed = remove_author(cfg_path, author_key=authors[0].key)
            self.assertTrue(removed)
            self.assertEqual(list_authors(cfg_path), [])


class MonitorSchedulerTests(unittest.TestCase):
    def _write_base_config(self, cfg_path: Path) -> None:
        cfg_path.write_text(
            textwrap.dedent(
                """
                data_dir: data
                authors: []
                storage:
                  rclone_remote: gdrive
                  base_dir: DouyinArchive
                wecom:
                  webhook_url: ""
                downloader: {}
                """
            ).strip()
            + "\n",
            encoding="utf-8",
        )

    def test_trigger_now_executes_once_and_updates_status(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            self._write_base_config(cfg_path)
            calls: list[list[str]] = []

            def _fake_run(cmd: list[str], text: bool, capture_output: bool):
                self.assertTrue(text)
                self.assertTrue(capture_output)
                calls.append(cmd)
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="ok", stderr="")

            with patch("pipeline.web_server.subprocess.run", side_effect=_fake_run):
                scheduler = MonitorScheduler(
                    config_path=cfg_path,
                    min_interval_seconds=3300,
                    max_interval_seconds=3900,
                )
                scheduler.start()
                scheduler.trigger_now("author_added:test")

                for _ in range(40):
                    if calls:
                        break
                    time.sleep(0.05)
                scheduler.stop()

            self.assertEqual(len(calls), 1)
            self.assertIn("--once", calls[0])
            status = scheduler.status()
            self.assertEqual(status["last_return_code"], 0)
            self.assertEqual(status["last_reason"], "author_added:test")
            self.assertEqual(status["min_interval_seconds"], 3300)
            self.assertEqual(status["max_interval_seconds"], 3900)
            self.assertFalse(status["running"])


class DryRunStateRegressionTests(unittest.TestCase):
    def _make_context(self, root: Path) -> tuple[RunContext, Author, Path]:
        author = Author(
            douyin_id="exampleid",
            name="示例作者",
            profile_url="https://www.douyin.com/user/MS4wLjABAAAAxxxx",
        )
        cfg = AppConfig(
            authors=[author],
            storage=StorageConfig(rclone_remote="gdrive", base_dir="DouyinArchive"),
            wecom=WecomConfig(
                webhook_url="",
                push_on_first_sync=False,
                max_list_items=20,
            ),
            downloader=DownloaderConfig(
                douyin_downloader_dir=root / ".tools" / "douyin-downloader",
                cookies_file=None,
                thread=5,
                retry_times=3,
                quiet_logs=True,
                browser_fallback={},
            ),
            data_dir=root / "data",
        )
        state_path = cfg.data_dir / "state" / "state.json"
        return RunContext(cfg=cfg, state_path=state_path), author, state_path

    def test_run_once_dry_run_does_not_write_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ctx, author, state_path = self._make_context(root)
            video_dir = (
                ctx.cfg.data_dir
                / "downloads"
                / author.key
                / "测试作者目录"
                / "post"
                / "2024-02-07_作品标题_7600224486650121526"
            )
            video_dir.mkdir(parents=True, exist_ok=True)
            mp4_path = video_dir / "x.mp4"
            mp4_path.write_text("x", encoding="utf-8")
            item = VideoItem(
                aweme_id="7600224486650121526",
                title="作品标题",
                date="2024-02-07",
                video_dir=video_dir,
            )

            class DummyStorage:
                def __init__(self, remote: str, base_dir: str) -> None:
                    self.remote = remote
                    self.base_dir = base_dir

                def remote_path(self, *parts: str) -> str:
                    return "gdrive:" + "/".join(parts)

                def mkdir(self, remote_dir: str, *, dry_run: bool) -> None:
                    return

                def copy_mp4_dir(self, src_dir: Path, dest_dir: str, *, dry_run: bool) -> None:
                    raise AssertionError("dry-run 下不应执行上传")

            with (
                patch("pipeline.runner.ensure_repo", return_value=root / "run.py"),
                patch("pipeline.runner.load_cookies", return_value={}),
                patch("pipeline.runner.run_downloader", return_value=None),
                patch("pipeline.runner.discover_video_items", return_value=[item]),
                patch("pipeline.runner.find_mp4_files", return_value=[mp4_path]),
                patch("pipeline.runner.RcloneStorage", DummyStorage),
            ):
                rc = run_once(ctx, dry_run=True)

            self.assertEqual(rc, 0)
            self.assertFalse(state_path.exists())

    def test_run_once_real_run_writes_state(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ctx, author, state_path = self._make_context(root)
            video_dir = (
                ctx.cfg.data_dir
                / "downloads"
                / author.key
                / "测试作者目录"
                / "post"
                / "2024-02-07_作品标题_7600224486650121526"
            )
            video_dir.mkdir(parents=True, exist_ok=True)
            mp4_path = video_dir / "x.mp4"
            mp4_path.write_text("x", encoding="utf-8")
            item = VideoItem(
                aweme_id="7600224486650121526",
                title="作品标题",
                date="2024-02-07",
                video_dir=video_dir,
            )

            class DummyStorage:
                copied: list[str] = []

                def __init__(self, remote: str, base_dir: str) -> None:
                    self.remote = remote
                    self.base_dir = base_dir

                def remote_path(self, *parts: str) -> str:
                    return "gdrive:" + "/".join(parts)

                def mkdir(self, remote_dir: str, *, dry_run: bool) -> None:
                    return

                def copy_mp4_dir(self, src_dir: Path, dest_dir: str, *, dry_run: bool) -> None:
                    DummyStorage.copied.append(dest_dir)
                    return

            with (
                patch("pipeline.runner.ensure_repo", return_value=root / "run.py"),
                patch("pipeline.runner.load_cookies", return_value={}),
                patch("pipeline.runner.run_downloader", return_value=None),
                patch("pipeline.runner.discover_video_items", return_value=[item]),
                patch("pipeline.runner.find_mp4_files", return_value=[mp4_path]),
                patch("pipeline.runner.RcloneStorage", DummyStorage),
            ):
                rc = run_once(ctx, dry_run=False)

            self.assertEqual(rc, 0)
            self.assertTrue(state_path.exists())
            data = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertIn("7600224486650121526", data["authors"][author.key]["uploaded"])
            self.assertEqual(DummyStorage.copied, [f"gdrive:{author.key}"])
            self.assertFalse(video_dir.exists())
            self.assertFalse(mp4_path.exists())


class ControlFlowRegressionTests(unittest.TestCase):
    def _minimal_cfg(self, root: Path) -> AppConfig:
        return AppConfig(
            authors=[
                Author(
                    douyin_id="exampleid",
                    name="示例作者",
                    profile_url="https://www.douyin.com/user/MS4wLjABAAAAxxxx",
                )
            ],
            storage=StorageConfig(rclone_remote="gdrive", base_dir="DouyinArchive"),
            wecom=WecomConfig(webhook_url="", push_on_first_sync=False, max_list_items=20),
            downloader=DownloaderConfig(
                douyin_downloader_dir=root / ".tools" / "douyin-downloader",
                cookies_file=None,
                thread=5,
                retry_times=3,
                quiet_logs=True,
                browser_fallback={},
            ),
            data_dir=root / "data",
        )

    def test_doctor_uses_check_remote(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg = self._minimal_cfg(Path(td))

            class DummyStorage:
                checked = False

                def __init__(self, remote: str, base_dir: str) -> None:
                    self.remote = remote
                    self.base_dir = base_dir

                def check_remote(self) -> None:
                    DummyStorage.checked = True

            with (
                patch("pipeline.runner.ensure_repo", return_value=Path(td) / "run.py"),
                patch("pipeline.runner.load_cookies", return_value={}),
                patch("pipeline.runner.RcloneStorage", DummyStorage),
            ):
                rc = doctor(cfg)

            self.assertEqual(rc, 0)
            self.assertTrue(DummyStorage.checked)

    def test_main_loop_fail_fast_exits_on_failure(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            cfg_path.write_text(
                textwrap.dedent(
                    """
                    data_dir: data
                    authors:
                      - douyin_id: "exampleid"
                        name: "示例作者"
                        profile_url: "https://www.douyin.com/user/MS4wLjABAAAAxxxx"
                    storage:
                      rclone_remote: gdrive
                      base_dir: DouyinArchive
                    wecom:
                      webhook_url: ""
                    downloader: {}
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("pipeline.runner.run_once", return_value=1):
                rc = main(["--loop", "--fail-fast", "-c", str(cfg_path), "--interval", "1"])

            self.assertEqual(rc, 1)

    def test_main_loop_without_fail_fast_reaches_retry_sleep(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cfg_path = Path(td) / "pipeline.yml"
            cfg_path.write_text(
                textwrap.dedent(
                    """
                    data_dir: data
                    authors:
                      - douyin_id: "exampleid"
                        name: "示例作者"
                        profile_url: "https://www.douyin.com/user/MS4wLjABAAAAxxxx"
                    storage:
                      rclone_remote: gdrive
                      base_dir: DouyinArchive
                    wecom:
                      webhook_url: ""
                    downloader: {}
                    """
                ).strip()
                + "\n",
                encoding="utf-8",
            )

            def _stop_sleep(_: int) -> None:
                raise KeyboardInterrupt

            with (
                patch("pipeline.runner.run_once", return_value=1) as run_once_mock,
                patch("pipeline.runner.time.sleep", side_effect=_stop_sleep) as sleep_mock,
                self.assertRaises(KeyboardInterrupt),
            ):
                _ = main(["--loop", "-c", str(cfg_path), "--interval", "1"])

            run_once_mock.assert_called_once()
            sleep_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
