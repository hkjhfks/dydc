from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
from typing import Sequence


class RcloneError(RuntimeError):
    pass


def ensure_rclone(rclone_bin: str = "rclone") -> str:
    resolved = shutil.which(rclone_bin)
    if not resolved:
        raise RcloneError(
            f"未找到 rclone（命令: {rclone_bin}）。\n"
            "- 需要先安装并配置 Google Drive remote（例如 remote 名称为 gdrive）。\n"
            "- 安装参考: https://rclone.org/install/\n"
            "- 配置参考: https://rclone.org/drive/\n"
        )
    return resolved


def _run(cmd: Sequence[str]) -> None:
    proc = subprocess.run(list(cmd), text=True)
    if proc.returncode != 0:
        raise RcloneError(f"rclone 执行失败（exit={proc.returncode}）: {' '.join(cmd)}")


@dataclass(frozen=True)
class RcloneStorage:
    remote: str
    base_dir: str
    rclone_bin: str = "rclone"

    def remote_path(self, *parts: str) -> str:
        # rclone paths look like: remote:dir/subdir
        clean = [p.strip("/").strip() for p in parts if p and p.strip("/").strip()]
        tail = "/".join(clean)
        base = self.base_dir.strip("/").strip()
        if base:
            tail = f"{base}/{tail}" if tail else base
        return f"{self.remote}:{tail}"

    def check_remote(self) -> None:
        ensure_rclone(self.rclone_bin)
        remote_root = f"{self.remote.strip(':').strip()}:"
        _run([self.rclone_bin, "lsd", remote_root])

    def mkdir(self, remote_dir: str, *, dry_run: bool) -> None:
        ensure_rclone(self.rclone_bin)
        if dry_run:
            return
        _run([self.rclone_bin, "mkdir", remote_dir])

    def copy_mp4_dir(self, src_dir: Path, dest_dir: str, *, dry_run: bool) -> None:
        ensure_rclone(self.rclone_bin)
        if dry_run:
            return
        # Copy only mp4(s) from a single work directory.
        _run(
            [
                self.rclone_bin,
                "copy",
                str(src_dir),
                dest_dir,
                "--include",
                "*.mp4",
                "--exclude",
                "*",
            ]
        )
