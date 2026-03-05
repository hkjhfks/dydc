from __future__ import annotations

import datetime as dt
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
import random
import subprocess
import sys
import threading
import time
from urllib.parse import unquote, urlparse

from .config import ConfigError, add_or_update_author, list_authors, remove_author
from .state import load_state, save_state


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _tail_text(text: str, *, max_lines: int = 20, max_chars: int = 5000) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    tail = "\n".join(lines[-max_lines:])
    if len(tail) <= max_chars:
        return tail
    return tail[-max_chars:]


class MonitorScheduler:
    def __init__(
        self,
        *,
        config_path: str | Path,
        min_interval_seconds: int,
        max_interval_seconds: int,
    ) -> None:
        self.config_path = Path(config_path).resolve()
        min_s = max(1, int(min_interval_seconds))
        max_s = max(1, int(max_interval_seconds))
        if min_s > max_s:
            min_s, max_s = max_s, min_s
        self.min_interval_seconds = min_s
        self.max_interval_seconds = max_s

        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None

        self._running = False
        self._current_reason = ""
        self._queued = False
        self._queued_reason = ""
        self._queued_at = ""

        self._last_started_at = ""
        self._last_finished_at = ""
        self._last_reason = ""
        self._last_return_code: int | None = None
        self._last_stdout = ""
        self._last_stderr = ""
        self._next_due_at = ""

    def start(self) -> None:
        with self._state_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._wake_event.clear()
            self._thread = threading.Thread(
                target=self._worker,
                name="pipeline-monitor",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=8)

    def trigger_now(self, reason: str) -> None:
        clean_reason = reason.strip() or "manual"
        with self._state_lock:
            self._queued = True
            self._queued_reason = clean_reason
            self._queued_at = utc_now_iso()
        self._wake_event.set()

    def status(self) -> dict[str, object]:
        with self._state_lock:
            return {
                "min_interval_seconds": self.min_interval_seconds,
                "max_interval_seconds": self.max_interval_seconds,
                "next_due_at": self._next_due_at,
                "running": self._running,
                "current_reason": self._current_reason,
                "queued": self._queued,
                "queued_reason": self._queued_reason,
                "queued_at": self._queued_at,
                "last_started_at": self._last_started_at,
                "last_finished_at": self._last_finished_at,
                "last_reason": self._last_reason,
                "last_return_code": self._last_return_code,
                "last_ok": None if self._last_return_code is None else self._last_return_code == 0,
                "last_stdout": self._last_stdout,
                "last_stderr": self._last_stderr,
            }

    def _draw_interval_seconds(self) -> int:
        return random.randint(self.min_interval_seconds, self.max_interval_seconds)

    def _set_next_due(self, *, after_seconds: int) -> None:
        due = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=int(after_seconds))
        with self._state_lock:
            self._next_due_at = due.isoformat()

    def _pop_queued_reason(self) -> str | None:
        with self._state_lock:
            if not self._queued:
                return None
            reason = self._queued_reason or "manual"
            self._queued = False
            self._queued_reason = ""
            self._queued_at = ""
            return reason

    def _set_running(self, *, running: bool, reason: str) -> None:
        with self._state_lock:
            self._running = running
            self._current_reason = reason if running else ""

    def _save_result(
        self,
        *,
        started_at: str,
        finished_at: str,
        reason: str,
        rc: int,
        stdout: str,
        stderr: str,
    ) -> None:
        with self._state_lock:
            self._last_started_at = started_at
            self._last_finished_at = finished_at
            self._last_reason = reason
            self._last_return_code = rc
            self._last_stdout = _tail_text(stdout)
            self._last_stderr = _tail_text(stderr)

    def _run_once(self, *, reason: str) -> None:
        started_at = utc_now_iso()
        self._set_running(running=True, reason=reason)
        cmd = [sys.executable, "-m", "pipeline", "--once", "-c", str(self.config_path)]

        rc = -1
        out = ""
        err = ""
        try:
            proc = subprocess.run(cmd, text=True, capture_output=True)
            rc = int(proc.returncode)
            out = proc.stdout or ""
            err = proc.stderr or ""
        except Exception as e:
            err = str(e)
        finally:
            finished_at = utc_now_iso()
            self._set_running(running=False, reason="")
            self._save_result(
                started_at=started_at,
                finished_at=finished_at,
                reason=reason,
                rc=rc,
                stdout=out,
                stderr=err,
            )

    def _worker(self) -> None:
        first_delay = self._draw_interval_seconds()
        next_due = time.monotonic() + first_delay
        self._set_next_due(after_seconds=first_delay)
        while not self._stop_event.is_set():
            wait_for = max(0.0, next_due - time.monotonic())
            self._wake_event.wait(wait_for)
            self._wake_event.clear()
            if self._stop_event.is_set():
                break

            reason = self._pop_queued_reason()
            now = time.monotonic()
            if reason is None and now < next_due:
                continue
            run_reason = reason or "scheduled"
            self._run_once(reason=run_reason)
            next_delay = self._draw_interval_seconds()
            next_due = time.monotonic() + next_delay
            self._set_next_due(after_seconds=next_delay)


class _PipelineWebHandler(SimpleHTTPRequestHandler):
    config_path: Path
    state_path: Path
    static_dir: Path
    scheduler: MonitorScheduler
    lock = threading.Lock()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(self.static_dir), **kwargs)

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0") or 0)
        if content_length <= 0:
            raise ValueError("请求体不能为空")
        raw = self.rfile.read(content_length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"JSON 解析失败: {e}") from e
        if not isinstance(data, dict):
            raise ValueError("JSON 请求体必须是对象")
        return data

    def _list_authors(self) -> list[dict[str, str]]:
        authors = list_authors(self.config_path)
        return [
            {
                "key": author.key,
                "douyin_id": author.douyin_id,
                "name": author.name,
                "profile_url": author.profile_url,
            }
            for author in authors
        ]

    def _handle_get_authors(self) -> None:
        try:
            items = self._list_authors()
        except ConfigError as e:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return
        self._send_json(HTTPStatus.OK, {"items": items})

    def _handle_get_monitor(self) -> None:
        self._send_json(HTTPStatus.OK, {"monitor": self.scheduler.status()})

    def _handle_create_author(self) -> None:
        try:
            payload = self._read_json()
            douyin_id = str(payload.get("douyin_id", "")).strip()
            name = str(payload.get("name", "")).strip()
            profile_url = str(payload.get("profile_url", "")).strip()
            with self.lock:
                result = add_or_update_author(
                    self.config_path,
                    douyin_id=douyin_id,
                    name=name,
                    profile_url=profile_url,
                )
                items = self._list_authors()
                self.scheduler.trigger_now(f"author_added:{result.author.key}")
        except (ConfigError, ValueError) as e:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        status = HTTPStatus.CREATED if result.created else HTTPStatus.OK
        self._send_json(
            status,
            {
                "message": "created" if result.created else "updated",
                "item": {
                    "key": result.author.key,
                    "douyin_id": result.author.douyin_id,
                    "name": result.author.name,
                    "profile_url": result.author.profile_url,
                },
                "items": items,
                "monitor": self.scheduler.status(),
            },
        )

    def _handle_trigger_monitor(self) -> None:
        self.scheduler.trigger_now("manual_api")
        self._send_json(
            HTTPStatus.ACCEPTED,
            {
                "message": "queued",
                "monitor": self.scheduler.status(),
            },
        )

    def _handle_delete_author(self, author_key: str) -> None:
        if not author_key:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "author_key 不能为空"})
            return
        try:
            with self.lock:
                removed = remove_author(self.config_path, author_key=author_key)
                if not removed:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "作者不存在"})
                    return
                state = load_state(self.state_path)
                if state.remove_author(author_key):
                    save_state(state)
                items = self._list_authors()
        except ConfigError as e:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return

        self._send_json(HTTPStatus.OK, {"message": "deleted", "items": items})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/authors":
            self._handle_get_authors()
            return
        if parsed.path == "/api/monitor":
            self._handle_get_monitor()
            return
        if parsed.path.startswith("/api/"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "API 路径不存在"})
            return
        if parsed.path == "/":
            self.path = "/index.html"
        super().do_GET()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/authors":
            self._handle_create_author()
            return
        if parsed.path == "/api/monitor/run-now":
            self._handle_trigger_monitor()
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "API 路径不存在"})

    def do_DELETE(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/authors/"):
            author_key = unquote(parsed.path[len("/api/authors/") :]).strip()
            self._handle_delete_author(author_key)
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "API 路径不存在"})


def run_web_server(
    *,
    config_path: str | Path,
    state_path: str | Path,
    host: str,
    port: int,
    min_interval_seconds: int,
    max_interval_seconds: int,
) -> int:
    static_dir = (Path(__file__).resolve().parent.parent / "web").resolve()
    if not static_dir.exists():
        raise RuntimeError(f"前端目录不存在: {static_dir}")
    config_path = Path(config_path).resolve()
    state_path = Path(state_path).resolve()
    scheduler = MonitorScheduler(
        config_path=config_path,
        min_interval_seconds=min_interval_seconds,
        max_interval_seconds=max_interval_seconds,
    )
    scheduler.start()

    class Handler(_PipelineWebHandler):
        pass

    Handler.config_path = config_path
    Handler.state_path = state_path
    Handler.static_dir = static_dir
    Handler.scheduler = scheduler

    httpd = ThreadingHTTPServer((host, int(port)), Handler)
    print(
        "Web 控制台已启动: "
        f"http://{host}:{port}（监控间隔随机 {scheduler.min_interval_seconds}s~{scheduler.max_interval_seconds}s）"
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()
        httpd.server_close()
    return 0
