"""Local process supervisor for the API and scheduled worker.

Each managed process starts in its own session/process group. That makes local
stop/restart deterministic: Gunicorn's workers and Playwright's browser children
receive the same shutdown signal as their owning process.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

import psutil


ROOT_DIR = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT_DIR / ".run"
API_STATE_PATH = RUNTIME_DIR / "api.json"
WORKER_STATE_PATH = RUNTIME_DIR / "worker.json"
API_LOG_PATH = RUNTIME_DIR / "api.log"
WORKER_LOG_PATH = RUNTIME_DIR / "worker.log"
GRACE_SECONDS = 15


def _read_state(path: Path) -> dict[str, Any] | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _is_recorded_process_running(state: dict[str, Any]) -> bool:
    try:
        process = psutil.Process(int(state["pid"]))
        return abs(process.create_time() - float(state["created_at"])) < 0.1
    except (KeyError, TypeError, ValueError, psutil.Error):
        return False


def _write_state(path: Path, process: subprocess.Popen[bytes]) -> None:
    managed = psutil.Process(process.pid)
    path.write_text(
        json.dumps({"pid": process.pid, "created_at": managed.create_time()}, indent=2),
        encoding="utf-8",
    )


def _kill_process_group(pid: int) -> None:
    """Gracefully terminate a process group, then force anything still alive."""
    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        return

    deadline = time.monotonic() + GRACE_SECONDS
    while time.monotonic() < deadline:
        try:
            leader = psutil.Process(pid)
            if not leader.is_running() or leader.status() == psutil.STATUS_ZOMBIE:
                break
        except psutil.Error:
            break
        time.sleep(0.1)

    try:
        os.killpg(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass


def _terminate_process_tree(process: psutil.Process) -> None:
    """Clean up a stale unmanaged process without touching this shell group."""
    children = process.children(recursive=True)
    for child in reversed(children):
        try:
            child.terminate()
        except psutil.Error:
            pass
    try:
        process.terminate()
    except (psutil.Error, OSError):
        return

    _, alive = psutil.wait_procs([*children, process], timeout=GRACE_SECONDS)
    for remaining in alive:
        try:
            remaining.kill()
        except psutil.Error:
            pass


def _is_local_api(process: psutil.Process) -> bool:
    try:
        command = " ".join(process.cmdline())
    except psutil.Error:
        return False
    return "gunicorn" in command and "app.api:app" in command


def _is_local_worker(process: psutil.Process) -> bool:
    try:
        command = " ".join(process.cmdline())
    except psutil.Error:
        return False
    return "app.worker" in command and "local_supervisor" not in command


def _stop_managed(path: Path) -> None:
    state = _read_state(path)
    if state and _is_recorded_process_running(state):
        _kill_process_group(int(state["pid"]))
    path.unlink(missing_ok=True)


def stop() -> None:
    """Stop recorded processes and any scoped local processes left from a crash."""
    _stop_managed(API_STATE_PATH)
    _stop_managed(WORKER_STATE_PATH)

    # A killed terminal can leave pid files behind. Clean only commands belonging
    # to this project; never issue a broad chromium/playwright process kill.
    current_pid = os.getpid()
    try:
        processes = list(psutil.process_iter())
    except (psutil.Error, OSError):
        # Recorded PID/process-group cleanup above is sufficient when a sandbox
        # disallows listing all processes. Normal local macOS/Linux execution
        # reaches the stale-process cleanup below.
        return

    for process in processes:
        if process.pid == current_pid:
            continue
        if _is_local_api(process) or _is_local_worker(process):
            _terminate_process_tree(process)


def _start_process(command: list[str], log_path: Path) -> subprocess.Popen[bytes]:
    log_handle = log_path.open("ab")
    try:
        return subprocess.Popen(
            command,
            cwd=ROOT_DIR,
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        log_handle.close()


def start() -> None:
    stop()
    RUNTIME_DIR.mkdir(exist_ok=True)

    python = ROOT_DIR / "venv" / "bin" / "python"
    if not python.is_file():
        raise RuntimeError("Expected project virtual environment at venv/bin/python")

    subprocess.run([str(python), "-m", "app.data.bootstrap"], cwd=ROOT_DIR, check=True)

    bind = os.getenv("PBE_BIND", "0.0.0.0:8080")
    api = _start_process(
        [
            str(python),
            "-m",
            "gunicorn",
            "app.api:app",
            "--bind",
            bind,
            "--workers",
            "2",
            "--worker-class",
            "uvicorn_worker.UvicornWorker",
            "--timeout",
            "60",
            "--graceful-timeout",
            "90",
            "--keep-alive",
            "5",
            "--max-requests",
            "2000",
            "--max-requests-jitter",
            "200",
        ],
        API_LOG_PATH,
    )
    _write_state(API_STATE_PATH, api)

    worker = _start_process([str(python), "-m", "app.worker"], WORKER_LOG_PATH)
    _write_state(WORKER_STATE_PATH, worker)

    time.sleep(0.5)
    if api.poll() is not None or worker.poll() is not None:
        stop()
        raise RuntimeError(
            f"Local process failed to start. Check {API_LOG_PATH} and {WORKER_LOG_PATH}."
        )

    print(f"API: http://{bind}")
    print(f"Logs: {API_LOG_PATH} and {WORKER_LOG_PATH}")


def status() -> int:
    states = (("api", API_STATE_PATH), ("worker", WORKER_STATE_PATH))
    running = True
    for name, path in states:
        state = _read_state(path)
        active = bool(state and _is_recorded_process_running(state))
        pid = state.get("pid") if state else "-"
        print(f"{name}: {'running' if active else 'stopped'} (pid: {pid})")
        running = running and active
    return 0 if running else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("start", "stop", "restart", "status"))
    args = parser.parse_args()

    if args.action == "start":
        start()
        return 0
    if args.action == "stop":
        stop()
        return 0
    if args.action == "restart":
        start()
        return 0
    return status()


if __name__ == "__main__":
    raise SystemExit(main())
