"""File and process helpers that behave the same on Windows as on Linux.

On Windows a file that another process has open cannot be replaced, so an atomic
write (write ``x.tmp``, then ``os.replace`` onto ``x``) fails with PermissionError while
a reader holds ``x`` for a moment. The runner polls the consumer's status file once a
second, so that collision happens. ``write_text_atomic`` retries briefly.

Killing a process on Windows does not kill its children (Spark's JVM, Kafka's JVM), so
``kill_tree`` uses ``taskkill /T``.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path


def replace_file(tmp: Path, target: Path, attempts: int = 100, wait_s: float = 0.02) -> None:
    """os.replace with retries while another process briefly holds the target open (Windows)."""
    for attempt in range(attempts):
        try:
            os.replace(tmp, target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(wait_s)


def write_text_atomic(path: Path, text: str, required: bool = True) -> bool:
    """Write via a temporary file and replace. With required=False a still-locked target is skipped."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    try:
        replace_file(tmp, path)
        return True
    except PermissionError:
        if required:
            raise
        return False


def kill_tree(process: subprocess.Popen | None, wait_s: float = 30.0) -> None:
    """Stop a process and everything it started; no-op if it has already exited."""
    if process is None or process.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(process.pid)], capture_output=True, check=False)
    else:
        process.kill()
    try:
        process.wait(wait_s)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(10)
