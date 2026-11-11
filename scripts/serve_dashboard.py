"""Week 8: serve the dashboard locally and let it follow a running replay live.

Serves docs/dashboard/ at http://127.0.0.1:8765 and answers /live with the alerts the
streaming consumer has written so far for the newest run under --work-root (or the run
named with --name). The page polls /live once a second and pops up every new stress alert.

Standard library only; listens on 127.0.0.1 (this computer) only. Read-only: it never
writes to the run folders.

    py -3.12 scripts\\serve_dashboard.py --work-root C:\\fs6            # Windows demo
    python scripts/serve_dashboard.py                                  # default work root
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
import webbrowser
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "docs" / "dashboard"
ALERT_FIELDS = ("observation_month", "model_stage", "score", "cutoff", "alert", "latency_s", "reason_codes", "features", "batch_id")


def read_json(path: Path):
    """Read a JSON file another process may be replacing; None if it is not readable right now."""
    for _ in range(3):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError):
            time.sleep(0.02)
    return None


def find_run(work_root: Path, name: str | None) -> Path | None:
    if name:
        run = work_root / name
        return run if (run / "effective_config.json").exists() else None
    runs = [d for d in work_root.iterdir() if (d / "effective_config.json").exists()] if work_root.is_dir() else []
    return max(runs, key=lambda d: (d / "effective_config.json").stat().st_mtime, default=None)


def live_payload(work_root: Path, name: str | None) -> dict:
    run = find_run(work_root, name)
    if run is None:
        return {"run": None, "work_root": str(work_root), "server_time": time.time(),
                "message": f"Waiting for a replay to start in {work_root}"}
    alerts = []
    for path in sorted((run / "alerts").glob("batch-*.jsonl")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:  # being replaced right now (Windows); picked up on the next poll
            continue
        for line in text.splitlines():
            try:
                a = json.loads(line)
            except ValueError:
                continue
            alerts.append({k: a.get(k) for k in ALERT_FIELDS})
    return {"run": run.name, "run_started": (run / "effective_config.json").stat().st_mtime, "work_root": str(work_root),
            "status": read_json(run / "consumer_status.json"), "finished": (run / "STOP").exists(),
            "alerts": alerts, "server_time": time.time()}


class Handler(SimpleHTTPRequestHandler):
    work_root: Path = Path(".")
    run_name: str | None = None

    def do_GET(self):  # noqa: N802
        if self.path.split("?", 1)[0] == "/live":
            body = json.dumps(live_payload(self.work_root, self.run_name)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        super().do_GET()

    def log_message(self, fmt, *args):  # keep the console quiet: no line per poll
        if "/live" not in (args[0] if args else ""):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--work-root", type=Path, default=None,
                        help="Folder the replay uses (--work-root of run_week6_replay.py), e.g. C:\\fs6.")
    parser.add_argument("--name", default=None, help="Follow this run only (default: the newest run in the work root).")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-browser", action="store_true", help="Do not open the page in the browser.")
    args = parser.parse_args()
    if args.work_root is None:
        cfg = json.loads((ROOT / "configs" / "week6_streaming.local.json").read_text(encoding="utf-8"))
        args.work_root = ROOT / cfg["work_root"]
    if not (DASHBOARD / "index.html").exists():
        sys.exit(f"{DASHBOARD / 'index.html'} not found: run scripts/build_week8_dashboard.py first")
    Handler.work_root, Handler.run_name = args.work_root.resolve(), args.name
    server = None
    for port in range(args.port, args.port + 10):
        try:
            server = ThreadingHTTPServer(("127.0.0.1", port), partial(Handler, directory=str(DASHBOARD)))
            break
        except OSError:
            continue
    if server is None:
        sys.exit(f"No free port between {args.port} and {args.port + 9}")
    url = f"http://127.0.0.1:{server.server_port}/#tab=streaming"
    print(f"FoodShield dashboard: {url}")
    print(f"Following replays in {Handler.work_root}" + (f" (run '{args.name}')" if args.name else " (newest run)"))
    print("Leave this window open during the demo; close it (or press Ctrl+C) to stop.")
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
