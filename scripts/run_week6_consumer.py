"""Week 6 Spark Structured Streaming consumer (started by run_week6_replay.py).

    python scripts/run_week6_consumer.py --run-dir data/interim/week6/runs/<name>

It runs until a file named STOP appears in the run folder. Restarting it with
the same --run-dir resumes from the Spark checkpoint and the saved monitor state.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from foodshield.streaming.spark_job import run_consumer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "configs" / "week6_streaming.local.json")
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    run_consumer(ROOT, json.loads(args.config.read_text(encoding="utf-8")), args.run_dir.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
