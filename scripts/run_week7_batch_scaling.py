"""Week 7 batch scaling series (SCALABILITY_EXPERIMENT.md, D9-E).

Fixed pipeline slice (the D9-B weather aggregation, unchanged logic):

    stage 1  read daily cell weather (Parquet) -> monthly per cell -> write Parquet
    stage 2  read cell-months -> join wheat-area weights -> national monthly -> write Parquet

Workload sizes are labelled, benchmark-only synthetic expansions of the real
1,864,120-row daily grid file. Factor k makes k copies with a
``benchmark_series`` id (0..k-1). Series 0 is the real data unchanged, so every
run can be checked against the D9-B batch output. Series 1..k-1 add seeded noise
(precipitation x (1 + 0.05 N(0,1)), floored at 0; temperatures + 0.5 N(0,1)).
They are never used for modelling, coverage or food-market conclusions.

Every configuration runs in its own Spark process: one warm-up run (reported,
not used), then ``--repeats`` timed runs. Shuffle and spill bytes come from
Spark's REST API (the UI is on 127.0.0.1 only).

Series run by ``--phase all``:
- size: k = 1, 10, 30, 100 at the reference configuration;
- cores: local[1] vs local[2] at k = 30;
- shuffle partitions: 2, 4, 8, 16, 64, 200 at k = 30;
- driver memory: 1g, 2g, 4g at k = 30;
- FAOSTAT: the D9-A query on CSV vs Parquet, repeated (real data, no expansion).

    python scripts/run_week7_batch_scaling.py --phase all
    python scripts/run_week7_batch_scaling.py --phase report    # tables and plots from existing runs

Outputs: data/processed/week7_batch_scaling/ (runs.jsonl, summary.json, summary.csv)
and docs/figures/week7_*.png. Workloads and run outputs stay in data/interim/week7/.

On a second machine, add ``--output-suffix _laptop`` so the results go to
data/processed/week7_batch_scaling_laptop/ and docs/figures/week7_*_laptop.png
instead of replacing the committed workspace series:

    python scripts/run_week7_batch_scaling.py --phase all --output-suffix _laptop
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median, stdev

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

WORK = ROOT / "data" / "interim" / "week7"
# The suffix is read from the environment so the child processes started by --phase all inherit it.
SUFFIX = os.environ.get("FOODSHIELD_W7_OUTPUT_SUFFIX", "")
OUT = ROOT / "data" / "processed" / f"week7_batch_scaling{SUFFIX}"
FIGURES = ROOT / "docs" / "figures"
GRID_CSV = ROOT / "data" / "raw" / "nasa_power_grid" / "nasa_power_grid_daily_2001_2022.csv"
CELLS = ROOT / "configs" / "d9b_grid_cells.json"
BATCH_MONTHLY = ROOT / "data" / "processed" / "d9b_grid_weather" / "nasa_power_grid_wheat_weighted_monthly.csv"
FAOSTAT_WORK = ROOT / "data" / "interim" / "faostat_bulk"
SEED = 20261107
REFERENCE = {"master": "local[2]", "shuffle": 8, "memory": "2g"}
BASE_ROWS, CELL_MONTHS, MONTHS = 1_864_120, 61_248, 264


def make_spark(master: str, memory: str, shuffle: int, app: str):
    hadoop_home = os.environ.get("HADOOP_HOME")
    if os.name == "nt" and hadoop_home:
        os.environ["PATH"] = f"{Path(hadoop_home, 'bin')}{os.pathsep}{os.environ.get('PATH', '')}"
    temp_root = WORK / ".spark-tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    tempfile.tempdir = str(temp_root)
    os.environ["TEMP"] = os.environ["TMP"] = str(temp_root)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName(app).master(master)
             .config("spark.driver.memory", memory)
             .config("spark.sql.shuffle.partitions", str(shuffle))
             .config("spark.sql.adaptive.enabled", "false")  # keep the configured partition count measurable
             .config("spark.ui.enabled", "true").config("spark.ui.host", "127.0.0.1").config("spark.ui.port", "4050")
             .config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.session.timeZone", "UTC").config("spark.local.dir", str(temp_root))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stage_metrics(spark) -> dict[int, dict]:
    url = f"{spark.sparkContext.uiWebUrl}/api/v1/applications/{spark.sparkContext.applicationId}/stages?status=complete"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    stages = json.loads(opener.open(url, timeout=30).read())
    return {s["stageId"]: s for s in stages}


def metric_sum(stages: list[dict]) -> dict:
    keys = {"shuffleReadBytes": "shuffle_read_bytes", "shuffleWriteBytes": "shuffle_write_bytes",
            "memoryBytesSpilled": "memory_spilled_bytes", "diskBytesSpilled": "disk_spilled_bytes",
            "inputBytes": "input_bytes", "outputBytes": "output_bytes", "numTasks": "tasks"}
    return {v: sum(int(s.get(k, 0) or 0) for s in stages) for k, v in keys.items()}


def dir_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


# --------------------------------------------------------------------------- workloads
def generate(factors: list[int]) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    schema = StructType([StructField("cell_id", StringType(), False), StructField("date", StringType(), False),
                         StructField("prectotcorr_mm", DoubleType()), StructField("t2m_max_c", DoubleType()),
                         StructField("t2m_min_c", DoubleType())])
    spark = make_spark(REFERENCE["master"], "3g", REFERENCE["shuffle"], "FoodShield-Week7-Generate")
    try:
        base = spark.read.schema(schema).option("header", "true").option("mode", "FAILFAST").csv(str(GRID_CSV)).cache()
        if base.count() != BASE_ROWS:
            raise ValueError("Unexpected base row count")
        for k in factors:
            path = WORK / f"workload_k{k:03d}.parquet"
            meta = path.with_suffix(".json")
            if meta.exists():
                continue
            start = time.perf_counter()
            series = spark.range(0, k, 1, numPartitions=max(1, k // 10)).withColumnRenamed("id", "benchmark_series")
            noisy = F.col("benchmark_series") > 0
            out = (base.crossJoin(series)
                   .withColumn("prectotcorr_mm", F.when(noisy, F.greatest(F.lit(0.0), F.col("prectotcorr_mm") * (1 + 0.05 * F.randn(SEED))))
                               .otherwise(F.col("prectotcorr_mm")))
                   .withColumn("t2m_max_c", F.when(noisy, F.col("t2m_max_c") + 0.5 * F.randn(SEED + 1)).otherwise(F.col("t2m_max_c")))
                   .withColumn("t2m_min_c", F.when(noisy, F.col("t2m_min_c") + 0.5 * F.randn(SEED + 2)).otherwise(F.col("t2m_min_c")))
                   .select("benchmark_series", "cell_id", "date", "prectotcorr_mm", "t2m_max_c", "t2m_min_c"))
            out.write.mode("overwrite").parquet(str(path))
            meta.write_text(json.dumps({
                "factor": k, "rows": BASE_ROWS * k, "bytes": dir_bytes(path), "files": len(list(path.glob("*.parquet"))),
                "seed": SEED, "generation_seconds": round(time.perf_counter() - start, 1),
                "label": "BENCHMARK-ONLY SYNTHETIC EXPANSION; series 0 = real D9-B data, series 1.. = seeded noise copies",
            }, indent=1), encoding="utf-8")
            print(f"generated k={k}: {BASE_ROWS * k:,} rows, {dir_bytes(path) / 1e6:.0f} MB", flush=True)
    finally:
        spark.stop()


# --------------------------------------------------------------------------- one configuration
def pipeline(spark, k: int, out_dir: Path) -> dict:
    from pyspark.sql import functions as F

    cells = json.loads(CELLS.read_text(encoding="utf-8"))["cells"]
    weights = spark.createDataFrame([(c["cell_id"], float(c["weight"])) for c in cells], ["cell_id", "weight"])
    shutil.rmtree(out_dir, ignore_errors=True)
    timings = {}
    t = time.perf_counter()
    daily = spark.read.parquet(str(WORK / f"workload_k{k:03d}.parquet"))
    (daily.groupBy("benchmark_series", "cell_id", F.substring("date", 1, 7).alias("month"))
     .agg(F.sum("prectotcorr_mm").alias("precipitation_sum_mm"), F.avg("t2m_max_c").alias("t2m_max_mean_c"),
          F.avg("t2m_min_c").alias("t2m_min_mean_c"), F.count("prectotcorr_mm").alias("days"))
     .write.mode("overwrite").parquet(str(out_dir / "cell_month")))
    timings["stage1_daily_to_cell_month_s"] = time.perf_counter() - t
    t = time.perf_counter()
    cm = spark.read.parquet(str(out_dir / "cell_month")).join(F.broadcast(weights), "cell_id")
    (cm.groupBy("benchmark_series", "month").agg(
        *[(F.sum(F.col(v) * F.col("weight")) / F.sum(F.when(F.col(v).isNotNull(), F.col("weight")))).alias(v)
          for v in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c")])
     .write.mode("overwrite").parquet(str(out_dir / "national")))
    timings["stage2_cell_month_to_national_s"] = time.perf_counter() - t
    timings["total_s"] = timings["stage1_daily_to_cell_month_s"] + timings["stage2_cell_month_to_national_s"]
    return timings


def check_output(spark, k: int, out_dir: Path) -> dict:
    from pyspark.sql import functions as F

    cell_rows = spark.read.parquet(str(out_dir / "cell_month")).count()
    national = spark.read.parquet(str(out_dir / "national"))
    national_rows = national.count()
    real = {r["month"]: r for r in national.filter(F.col("benchmark_series") == 0).collect()}
    batch = {r["month"]: r for r in csv.DictReader(BATCH_MONTHLY.open(encoding="utf-8"))}
    diff = max(abs(real[m][v] - float(batch[m][v])) for m in batch
               for v in ("precipitation_sum_mm", "t2m_max_mean_c", "t2m_min_mean_c"))
    return {"cell_month_rows": cell_rows, "national_rows": national_rows,
            "rows_ok": cell_rows == CELL_MONTHS * k and national_rows == MONTHS * k,
            "series0_max_abs_diff_vs_d9b": diff, "series0_matches_d9b": diff < 1e-3}


def measure(args) -> None:
    spark = make_spark(args.master, args.memory, args.shuffle, f"FoodShield-Week7-{args.tag}")
    out_dir = WORK / "outputs" / args.tag
    runs_path = OUT / "runs.jsonl"
    try:
        for i in range(args.repeats + 1):
            before = set(stage_metrics(spark))
            timings = pipeline(spark, args.factor, out_dir)
            after = stage_metrics(spark)
            new = [s for sid, s in after.items() if sid not in before]
            row = {"tag": args.tag, "series": args.series, "factor": args.factor, "rows": BASE_ROWS * args.factor,
                   "master": args.master, "shuffle_partitions": args.shuffle, "driver_memory": args.memory,
                   "run": i, "warmup": i == 0, **{k: round(v, 3) for k, v in timings.items()},
                   "rows_per_s": round(BASE_ROWS * args.factor / timings["total_s"], 1), **metric_sum(new),
                   "utc": datetime.now(timezone.utc).isoformat()}
            if i == args.repeats:
                row.update(check_output(spark, args.factor, out_dir))
            with runs_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row) + "\n")
            print(f"  {args.tag} run {i}{' (warm-up)' if i == 0 else ''}: {timings['total_s']:.1f} s", flush=True)
    finally:
        spark.stop()
    shutil.rmtree(out_dir, ignore_errors=True)


def measure_faostat(args) -> None:
    """D9-A query (all-country wheat rows 2010-2022) on CSV vs Parquet, repeated, real data only."""
    from foodshield.etl.bulk import DOMAINS, read_bronze
    from pyspark.sql import functions as F

    spark = make_spark(REFERENCE["master"], "3g", 16, "FoodShield-Week7-FAOSTAT")
    query = lambda df: df.filter((F.col("item_code") == 15) & F.col("year").between(2010, 2022)).count()
    try:
        for code in ("QCL", "TCL", "PP"):
            csv_path = FAOSTAT_WORK / "csv" / f"{DOMAINS[code]}.csv"
            pq_path = FAOSTAT_WORK / "parquet" / code
            for i in range(args.repeats + 1):
                for fmt in ("csv", "parquet"):
                    t = time.perf_counter()
                    df = read_bronze(spark, csv_path, code) if fmt == "csv" else spark.read.parquet(str(pq_path))
                    n = query(df)
                    row = {"tag": f"faostat_{code}_{fmt}", "series": "faostat_query", "domain": code, "format": fmt,
                           "run": i, "warmup": i == 0, "total_s": round(time.perf_counter() - t, 3), "result_rows": n,
                           "input_bytes_on_disk": csv_path.stat().st_size if fmt == "csv" else dir_bytes(pq_path),
                           "master": REFERENCE["master"], "utc": datetime.now(timezone.utc).isoformat()}
                    with (OUT / "runs.jsonl").open("a", encoding="utf-8") as stream:
                        stream.write(json.dumps(row) + "\n")
            print(f"  faostat {code} done", flush=True)
    finally:
        spark.stop()


# --------------------------------------------------------------------------- orchestration
def child(*extra: str) -> None:
    subprocess.run([sys.executable, __file__, *extra], check=True)


def run_all(repeats: int) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "runs.jsonl").unlink(missing_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    child("--phase", "generate")
    ref = REFERENCE
    plan = [("size", k, ref["master"], ref["shuffle"], ref["memory"]) for k in (1, 10, 30, 100)]
    plan += [("cores", 30, "local[1]", ref["shuffle"], ref["memory"])]
    plan += [("shuffle", 30, ref["master"], s, ref["memory"]) for s in (2, 4, 16, 64, 200)]
    plan += [("memory", 30, ref["master"], ref["shuffle"], m) for m in ("1g", "4g")]
    for series, k, master, shuffle, memory in plan:
        tag = f"{series}_k{k}_{master.replace('[', '').replace(']', '')}_sp{shuffle}_{memory}"
        print(f"{tag}", flush=True)
        child("--phase", "measure", "--series", series, "--tag", tag, "--factor", str(k), "--master", master,
              "--shuffle", str(shuffle), "--memory", memory, "--repeats", str(repeats))
    print("faostat", flush=True)
    child("--phase", "faostat", "--repeats", str(repeats))
    report()


# --------------------------------------------------------------------------- report
def report() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    runs = [json.loads(l) for l in (OUT / "runs.jsonl").read_text(encoding="utf-8").splitlines() if l.strip()]
    timed = [r for r in runs if not r["warmup"]]
    groups: dict[str, list[dict]] = {}
    for r in timed:
        groups.setdefault(r["tag"], []).append(r)
    summary = []
    for tag, rs in groups.items():
        first = rs[0]
        totals = [r["total_s"] for r in rs]
        checked = next((r for r in runs if r["tag"] == tag and "rows_ok" in r), {})
        entry = {"tag": tag, "series": first["series"], "repeats": len(rs), "median_s": round(median(totals), 3),
                 "mean_s": round(mean(totals), 3), "sd_s": round(stdev(totals), 3) if len(totals) > 1 else 0.0,
                 "min_s": min(totals), "max_s": max(totals)}
        if first["series"] == "faostat_query":
            entry.update(domain=first["domain"], format=first["format"], result_rows=first["result_rows"],
                         input_bytes_on_disk=first["input_bytes_on_disk"])
        else:
            entry.update(factor=first["factor"], rows=first["rows"], master=first["master"],
                         shuffle_partitions=first["shuffle_partitions"], driver_memory=first["driver_memory"],
                         stage1_median_s=round(median(r["stage1_daily_to_cell_month_s"] for r in rs), 3),
                         stage2_median_s=round(median(r["stage2_cell_month_to_national_s"] for r in rs), 3),
                         rows_per_s_median=round(first["rows"] / median(totals), 0),
                         shuffle_write_mb=round(median(r["shuffle_write_bytes"] for r in rs) / 1e6, 1),
                         spilled_mb=round(median(r["memory_spilled_bytes"] + r["disk_spilled_bytes"] for r in rs) / 1e6, 1),
                         tasks=rs[0]["tasks"], rows_ok=checked.get("rows_ok"),
                         series0_matches_d9b=checked.get("series0_matches_d9b"))
        summary.append(entry)
    ref_tag = next(s["tag"] for s in summary if s["series"] == "size" and s["factor"] == 30)
    ref_median = next(s["median_s"] for s in summary if s["tag"] == ref_tag)
    for s in summary:
        if s["series"] in ("cores", "shuffle", "memory"):
            s["speedup_vs_reference_k30"] = round(ref_median / s["median_s"], 3)
    one_core = next((s for s in summary if s["series"] == "cores"), None)
    if one_core:
        speedup = one_core["median_s"] / ref_median
        one_core["note"] = f"local[2] speed-up over local[1] = {speedup:.2f}x, parallel efficiency {speedup / 2:.0%}"
    workloads = {p.stem: json.loads(p.read_text(encoding="utf-8")) for p in sorted(WORK.glob("workload_k*.json"))}
    machine = {"os": platform.platform(), "cpu_count": os.cpu_count(), "python": platform.python_version()}
    try:
        import pyspark
        machine["pyspark"] = pyspark.__version__
        machine["ram_gb"] = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9, 1)
    except (ImportError, ValueError, AttributeError, OSError):
        pass
    (OUT / "summary.json").write_text(json.dumps({"generated_utc": datetime.now(timezone.utc).isoformat(),
                                                  "machine": machine, "reference_config": REFERENCE,
                                                  "workloads": workloads, "configs": summary}, indent=2) + "\n", encoding="utf-8")
    keys = sorted({k for s in summary for k in s})
    with (OUT / "summary.csv").open("w", encoding="utf-8", newline="") as stream:
        w = csv.DictWriter(stream, fieldnames=keys, lineterminator="\n")
        w.writeheader()
        w.writerows(summary)

    # ---- plots (same palette as the Week 4 review figures)
    ink, muted, accent, accent2, surface = "#1f2933", "#7b8794", "#2563eb", "#d97706", "#ffffff"
    plt.rcParams.update({"font.size": 10, "axes.edgecolor": muted, "axes.labelcolor": ink, "xtick.color": ink,
                         "ytick.color": ink, "axes.spines.top": False, "axes.spines.right": False})
    FIGURES.mkdir(parents=True, exist_ok=True)

    size = sorted((s for s in summary if s["series"] == "size"), key=lambda s: s["factor"])
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    rows_m = [s["rows"] / 1e6 for s in size]
    for s, x in zip(size, rows_m):
        rs = [r["total_s"] for r in timed if r["tag"] == s["tag"]]
        a1.scatter([x] * len(rs), rs, color=accent, s=14, alpha=0.6, zorder=3)
    a1.plot(rows_m, [s["median_s"] for s in size], color=accent, marker="o", label="measured (median of repeats)")
    a1.plot(rows_m, [size[0]["median_s"] * x / rows_m[0] for x in rows_m], color=muted, linestyle="--", label="linear from 1×")
    a1.set_xscale("log"); a1.set_yscale("log")
    a1.set_xlabel("daily weather rows (millions, log)"); a1.set_ylabel("runtime, seconds (log)")
    a1.set_title("Runtime vs workload size", color=ink, loc="left"); a1.legend(frameon=False, fontsize=8)
    a2.plot(rows_m, [s["rows_per_s_median"] / 1e6 for s in size], color=accent2, marker="o")
    a2.set_xscale("log"); a2.set_xlabel("daily weather rows (millions, log)"); a2.set_ylabel("million rows per second")
    a2.set_ylim(bottom=0); a2.set_title("Throughput vs workload size", color=ink, loc="left")
    for s, x in zip(size, rows_m):
        a2.annotate(f"{s['factor']}×", (x, s["rows_per_s_median"] / 1e6), textcoords="offset points", xytext=(0, 6),
                    ha="center", fontsize=8, color=muted)
    fig.tight_layout(); fig.savefig(FIGURES / f"week7_batch_size_scaling{SUFFIX}.png", dpi=150, facecolor=surface); plt.close(fig)

    res = [s for s in summary if s["series"] in ("cores", "shuffle", "memory")] + [s for s in summary if s["tag"] == ref_tag]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.4), gridspec_kw={"width_ratios": [2, 6, 3]})
    def bar(ax, items, label_fn, title):
        ax.bar(range(len(items)), [s["median_s"] for s in items],
               yerr=[[s["median_s"] - s["min_s"] for s in items], [s["max_s"] - s["median_s"] for s in items]],
               color=[accent if s["tag"] == ref_tag else "#9fb3c8" for s in items], capsize=3)
        ax.set_xticks(range(len(items)), [label_fn(s) for s in items]); ax.set_title(title, color=ink, loc="left")
    cores = sorted([s for s in res if s["series"] == "cores" or s["tag"] == ref_tag], key=lambda s: s["master"])
    bar(axes[0], cores, lambda s: s["master"], "Cores"); axes[0].set_ylabel("runtime at 30× (s)")
    shuffles = sorted([s for s in res if s["series"] == "shuffle" or s["tag"] == ref_tag], key=lambda s: s["shuffle_partitions"])
    bar(axes[1], shuffles, lambda s: str(s["shuffle_partitions"]), "Shuffle partitions")
    mems = sorted([s for s in res if s["series"] == "memory" or s["tag"] == ref_tag], key=lambda s: int(s["driver_memory"][:-1]))
    bar(axes[2], mems, lambda s: s["driver_memory"], "Driver memory")
    fig.suptitle("One factor at a time at 30× (55.9 million rows); blue = reference local[2], 8 partitions, 2g; bars = median, whiskers = min-max",
                 fontsize=9, color=muted, x=0.01, ha="left")
    fig.tight_layout(); fig.savefig(FIGURES / f"week7_batch_resources{SUFFIX}.png", dpi=150, facecolor=surface); plt.close(fig)

    fao = [s for s in summary if s["series"] == "faostat_query"]
    if fao:
        fig, ax = plt.subplots(figsize=(6, 3.2))
        domains = ["QCL", "TCL", "PP"]
        for j, fmt in enumerate(("csv", "parquet")):
            vals = [next(s["median_s"] for s in fao if s["domain"] == d and s["format"] == fmt) for d in domains]
            ax.bar([i + (j - 0.5) * 0.38 for i in range(3)], vals, width=0.38, color=[muted, accent][j], label=fmt.upper())
        ax.set_xticks(range(3), ["Production (545 MB CSV)", "Trade (2.4 GB CSV)", "Prices (214 MB CSV)"])
        ax.set_ylabel("query time, seconds (median)"); ax.legend(frameon=False)
        ax.set_title("Same query on CSV vs year-partitioned Parquet", color=ink, loc="left")
        fig.tight_layout(); fig.savefig(FIGURES / f"week7_faostat_csv_vs_parquet{SUFFIX}.png", dpi=150, facecolor=surface); plt.close(fig)
    print(json.dumps(summary, indent=1))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--phase", choices=["all", "generate", "measure", "faostat", "report"], default="all")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--series", default="size")
    parser.add_argument("--tag", default="adhoc")
    parser.add_argument("--factor", type=int, default=1)
    parser.add_argument("--master", default=REFERENCE["master"])
    parser.add_argument("--shuffle", type=int, default=REFERENCE["shuffle"])
    parser.add_argument("--memory", default=REFERENCE["memory"])
    parser.add_argument("--factors", type=int, nargs="+", default=[1, 10, 30, 100])
    parser.add_argument("--output-suffix", default=None,
                        help="e.g. _laptop: write to week7_batch_scaling_laptop/ and *_laptop.png figures")
    args = parser.parse_args()
    if args.output_suffix is not None:
        global SUFFIX, OUT
        if args.output_suffix and not args.output_suffix.replace("_", "").replace("-", "").isalnum():
            parser.error("--output-suffix may hold only letters, digits, _ and -")
        SUFFIX = os.environ["FOODSHIELD_W7_OUTPUT_SUFFIX"] = args.output_suffix
        OUT = ROOT / "data" / "processed" / f"week7_batch_scaling{SUFFIX}"
    OUT.mkdir(parents=True, exist_ok=True)
    WORK.mkdir(parents=True, exist_ok=True)
    if args.phase == "all":
        run_all(args.repeats)
    elif args.phase == "generate":
        generate(args.factors)
    elif args.phase == "measure":
        measure(args)
    elif args.phase == "faostat":
        measure_faostat(args)
    else:
        report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
