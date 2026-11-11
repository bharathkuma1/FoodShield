# FoodShield software prerequisites

This is the team setup reference for the current wheat-only project. It lists common software separately from platform-specific setup and tools needed only in later weeks. It does not authorize or perform installations.

## Core tools for Weeks 1–5

| Software | Required baseline | Notes and trusted source |
|---|---|---|
| Git | A maintained Git version | Needed to clone, branch, commit, and pull shared code. [Official Git install page](https://git-scm.com/install/). Clone the team's working branch: `feature/workinprogress_v1`. |
| Python | Python 3.11 or later | Required by the project's pinned Spark 4.3.0. The maintainer's current machine uses Python 3.13.5; each teammate should use a supported version and keep driver/worker Python versions aligned. [Python for Windows](https://www.python.org/downloads/windows/). |
| Java JDK | Java 17 or later | Required by PySpark 4.x. Set `JAVA_HOME` if Java is not already on `PATH`. [Eclipse Temurin JDK 17 downloads](https://adoptium.net/temurin/releases/?version=17). |
| PySpark | 4.3.0 | Pinned in `requirements.txt`; install inside a project virtual environment. No separate Spark download is needed for this local PySpark workflow. [Spark 4.3.0 installation guide](https://spark.apache.org/docs/latest/api/python/getting_started/install.html). |
| PowerShell or terminal | PowerShell on Windows; a shell on other systems | Run repository commands and set per-session environment variables. Windows PowerShell is included with Windows. |

The current official Spark guide specifies Python 3.11+ and Java 17+ for PySpark 4.3.0. Use `requirements.txt` as the project's Python dependency contract; do not add packages ad hoc without recording the reason and updating the shared dependency file.

## Windows-only Hadoop support

The current Week 2 runner was exercised on Windows with Hadoop 3.5.0 native files. On Windows, configure `HADOOP_HOME` to a compatible Hadoop home containing `bin\winutils.exe` and `bin\hadoop.dll`. The maintainer's build lives in a folder on one machine; use a path that suits your own computer (the laptop scripts use `C:\hadoop`). Each Windows user needs their own compatible setup. If building the Windows native components from source, the maintainer's process used Visual Studio 2022 Build Tools/MSVC, the Windows SDK, and CMake; those compiler tools are needed only for that build step, not ordinary project coding. Refer to [Apache Hadoop's Windows guidance](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems) and use Apache source releases.

The Hadoop Windows files are not included in Git. Windows-specific native files are not needed on Linux or macOS; those platforms still need compatible Java/Python and have not yet been validated by this repository's Week 2 run instructions. Windows 11 is the current documented host, not a project-wide OS requirement.

## Additional tools for later milestones

| Milestone | Additional software | Status |
|---|---|---|
| Week 6 — streaming replay | **Pinned (D12):** Apache Kafka 4.4.0 (`kafka_2.13-4.4.0.tgz`, KRaft, one broker); Spark Kafka connector `spark-sql-kafka-0-10_2.13-4.3.0` with `spark-token-provider-kafka-0-10_2.13-4.3.0`, `kafka-clients-3.9.2` and `commons-pool2-2.13.1`; Python `kafka-python==3.0.13` (in `requirements.txt`). | Put the archive and the four jars in a `Kafka` folder in the repository (Git-ignored); `run_week6_replay.py` checks the archive's SHA-512 and unpacks it. Java 17+ is required by Kafka 4.x. On Windows, `HADOOP_HOME` (above) is also required, because Spark writes streaming checkpoints. Setup and commands: [`WEEK6_STREAMING.md`](WEEK6_STREAMING.md). Sources: [Kafka downloads](https://kafka.apache.org/community/downloads/), [Maven Central](https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.3.0/), [Spark's Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html). |
| Week 8 — dashboard/demo | Static HTML dashboard (D15); live mode uses only the Python standard library | Nothing extra to install. Live demo: `RUN_DEMO_LIVE.bat` (needs the Week 6 laptop setup). |

Docker, cloud accounts, paid services, Maven, Scala, Jupyter, and separate data-visualization packages are not current prerequisites (D12 chose native Kafka without Docker). Add one only if a documented implementation decision requires it.

## Hardware, operating system, and storage are per-machine

There is no agreed project-wide minimum RAM, processor, operating system, or drive letter. The current maintainer develops on Windows 11 with 8 GB RAM; these are personal machine details, not requirements for teammates. Use a local path appropriate to your computer. Keep acquired/processed data local except the small, explicitly allowlisted and attributed snapshots; record OS/CPU/RAM/runtime details for each scalability result. Tune Spark settings to the host while keeping a fixed reference workload/configuration when comparing runs.

## Get the code and prepare the Python environment

From a terminal with Git and Python available:

```powershell
git clone --branch feature/workinprogress_v1 https://github.com/bharathkuma1/FoodShield.git
cd FoodShield
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

The activation command above is for Windows PowerShell. On Linux/macOS, activate with `source .venv/bin/activate` and use the same `python -m pip install -r requirements.txt` command.

Verify versions:

```powershell
git --version
python --version
java -version
```

For a Windows Week 2 run, set `HADOOP_HOME` to that machine's compatible Hadoop 3.5.0 native home. Run `python scripts\fetch_week2_inputs.py` to populate missing inputs from the URLs and filters in `configs/week2.local.json`; reviewed FAOSTAT/NASA snapshots are already versioned, while the FPMA response is fetched locally because partner reuse rights remain unresolved. The script never overwrites existing source files and records retrieval hashes in `data/raw/retrieval_manifest.json`. Then run `python scripts\run_week2_etl.py`. See `docs/DATA_SOURCE_REGISTER.md` for source terms and caveats. The synthetic fixture can unblock Week 3 plumbing, but is never final evaluation data; see `docs/SYNTHETIC_DATA.md`.

## Current maintainer reference profile

These values describe the machine used for the documented Week 2 local run; they are evidence for reproducing that run, not a team-wide hardware specification:

- Windows 11, 8 GB RAM;
- Python 3.13.5, PySpark 4.3.0, Java 17;
- Hadoop 3.5.0 Windows native files in the maintainer's local Hadoop folder;
- Spark local mode `local[2]`, 1 GiB driver memory, and four SQL shuffle partitions.

The local configuration is in `configs/week2.local.json`; outputs and counts are described in `docs/WEEK2_ETL.md`. Future scalability work should record each host's actual environment and distinguish the fixed comparison workload from host-specific tuning.

## Official documentation

- [Apache Spark 4.3.0](https://spark.apache.org/docs/4.3.0/)
- [PySpark installation and supported runtime requirements](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Apache Spark Structured Streaming + Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Hadoop](https://hadoop.apache.org/docs/current/)
