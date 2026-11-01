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

The current Week 2 runner was exercised on Windows with Hadoop 3.5.0 native files. On Windows, configure `HADOOP_HOME` to a compatible Hadoop home containing `bin\winutils.exe` and `bin\hadoop.dll`. The maintainer's build is located at `D:\Bharath\Installations\Hadoop-3.5.0-source-build\hadoop-home`; this is a path on one machine and is not a path teammates should copy literally. Each Windows user needs their own compatible setup. If building the Windows native components from source, the maintainer's process used Visual Studio 2022 Build Tools/MSVC, the Windows SDK, and CMake; those compiler tools are needed only for that build step, not ordinary project coding. Refer to [Apache Hadoop's Windows guidance](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems) and use Apache source releases.

The Hadoop Windows files are not included in Git. Windows-specific native files are not needed on Linux or macOS; those platforms still need compatible Java/Python and have not yet been validated by this repository's Week 2 run instructions. Windows 11 is the current documented host, not a project-wide OS requirement.

## Additional tools for later milestones

| Milestone | Additional software | Status |
|---|---|---|
| Week 6 — streaming replay | Local Apache Kafka broker and the Spark Structured Streaming Kafka connector matching Spark 4.3.0 | Required for the TA replay demonstration, but the broker version and connector configuration are not yet pinned. Confirm them with a bounded local integration run before standardizing the team setup. See [Spark's Kafka integration guide](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) and [Apache Kafka documentation](https://kafka.apache.org/documentation/). |
| Week 8 — dashboard/demo | No dashboard framework selected | Do not install a dashboard tool until the team picks one based on the final report/demo needs. |

Docker, cloud accounts, paid services, Maven, Scala, Jupyter, and separate data-visualization packages are not current prerequisites. Add one only if a documented implementation decision requires it.

## Hardware, operating system, and storage are per-machine

There is no agreed project-wide minimum RAM, processor, operating system, or drive letter. The current maintainer develops on Windows 11 with 8 GB RAM and prefers `D:\Bharath\Installations` for software; these are personal machine details, not requirements for teammates. Use a local path appropriate to your computer, keep raw/processed data out of Git, and record OS/CPU/RAM/runtime details for each scalability result. Tune Spark settings to the host while keeping a fixed reference workload/configuration when comparing runs.

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

For a Windows Week 2 run, set `HADOOP_HOME` to that machine's compatible Hadoop 3.5.0 native home, then run `python scripts\run_week2_etl.py`. The ETL also needs the four input files configured in `configs/week2.local.json`; raw downloads are intentionally not in Git. Follow `docs/DATA_SOURCE_REGISTER.md` for the recorded URLs, retrieval steps, and reuse caveats. The committed synthetic fixture can be used to develop Week 3 plumbing before local raw inputs are ready; it is never final evaluation data. See `docs/SYNTHETIC_DATA.md`.

## Current maintainer reference profile

These values describe the machine used for the documented Week 2 local run; they are evidence for reproducing that run, not a team-wide hardware specification:

- Windows 11, 8 GB RAM;
- Python 3.13.5, PySpark 4.3.0, Java 17;
- Hadoop 3.5.0 Windows native files under the maintainer's `D:\Bharath\Installations` folder;
- Spark local mode `local[2]`, 1 GiB driver memory, and four SQL shuffle partitions.

The local configuration is in `configs/week2.local.json`; outputs and counts are described in `docs/WEEK2_ETL.md`. Future scalability work should record each host's actual environment and distinguish the fixed comparison workload from host-specific tuning.

## Official documentation

- [Apache Spark 4.3.0](https://spark.apache.org/docs/4.3.0/)
- [PySpark installation and supported runtime requirements](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Apache Spark Structured Streaming + Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Apache Kafka](https://kafka.apache.org/documentation/)
- [Apache Hadoop](https://hadoop.apache.org/docs/current/)
