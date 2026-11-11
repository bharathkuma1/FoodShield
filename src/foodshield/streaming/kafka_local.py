"""Start and stop one local Apache Kafka broker in KRaft mode (decision D12a), from Python.

Java is launched directly (``java -cp <kafka>/libs/* kafka.Kafka ...``) instead of
through Kafka's ``bin\\windows\\*.bat`` scripts, because those scripts break on
folder paths with spaces and their stop script relies on ``wmic``, which recent
Windows 11 builds no longer include. The same code runs on Linux.

The broker listens on 127.0.0.1 only. Retention is unlimited so Kafka never
deletes segment files during a run (file deletion is the fragile part of Kafka
on Windows); ``reset`` removes the whole data folder while the broker is stopped.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import Any

from foodshield.streaming.winsafe import kill_tree


def java_executable() -> str:
    home = os.environ.get("JAVA_HOME")
    if home:
        candidate = Path(home) / "bin" / ("java.exe" if os.name == "nt" else "java")
        if candidate.exists():
            return str(candidate)
    found = shutil.which("java")
    if not found:
        sys.exit("Java 17 or later is required for Kafka and Spark. Install it and set JAVA_HOME (see docs/WEEK6_STREAMING.md).")
    return found


def ensure_kafka_home(archive: Path, home: Path) -> Path:
    """Unpack the verified Kafka archive once (Python's tarfile works on Windows too)."""
    if (home / "libs").is_dir():
        return home
    if not archive.exists():
        sys.exit(f"Kafka archive not found: {archive}")
    with tarfile.open(archive, "r:gz") as tar:
        top = {m.name.split("/")[0] for m in tar.getmembers()}
        if top != {home.name}:
            raise ValueError(f"Unexpected archive layout {top}")
        tar.extractall(home.parent, filter="data")
    return home


def _classpath(home: Path) -> str:
    return str(home / "libs" / "*")


def write_properties(cfg: dict[str, Any], data_dir: Path) -> Path:
    host, port = cfg["bootstrap"].split(":")
    controller = int(cfg["controller_port"])
    props = {
        "process.roles": "broker,controller", "node.id": "1",
        "controller.quorum.bootstrap.servers": f"{host}:{controller}",
        "listeners": f"PLAINTEXT://{host}:{port},CONTROLLER://{host}:{controller}",
        "advertised.listeners": f"PLAINTEXT://{host}:{port}",
        "inter.broker.listener.name": "PLAINTEXT", "controller.listener.names": "CONTROLLER",
        "listener.security.protocol.map": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "log.dirs": (data_dir / "logs").as_posix(),
        "num.partitions": "1", "offsets.topic.replication.factor": "1",
        "share.coordinator.state.topic.replication.factor": "1", "share.coordinator.state.topic.min.isr": "1",
        "transaction.state.log.replication.factor": "1", "transaction.state.log.min.isr": "1",
        "log.retention.ms": "-1", "log.retention.bytes": "-1", "auto.create.topics.enable": "false",
        "group.initial.rebalance.delay.ms": "0",
    }
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / "server.properties"
    path.write_text("".join(f"{k}={v}\n" for k, v in props.items()), encoding="utf-8")
    return path


def _port_open(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex((host, port)) == 0


class LocalKafka:
    def __init__(self, root: Path, cfg: dict[str, Any]):
        self.cfg = cfg
        self.home = ensure_kafka_home(root / cfg["archive"], root / cfg["home"])
        self.data_dir = root / cfg["data_dir"]
        self.java = java_executable()
        self.process: subprocess.Popen | None = None
        self.log = None

    def _java(self, main: str, *args: str, heap: str = "-Xmx256m") -> list[str]:
        return [self.java, *heap.split(), f"-Dlog4j2.configurationFile={(self.home / 'config' / 'log4j2.yaml').as_posix()}",
                f"-Dkafka.logs.dir={(self.data_dir / 'server-logs').as_posix()}", "-cp", _classpath(self.home), main, *args]

    def format_storage(self) -> None:
        props = write_properties(self.cfg, self.data_dir)
        if (self.data_dir / "logs" / "meta.properties").exists():
            return
        cluster = subprocess.run(self._java("kafka.tools.StorageTool", "random-uuid"), capture_output=True, text=True,
                                 check=True).stdout.strip().splitlines()[-1]
        done = subprocess.run(self._java("kafka.tools.StorageTool", "format", "--standalone", "-t", cluster, "-c", str(props)),
                              capture_output=True, text=True)
        if done.returncode != 0:
            sys.exit(f"Kafka storage format failed in {self.data_dir}:\n{(done.stderr or done.stdout).strip()[-2000:]}")

    def start(self, timeout: float = 90.0) -> None:
        host, port = self.cfg["bootstrap"].split(":")
        if _port_open(host, int(port)):
            sys.exit(f"Something is already listening on {self.cfg['bootstrap']}; stop it first.")
        self.format_storage()
        (self.data_dir / "server-logs").mkdir(parents=True, exist_ok=True)
        self.log = open(self.data_dir / "broker-console.log", "ab")
        self.process = subprocess.Popen(self._java("kafka.Kafka", str(self.data_dir / "server.properties"),
                                                   heap=self.cfg.get("heap", "-Xmx512m -Xms512m")),
                                        stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.process.poll() is not None:
                sys.exit(f"Kafka exited during start-up; see {self.data_dir / 'broker-console.log'}")
            if _port_open(host, int(port)):
                time.sleep(2)  # let the controller finish electing itself
                return
            time.sleep(1)
        self.stop()
        sys.exit("Kafka did not start in time")

    def stop(self) -> None:
        if self.process and self.process.poll() is None:
            if os.name == "nt":
                kill_tree(self.process)  # TerminateProcess; also ends anything the broker started
            else:
                self.process.terminate()
                try:
                    self.process.wait(30)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(10)
        if self.log:
            self.log.close()
        self.process = None

    def reset(self) -> None:
        if self.process:
            raise RuntimeError("Stop Kafka before reset")
        shutil.rmtree(self.data_dir, ignore_errors=True)
        if self.data_dir.exists():
            # Windows: a file is still held open (for example by a broker from an earlier run); use a fresh folder.
            self.data_dir = self.data_dir.with_name(f"{self.data_dir.name}-{int(time.time())}")
            print(f"Previous Kafka data could not be removed; using {self.data_dir}", flush=True)


def create_topics(bootstrap: str, topics: dict[str, dict[str, Any]]) -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="foodshield-admin")
    try:
        existing = set(admin.list_topics())
        new = [NewTopic(t["name"], num_partitions=int(t["partitions"]), replication_factor=1)
               for t in topics.values() if t["name"] not in existing]
        if new:
            try:
                admin.create_topics(new)
            except TopicAlreadyExistsError:
                pass
    finally:
        admin.close()
