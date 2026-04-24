"""Subprocess manager for simulation runs.

Tracks running subprocesses by run_id (uuid4). Spawns
``scripts/run_scenario.py`` as a detached subprocess, persists state
to ``ui/backend/run_registry.json`` so the backend survives restarts.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = PROJECT_ROOT / "ui" / "backend" / "run_registry.json"
PYTHON = str(Path.home() / "venvs" / "aragen" / "bin" / "python")
RUN_SCRIPT = str(PROJECT_ROOT / "scripts" / "run_scenario.py")


@dataclass
class RunInfo:
    """Metadata for a single simulation run."""

    run_id: str
    scenario_name: str
    scenario_path: str
    seed: int
    no_llm: bool
    pid: int | None
    output_dir: str
    status: Literal["running", "completed", "failed"] = "running"
    start_time: str = ""
    end_time: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RunInfo":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class RunManager:
    """Singleton manager for simulation subprocess lifecycle."""

    _instance: "RunManager | None" = None

    def __init__(self) -> None:
        self._runs: dict[str, RunInfo] = {}
        self._procs: dict[str, subprocess.Popen] = {}
        self._load_registry()

    @classmethod
    def get(cls) -> "RunManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start_run(
        self,
        scenario_path: str,
        seed: int = 42,
        no_llm: bool = False,
    ) -> tuple[str, int, str]:
        """Launch a simulation subprocess.

        Returns (run_id, pid, output_dir).
        """
        run_id = uuid.uuid4().hex[:12]
        output_dir = str(PROJECT_ROOT / "results" / f"run_{run_id}")
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        log_path = Path(output_dir) / "run.log"

        cmd = [
            PYTHON,
            RUN_SCRIPT,
            str(scenario_path),
            "--seed",
            str(seed),
            "--output-dir",
            output_dir,
        ]
        if no_llm:
            cmd.append("--no-llm")

        log_file = open(log_path, "w")
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=str(PROJECT_ROOT),
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )

        # Derive scenario name from path
        try:
            scenario_name = Path(scenario_path).stem
        except Exception:
            scenario_name = scenario_path

        info = RunInfo(
            run_id=run_id,
            scenario_name=scenario_name,
            scenario_path=str(scenario_path),
            seed=seed,
            no_llm=no_llm,
            pid=proc.pid,
            output_dir=output_dir,
            status="running",
            start_time=datetime.now(timezone.utc).isoformat(),
        )

        self._runs[run_id] = info
        self._procs[run_id] = proc
        self._save_registry()

        logger.info("Started run %s (pid=%s, output=%s)", run_id, proc.pid, output_dir)
        return run_id, proc.pid, output_dir

    def list_runs(self) -> list[RunInfo]:
        """Return all runs, polling status for running ones."""
        for rid in list(self._runs):
            self._poll_status(rid)
        return list(self._runs.values())

    def get_run(self, run_id: str) -> RunInfo | None:
        """Get a single run, refreshing its status."""
        if run_id not in self._runs:
            return None
        self._poll_status(run_id)
        return self._runs[run_id]

    def stop_run(self, run_id: str) -> bool:
        """Stop a running simulation. Returns True if stopped."""
        info = self._runs.get(run_id)
        if info is None or info.status != "running":
            return False

        proc = self._procs.get(run_id)
        if proc is not None:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        elif info.pid:
            try:
                os.killpg(os.getpgid(info.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass

        info.status = "failed"
        info.error = "stopped by user"
        info.end_time = datetime.now(timezone.utc).isoformat()
        self._save_registry()
        return True

    def poll_telemetry(self, run_id: str, since_step: int = 0) -> list[dict]:
        """Read new action rows from the run's actions.parquet.

        Returns rows where step > since_step.
        """
        info = self._runs.get(run_id)
        if info is None:
            return []

        parquet_path = Path(info.output_dir) / "actions.parquet"
        if not parquet_path.exists():
            return []

        try:
            import pyarrow.parquet as pq
            table = pq.read_table(str(parquet_path))
            df = table.to_pandas()
            new_rows = df[df["step"] > since_step]
            return new_rows.to_dict(orient="records")
        except Exception as exc:
            logger.warning("Failed to read telemetry for %s: %s", run_id, exc)
            return []

    def read_parquet_section(self, run_id: str, section: str) -> list[dict]:
        """Read a parquet section as a list of dicts."""
        info = self._runs.get(run_id)
        if info is None:
            return []

        parquet_path = Path(info.output_dir) / f"{section}.parquet"
        if not parquet_path.exists():
            return []

        try:
            import pyarrow.parquet as pq
            table = pq.read_table(str(parquet_path))
            df = table.to_pandas()
            # Convert NaN to None for JSON serialization
            return json.loads(df.to_json(orient="records"))
        except Exception as exc:
            logger.warning("Failed to read %s for %s: %s", section, run_id, exc)
            return []

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _poll_status(self, run_id: str) -> None:
        """Update status by checking if the subprocess is still alive."""
        info = self._runs.get(run_id)
        if info is None or info.status != "running":
            return

        proc = self._procs.get(run_id)
        if proc is not None:
            rc = proc.poll()
            if rc is not None:
                info.status = "completed" if rc == 0 else "failed"
                if rc != 0:
                    info.error = f"exit code {rc}"
                info.end_time = datetime.now(timezone.utc).isoformat()
                self._save_registry()
        elif info.pid:
            # Backend restarted — check if PID is still alive
            try:
                os.kill(info.pid, 0)
            except ProcessLookupError:
                # Process gone — check output for parquet to infer success
                out = Path(info.output_dir)
                if (out / "prices.parquet").exists():
                    info.status = "completed"
                else:
                    info.status = "failed"
                    info.error = "process gone after restart"
                info.end_time = datetime.now(timezone.utc).isoformat()
                self._save_registry()
            except PermissionError:
                pass

    def _load_registry(self) -> None:
        """Load persisted run state from JSON."""
        if not REGISTRY_PATH.exists():
            return
        try:
            data = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
            for entry in data:
                info = RunInfo.from_dict(entry)
                self._runs[info.run_id] = info
        except Exception as exc:
            logger.warning("Failed to load run registry: %s", exc)

    def _save_registry(self) -> None:
        """Persist run state to JSON."""
        REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        data = [info.to_dict() for info in self._runs.values()]
        REGISTRY_PATH.write_text(
            json.dumps(data, indent=2, default=str), encoding="utf-8"
        )
