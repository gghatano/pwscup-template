"""Docker実行ランナー.

参加者コードをDockerコンテナまたはsubprocessで安全に実行する。
Docker未インストール時はsubprocessフォールバックを使用。
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pwscup.config import ContestConfig, ResourceConstraints

logger = logging.getLogger(__name__)

SANDBOX_IMAGE = "pwscup-sandbox"


def _is_docker_available() -> bool:
    """Dockerが利用可能か確認する."""
    try:
        result = subprocess.run(
            ["docker", "version"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _docker_image_exists(image: str = SANDBOX_IMAGE) -> bool:
    """Dockerイメージが存在するか確認する."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


@dataclass
class RunResult:
    """実行結果."""

    status: str  # "success", "timeout", "error", "oom"
    stdout: str = ""
    stderr: str = ""
    execution_time_sec: float = 0.0
    memory_peak_mb: float = 0.0
    output_files: list[str] = field(default_factory=list)


class DockerRunner:
    """参加者コードの実行ランナー."""

    def __init__(
        self,
        config: Optional[ContestConfig] = None,
        force_subprocess: bool = False,
    ) -> None:
        self.config = config or ContestConfig()
        self.use_docker = (
            not force_subprocess
            and _is_docker_available()
            and _docker_image_exists()
        )
        if not self.use_docker:
            logger.warning(
                "Docker未使用: subprocessフォールバック（セキュリティ制限なし）"
            )

    def run_anonymization(
        self,
        submission_dir: Path,
        input_csv: Path,
        schema_path: Path,
        output_dir: Path,
    ) -> RunResult:
        """匿名化アルゴリズムを実行する."""
        constraints = self.config.constraints.anonymize
        output_dir.mkdir(parents=True, exist_ok=True)

        if self.use_docker:
            return self._run_docker_anon(
                submission_dir, input_csv, schema_path, output_dir, constraints
            )
        else:
            output_csv = output_dir / "anonymized.csv"
            return self._run_subprocess(
                submission_dir, input_csv, schema_path, output_csv, constraints, "anonymize"
            )

    def run_reidentification(
        self,
        submission_dir: Path,
        anon_csv: Path,
        auxiliary_csv: Path,
        schema_path: Path,
        output_dir: Path,
    ) -> RunResult:
        """再識別アルゴリズムを実行する."""
        constraints = self.config.constraints.reidentify
        output_dir.mkdir(parents=True, exist_ok=True)

        if self.use_docker:
            return self._run_docker_reid(
                submission_dir, anon_csv, auxiliary_csv, schema_path, output_dir, constraints
            )
        else:
            output_json = output_dir / "mappings.json"
            return self._run_subprocess_reid(
                submission_dir, anon_csv, auxiliary_csv, schema_path, output_json, constraints
            )

    # --- Docker実行 ---

    def _run_docker_anon(
        self,
        submission_dir: Path,
        input_csv: Path,
        schema_path: Path,
        output_dir: Path,
        constraints: ResourceConstraints,
    ) -> RunResult:
        """Dockerコンテナで匿名化を実行する."""
        # 入力ディレクトリを一時作成（data.csv + schema.json）
        input_dir = Path(tempfile.mkdtemp())
        shutil.copy(input_csv, input_dir / "data.csv")
        shutil.copy(schema_path, input_dir / "schema.json")

        script = (
            "import sys; sys.path.insert(0, '/submission'); "
            "from algorithm import anonymize; "
            "anonymize('/input/data.csv', '/input/schema.json', '/output/anonymized.csv')"
        )

        result = self._run_docker_container(
            script, submission_dir, input_dir, output_dir, constraints
        )

        # クリーンアップ
        shutil.rmtree(input_dir, ignore_errors=True)

        if result.status == "success":
            out_file = output_dir / "anonymized.csv"
            if out_file.exists():
                result.output_files = [str(out_file)]

        return result

    def _run_docker_reid(
        self,
        submission_dir: Path,
        anon_csv: Path,
        auxiliary_csv: Path,
        schema_path: Path,
        output_dir: Path,
        constraints: ResourceConstraints,
    ) -> RunResult:
        """Dockerコンテナで再識別を実行する."""
        input_dir = Path(tempfile.mkdtemp())
        shutil.copy(anon_csv, input_dir / "anon.csv")
        shutil.copy(auxiliary_csv, input_dir / "auxiliary.csv")
        shutil.copy(schema_path, input_dir / "schema.json")

        script = (
            "import sys; sys.path.insert(0, '/submission'); "
            "from algorithm import reidentify; "
            "reidentify('/input/anon.csv', '/input/auxiliary.csv', "
            "'/input/schema.json', '/output/mappings.json')"
        )

        result = self._run_docker_container(
            script, submission_dir, input_dir, output_dir, constraints
        )

        shutil.rmtree(input_dir, ignore_errors=True)

        if result.status == "success":
            out_file = output_dir / "mappings.json"
            if out_file.exists():
                result.output_files = [str(out_file)]

        return result

    def _run_docker_container(
        self,
        script: str,
        submission_dir: Path,
        input_dir: Path,
        output_dir: Path,
        constraints: ResourceConstraints,
    ) -> RunResult:
        """Dockerコンテナを起動してスクリプトを実行する."""
        cmd = [
            "docker", "run",
            "--rm",
            "--network=none",
            f"--memory={constraints.memory_limit_mb}m",
            f"--cpus={constraints.cpu_cores}",
            "--pids-limit=64",
            "--security-opt=no-new-privileges",
            "-v", f"{submission_dir.resolve()}:/submission:ro",
            "-v", f"{input_dir.resolve()}:/input:ro",
            "-v", f"{output_dir.resolve()}:/output",
            SANDBOX_IMAGE,
            "-c", script,
        ]

        start_time = time.time()
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=constraints.time_limit_sec + 10,  # Docker起動分の余裕
            )
            execution_time = time.time() - start_time

            if result.returncode == 137:
                return RunResult(
                    status="oom",
                    stderr="メモリ制限超過（OOM Kill）",
                    execution_time_sec=execution_time,
                )

            if result.returncode != 0:
                return RunResult(
                    status="error",
                    stdout=result.stdout[:2000],
                    stderr=result.stderr[:2000],
                    execution_time_sec=execution_time,
                )

            return RunResult(
                status="success",
                stdout=result.stdout,
                stderr=result.stderr,
                execution_time_sec=execution_time,
            )

        except subprocess.TimeoutExpired:
            return RunResult(
                status="timeout",
                stderr=f"実行時間上限({constraints.time_limit_sec}秒)超過",
                execution_time_sec=float(constraints.time_limit_sec),
            )

    # --- subprocess フォールバック ---

    def _run_subprocess(
        self,
        submission_dir: Path,
        input_csv: Path,
        schema_path: Path,
        output_csv: Path,
        constraints: ResourceConstraints,
        func_name: str,
    ) -> RunResult:
        """subprocessで匿名化を実行する."""
        script = (
            f"import sys; sys.path.insert(0, '{submission_dir}'); "
            f"from algorithm import {func_name}; "
            f"{func_name}('{input_csv}', '{schema_path}', '{output_csv}')"
        )
        return self._exec(script, constraints, [str(output_csv)])

    def _run_subprocess_reid(
        self,
        submission_dir: Path,
        anon_csv: Path,
        auxiliary_csv: Path,
        schema_path: Path,
        output_json: Path,
        constraints: ResourceConstraints,
    ) -> RunResult:
        """subprocessで再識別を実行する."""
        script = (
            f"import sys; sys.path.insert(0, '{submission_dir}'); "
            f"from algorithm import reidentify; "
            f"reidentify('{anon_csv}', '{auxiliary_csv}', '{schema_path}', '{output_json}')"
        )
        return self._exec(script, constraints, [str(output_json)])

    def _exec(
        self, script: str, constraints: ResourceConstraints, output_files: list[str]
    ) -> RunResult:
        """Pythonスクリプトを実行する."""
        start_time = time.time()
        try:
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                text=True,
                timeout=constraints.time_limit_sec,
            )
            execution_time = time.time() - start_time

            if result.returncode != 0:
                return RunResult(
                    status="error",
                    stdout=result.stdout,
                    stderr=result.stderr[:2000],
                    execution_time_sec=execution_time,
                )

            existing_outputs = [f for f in output_files if Path(f).exists()]
            return RunResult(
                status="success",
                stdout=result.stdout,
                stderr=result.stderr,
                execution_time_sec=execution_time,
                output_files=existing_outputs,
            )

        except subprocess.TimeoutExpired:
            return RunResult(
                status="timeout",
                stderr=f"実行時間上限({constraints.time_limit_sec}秒)超過",
                execution_time_sec=float(constraints.time_limit_sec),
            )
