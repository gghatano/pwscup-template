"""Docker実行ランナーのテスト."""

import tempfile
from pathlib import Path

import pytest

from pwscup.sandbox.docker_runner import DockerRunner, _docker_image_exists, _is_docker_available

EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"
DATA_DIR = Path(__file__).parent.parent.parent / "data"
SCHEMA_PATH = DATA_DIR / "schema" / "schema.json"


@pytest.fixture
def runner() -> DockerRunner:
    return DockerRunner()


@pytest.fixture
def subprocess_runner() -> DockerRunner:
    return DockerRunner(force_subprocess=True)


class TestDockerAvailability:
    def test_docker_available(self) -> None:
        assert _is_docker_available()

    def test_image_exists(self) -> None:
        assert _docker_image_exists("pwscup-sandbox")


class TestSubprocessFallback:
    def test_anonymize_subprocess(self, subprocess_runner: DockerRunner) -> None:
        output_dir = Path(tempfile.mkdtemp())
        result = subprocess_runner.run_anonymization(
            submission_dir=EXAMPLES_DIR / "anonymize_example",
            input_csv=DATA_DIR / "sample" / "sample_original.csv",
            schema_path=SCHEMA_PATH,
            output_dir=output_dir,
        )
        assert result.status == "success"
        assert len(result.output_files) > 0
        assert (output_dir / "anonymized.csv").exists()

    def test_reidentify_subprocess(self, subprocess_runner: DockerRunner) -> None:
        # まず匿名化データを生成
        anon_dir = Path(tempfile.mkdtemp())
        subprocess_runner.run_anonymization(
            submission_dir=EXAMPLES_DIR / "anonymize_example",
            input_csv=DATA_DIR / "sample" / "sample_original.csv",
            schema_path=SCHEMA_PATH,
            output_dir=anon_dir,
        )

        output_dir = Path(tempfile.mkdtemp())
        result = subprocess_runner.run_reidentification(
            submission_dir=EXAMPLES_DIR / "reidentify_example",
            anon_csv=anon_dir / "anonymized.csv",
            auxiliary_csv=DATA_DIR / "auxiliary" / "sample_auxiliary.csv",
            schema_path=SCHEMA_PATH,
            output_dir=output_dir,
        )
        assert result.status == "success"
        assert (output_dir / "mappings.json").exists()


class TestDockerExecution:
    def test_anonymize_docker(self, runner: DockerRunner) -> None:
        if not runner.use_docker:
            pytest.skip("Docker not available")
        output_dir = Path(tempfile.mkdtemp())
        result = runner.run_anonymization(
            submission_dir=EXAMPLES_DIR / "anonymize_example",
            input_csv=DATA_DIR / "sample" / "sample_original.csv",
            schema_path=SCHEMA_PATH,
            output_dir=output_dir,
        )
        assert result.status == "success", f"stderr: {result.stderr}"
        assert (output_dir / "anonymized.csv").exists()
        assert result.execution_time_sec > 0

    def test_reidentify_docker(self, runner: DockerRunner) -> None:
        if not runner.use_docker:
            pytest.skip("Docker not available")
        # まず匿名化
        anon_dir = Path(tempfile.mkdtemp())
        runner.run_anonymization(
            submission_dir=EXAMPLES_DIR / "anonymize_example",
            input_csv=DATA_DIR / "sample" / "sample_original.csv",
            schema_path=SCHEMA_PATH,
            output_dir=anon_dir,
        )

        # 再識別（小さいデータで速く）
        import pandas as pd
        anon_small = pd.read_csv(anon_dir / "anonymized.csv").head(30)
        small_dir = Path(tempfile.mkdtemp())
        anon_small.to_csv(small_dir / "anon_small.csv", index=False)

        aux_small = pd.read_csv(DATA_DIR / "auxiliary" / "sample_auxiliary.csv").head(10)
        aux_small.to_csv(small_dir / "aux_small.csv", index=False)

        output_dir = Path(tempfile.mkdtemp())
        result = runner.run_reidentification(
            submission_dir=EXAMPLES_DIR / "reidentify_example",
            anon_csv=small_dir / "anon_small.csv",
            auxiliary_csv=small_dir / "aux_small.csv",
            schema_path=SCHEMA_PATH,
            output_dir=output_dir,
        )
        assert result.status == "success", f"stderr: {result.stderr}"
        assert (output_dir / "mappings.json").exists()
