"""ローカル評価コマンド."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from pwscup.config import load_contest_config
from pwscup.pipeline.orchestrator import PipelineOrchestrator
from pwscup.pipeline.reidentify import load_ground_truth, load_mappings
from pwscup.schema import load_schema

console = Console()

evaluate_app = typer.Typer(help="ローカル評価を実行する")


@evaluate_app.command("anonymize")
def evaluate_anonymize(
    submission_dir: Path = typer.Argument(..., help="提出物ディレクトリのパス"),
    data_dir: Path = typer.Option("data/sample", help="データディレクトリ"),
    schema_path: Path = typer.Option("data/schema/schema.json", help="スキーマファイルパス"),
    config_path: Path = typer.Option("configs/contest.yaml", help="設定ファイルパス"),
) -> None:
    """匿名化アルゴリズムをローカル評価する."""
    # バリデーション
    algorithm_path = submission_dir / "algorithm.py"
    if not algorithm_path.exists():
        console.print(f"[red]エラー: {algorithm_path} が見つかりません[/red]")
        raise typer.Exit(1)

    # 元データの特定
    original_path = _find_original_data(data_dir)
    if original_path is None:
        console.print(f"[red]エラー: {data_dir} に元データCSVが見つかりません[/red]")
        raise typer.Exit(1)

    config = load_contest_config(config_path if config_path.exists() else None)
    orch = PipelineOrchestrator(schema_path, config)

    console.print(f"[bold]匿名化評価開始[/bold]: {submission_dir}")
    console.print(f"  元データ: {original_path}")

    # 匿名化の実行
    sys.path.insert(0, str(submission_dir))
    try:
        import tempfile

        from algorithm import anonymize  # type: ignore[import-not-found]

        output_dir = Path(tempfile.mkdtemp())
        output_csv = output_dir / "anonymized.csv"

        import time

        start = time.time()
        anonymize(str(original_path), str(schema_path), str(output_csv))
        exec_time = time.time() - start

        if not output_csv.exists():
            console.print("[red]エラー: 匿名化出力が生成されませんでした[/red]")
            raise typer.Exit(1)

        anonymized_df = pd.read_csv(output_csv)
        original_df = pd.read_csv(original_path)
        # 識別子カラムを除外
        schema = load_schema(schema_path)
        id_cols = [c.name for c in schema.get_columns_by_role("identifier")]
        original_df = original_df.drop(columns=[c for c in id_cols if c in original_df.columns])

    except Exception as e:
        console.print(f"[red]匿名化実行エラー: {e}[/red]")
        raise typer.Exit(1)
    finally:
        # sys.pathの復元
        if str(submission_dir) in sys.path:
            sys.path.remove(str(submission_dir))
        # importキャッシュのクリア
        if "algorithm" in sys.modules:
            del sys.modules["algorithm"]

    # 評価
    result = orch.evaluate_anonymization_direct(original_df, anonymized_df)

    if not result.success:
        console.print(f"[red]評価エラー: {result.error}[/red]")
        raise typer.Exit(1)

    # 結果表示
    console.print()
    console.print("[bold green]=== ローカル評価結果（匿名化）===[/bold green]")

    table = Table(show_header=True)
    table.add_column("指標", style="cyan")
    table.add_column("値", justify="right")

    assert result.utility is not None
    assert result.safety is not None

    table.add_row("有用性スコア", f"{result.utility.utility_score:.3f}")
    table.add_row("  分布距離", f"{result.utility.distribution_distance:.3f}")
    table.add_row("  相関保存", f"{result.utility.correlation_preservation:.3f}")
    table.add_row("  クエリ精度", f"{result.utility.query_accuracy:.3f}")
    table.add_row("  ML有用性", f"{result.utility.ml_utility:.3f}")
    table.add_row("安全性スコア(S_auto)", f"{result.safety.safety_score_auto:.3f}")
    k_val = result.safety.k_anonymity
    k_sc = result.safety.k_score
    table.add_row("  k-匿名性", f"k={k_val} (スコア: {k_sc:.2f})")
    l_val = result.safety.l_diversity
    l_sc = result.safety.l_score
    table.add_row("  l-多様性", f"l={l_val} (スコア: {l_sc:.2f})")
    t_val = result.safety.t_closeness
    t_sc = result.safety.t_score
    table.add_row("  t-近接性", f"t={t_val:.3f} (スコア: {t_sc:.2f})")
    table.add_row("暫定スコア", f"[bold]{result.anon_score:.3f}[/bold]")
    table.add_row("実行時間", f"{exec_time:.1f}秒")
    table.add_row("レコード数", f"{len(anonymized_df)}")

    console.print(table)


@evaluate_app.command("reidentify")
def evaluate_reidentify(
    submission_dir: Path = typer.Argument(..., help="提出物ディレクトリのパス"),
    anon_csv: Path = typer.Option(..., help="匿名化済みCSVのパス"),
    auxiliary_csv: Path = typer.Option(..., help="補助知識CSVのパス"),
    ground_truth_path: Path = typer.Option(..., help="正解マッピングJSONのパス"),
    schema_path: Path = typer.Option("data/schema/schema.json", help="スキーマファイルパス"),
    config_path: Path = typer.Option("configs/contest.yaml", help="設定ファイルパス"),
) -> None:
    """再識別アルゴリズムをローカル評価する."""
    algorithm_path = submission_dir / "algorithm.py"
    if not algorithm_path.exists():
        console.print(f"[red]エラー: {algorithm_path} が見つかりません[/red]")
        raise typer.Exit(1)

    config = load_contest_config(config_path if config_path.exists() else None)
    orch = PipelineOrchestrator(schema_path, config)

    console.print(f"[bold]再識別評価開始[/bold]: {submission_dir}")

    # 再識別の実行
    sys.path.insert(0, str(submission_dir))
    try:
        import tempfile
        import time

        from algorithm import reidentify  # type: ignore[import-not-found]

        output_dir = Path(tempfile.mkdtemp())
        output_json = output_dir / "mappings.json"

        start = time.time()
        reidentify(str(anon_csv), str(auxiliary_csv), str(schema_path), str(output_json))
        exec_time = time.time() - start

        if not output_json.exists():
            console.print("[red]エラー: マッピング出力が生成されませんでした[/red]")
            raise typer.Exit(1)

        mappings = load_mappings(output_json)
        ground_truth = load_ground_truth(ground_truth_path)

    except Exception as e:
        console.print(f"[red]再識別実行エラー: {e}[/red]")
        raise typer.Exit(1)
    finally:
        if str(submission_dir) in sys.path:
            sys.path.remove(str(submission_dir))
        if "algorithm" in sys.modules:
            del sys.modules["algorithm"]

    result = orch.evaluate_reidentification_direct(mappings, ground_truth)

    if not result.success:
        console.print(f"[red]評価エラー: {result.error}[/red]")
        raise typer.Exit(1)

    console.print()
    console.print("[bold green]=== ローカル評価結果（再識別）===[/bold green]")

    table = Table(show_header=True)
    table.add_column("指標", style="cyan")
    table.add_column("値", justify="right")

    assert result.result is not None
    table.add_row("Precision", f"{result.result.precision:.3f}")
    table.add_row("Recall", f"{result.result.recall:.3f}")
    table.add_row("F1", f"{result.result.f1:.3f}")
    table.add_row("予測数", f"{result.result.n_predicted}")
    table.add_row("正解数", f"{result.result.n_correct}")
    table.add_row("全レコード数", f"{result.result.n_total}")
    table.add_row("実行時間", f"{exec_time:.1f}秒")

    console.print(table)


def _find_original_data(data_dir: Path) -> Path | None:
    """データディレクトリから元データCSVを探す."""
    candidates = [
        data_dir / "sample_original.csv",
        data_dir / "original.csv",
        data_dir / "sample.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    # ディレクトリ内の最初のCSV
    csvs = list(data_dir.glob("*.csv"))
    return csvs[0] if csvs else None
