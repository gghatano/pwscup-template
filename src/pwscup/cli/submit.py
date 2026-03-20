"""提出コマンド."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from pwscup.config import load_contest_config, load_whitelist_config
from pwscup.db.engine import get_session, init_db
from pwscup.db.repository import (
    count_daily_submissions,
    create_submission,
    create_team,
    get_team_by_name,
    save_anon_evaluation,
    save_reid_evaluation,
    update_submission_status,
)
from pwscup.models.evaluation import AnonymizationEvaluation, ReidentificationEvaluation
from pwscup.models.submission import SubmissionDivision, SubmissionStatus
from pwscup.pipeline.orchestrator import PipelineOrchestrator
from pwscup.pipeline.reidentify import load_ground_truth, load_mappings
from pwscup.sandbox.docker_runner import DockerRunner
from pwscup.sandbox.whitelist import validate_requirements
from pwscup.schema import load_schema

console = Console()

submit_app = typer.Typer(help="アルゴリズムを提出する")


@submit_app.command("anonymize")
def submit_anonymize(
    submission_dir: Path = typer.Argument(..., help="提出物ディレクトリのパス"),
    data_dir: Path = typer.Option("data/sample", help="データディレクトリ"),
    schema_path: Path = typer.Option("data/schema/schema.json", help="スキーマファイル"),
    config_path: Path = typer.Option("configs/contest.yaml", help="設定ファイル"),
    whitelist_path: Path = typer.Option("configs/whitelist.yaml", help="ホワイトリスト"),
    team_name: str = typer.Option("default", help="チーム名"),
    db_path: Path = typer.Option("data/pwscup.db", help="DBファイルパス"),
    use_docker: bool = typer.Option(True, help="Docker実行を使用する"),
) -> None:
    """匿名化アルゴリズムを提出する."""
    _submit(
        submission_dir=submission_dir,
        division=SubmissionDivision.ANONYMIZE,
        data_dir=data_dir,
        schema_path=schema_path,
        config_path=config_path,
        whitelist_path=whitelist_path,
        team_name=team_name,
        db_path=db_path,
        use_docker=use_docker,
    )


@submit_app.command("reidentify")
def submit_reidentify(
    submission_dir: Path = typer.Argument(..., help="提出物ディレクトリのパス"),
    anon_csv: Path = typer.Option(..., help="匿名化済みCSV"),
    auxiliary_csv: Path = typer.Option(..., help="補助知識CSV"),
    ground_truth_path: Path = typer.Option(..., help="正解マッピングJSON"),
    schema_path: Path = typer.Option("data/schema/schema.json", help="スキーマファイル"),
    config_path: Path = typer.Option("configs/contest.yaml", help="設定ファイル"),
    whitelist_path: Path = typer.Option("configs/whitelist.yaml", help="ホワイトリスト"),
    team_name: str = typer.Option("default", help="チーム名"),
    db_path: Path = typer.Option("data/pwscup.db", help="DBファイルパス"),
    use_docker: bool = typer.Option(True, help="Docker実行を使用する"),
) -> None:
    """再識別アルゴリズムを提出する."""
    _submit(
        submission_dir=submission_dir,
        division=SubmissionDivision.REIDENTIFY,
        data_dir=None,
        schema_path=schema_path,
        config_path=config_path,
        whitelist_path=whitelist_path,
        team_name=team_name,
        db_path=db_path,
        use_docker=use_docker,
        anon_csv=anon_csv,
        auxiliary_csv=auxiliary_csv,
        ground_truth_path=ground_truth_path,
    )


def _submit(
    submission_dir: Path,
    division: SubmissionDivision,
    schema_path: Path,
    config_path: Path,
    whitelist_path: Path,
    team_name: str,
    db_path: Path,
    use_docker: bool,
    data_dir: Path | None = None,
    anon_csv: Path | None = None,
    auxiliary_csv: Path | None = None,
    ground_truth_path: Path | None = None,
) -> None:
    """提出処理の共通ロジック."""
    # 1. 提出物バリデーション
    algorithm_path = submission_dir / "algorithm.py"
    if not algorithm_path.exists():
        console.print(f"[red]エラー: {algorithm_path} が見つかりません[/red]")
        raise typer.Exit(1)

    # ホワイトリスト検証
    req_path = submission_dir / "requirements.txt"
    wl_config = load_whitelist_config(whitelist_path if whitelist_path.exists() else None)
    wl_result = validate_requirements(req_path, whitelist_config=wl_config)
    if not wl_result.is_valid:
        console.print("[red]ホワイトリスト違反:[/red]")
        for msg in wl_result.messages:
            console.print(f"  {msg}")
        raise typer.Exit(1)

    # 2. DB初期化・チーム登録
    init_db(db_path)
    config = load_contest_config(config_path if config_path.exists() else None)

    with get_session(db_path) as session:
        team = get_team_by_name(session, team_name)
        if team is None:
            team = create_team(session, name=team_name)
            console.print(f"チーム登録: {team_name}")

        # 3. 提出制限チェック
        daily_count = count_daily_submissions(session, team.id, division)  # type: ignore[arg-type]
        if daily_count >= config.submission.daily_limit:
            console.print(
                f"[red]本日の提出上限({config.submission.daily_limit}回)に達しています[/red]"
            )
            raise typer.Exit(1)

        # 4. 提出をDBに記録
        metadata_path = submission_dir / "metadata.json"
        metadata_json = "{}"
        if metadata_path.exists():
            metadata_json = metadata_path.read_text()

        sub = create_submission(
            session,
            team_id=team.id,  # type: ignore[arg-type]
            division=division,
            file_path=str(submission_dir),
            metadata_json=metadata_json,
        )
        console.print(f"提出受付 (ID: {sub.id}, 部門: {division.value})")

    # 5. 評価実行
    output_dir = Path(tempfile.mkdtemp())
    runner = DockerRunner(config, force_subprocess=not use_docker)

    if division == SubmissionDivision.ANONYMIZE:
        _run_anonymize_eval(
            sub_id=sub.id,  # type: ignore[arg-type]
            runner=runner,
            submission_dir=submission_dir,
            data_dir=data_dir,  # type: ignore[arg-type]
            schema_path=schema_path,
            output_dir=output_dir,
            config=config,
            db_path=db_path,
        )
    else:
        _run_reidentify_eval(
            sub_id=sub.id,  # type: ignore[arg-type]
            runner=runner,
            submission_dir=submission_dir,
            anon_csv=anon_csv,  # type: ignore[arg-type]
            auxiliary_csv=auxiliary_csv,  # type: ignore[arg-type]
            ground_truth_path=ground_truth_path,  # type: ignore[arg-type]
            schema_path=schema_path,
            output_dir=output_dir,
            config=config,
            db_path=db_path,
        )


def _find_original_data(data_dir: Path) -> Path | None:
    for name in ["sample_original.csv", "original.csv", "sample.csv"]:
        p = data_dir / name
        if p.exists():
            return p
    csvs = list(data_dir.glob("*.csv"))
    return csvs[0] if csvs else None


def _run_anonymize_eval(
    sub_id: int,
    runner: DockerRunner,
    submission_dir: Path,
    data_dir: Path,
    schema_path: Path,
    output_dir: Path,
    config: object,
    db_path: Path,
) -> None:
    original_path = _find_original_data(data_dir)
    if original_path is None:
        console.print(f"[red]元データが見つかりません: {data_dir}[/red]")
        _update_status_error(sub_id, "元データが見つかりません", db_path)
        raise typer.Exit(1)

    console.print("匿名化実行中...")
    result = runner.run_anonymization(submission_dir, original_path, schema_path, output_dir)

    if result.status != "success":
        console.print(f"[red]実行失敗 ({result.status}): {result.stderr[:300]}[/red]")
        _update_status_error(sub_id, result.stderr[:500], db_path, result.execution_time_sec)
        raise typer.Exit(1)

    # 評価
    anon_csv = output_dir / "anonymized.csv"
    if not anon_csv.exists():
        console.print("[red]匿名化出力が生成されませんでした[/red]")
        _update_status_error(sub_id, "出力なし", db_path, result.execution_time_sec)
        raise typer.Exit(1)

    orch = PipelineOrchestrator(schema_path, config)  # type: ignore[arg-type]
    original_df = pd.read_csv(original_path)
    schema = load_schema(schema_path)
    id_cols = [c.name for c in schema.get_columns_by_role("identifier")]
    original_df = original_df.drop(columns=[c for c in id_cols if c in original_df.columns])
    anonymized_df = pd.read_csv(anon_csv)

    eval_result = orch.evaluate_anonymization_direct(original_df, anonymized_df)

    if not eval_result.success:
        console.print(f"[red]評価失敗: {eval_result.error}[/red]")
        _update_status_error(sub_id, eval_result.error or "", db_path, result.execution_time_sec)
        raise typer.Exit(1)

    # DB保存
    with get_session(db_path) as session:
        update_submission_status(
            session, sub_id, SubmissionStatus.COMPLETED,
            execution_time_sec=result.execution_time_sec,
        )
        assert eval_result.utility is not None
        assert eval_result.safety is not None
        save_anon_evaluation(
            session,
            AnonymizationEvaluation(
                submission_id=sub_id,
                utility_score=eval_result.utility.utility_score,
                distribution_distance=eval_result.utility.distribution_distance,
                correlation_preservation=eval_result.utility.correlation_preservation,
                query_accuracy=eval_result.utility.query_accuracy,
                ml_utility=eval_result.utility.ml_utility,
                safety_score_auto=eval_result.safety.safety_score_auto,
                k_anonymity=eval_result.safety.k_anonymity,
                l_diversity=eval_result.safety.l_diversity,
                t_closeness=eval_result.safety.t_closeness,
                final_score=eval_result.anon_score,
            ),
        )

    # 結果表示
    console.print()
    console.print("[bold green]提出完了[/bold green]")
    table = Table(show_header=True)
    table.add_column("指標", style="cyan")
    table.add_column("値", justify="right")
    table.add_row("提出ID", str(sub_id))
    table.add_row("有用性スコア", f"{eval_result.utility.utility_score:.3f}")
    table.add_row("安全性スコア(暫定)", f"{eval_result.safety.safety_score_auto:.3f}")
    table.add_row("匿名化スコア(暫定)", f"[bold]{eval_result.anon_score:.3f}[/bold]")
    table.add_row("実行時間", f"{result.execution_time_sec:.1f}秒")
    console.print(table)


def _run_reidentify_eval(
    sub_id: int,
    runner: DockerRunner,
    submission_dir: Path,
    anon_csv: Path,
    auxiliary_csv: Path,
    ground_truth_path: Path,
    schema_path: Path,
    output_dir: Path,
    config: object,
    db_path: Path,
) -> None:
    console.print("再識別実行中...")
    result = runner.run_reidentification(
        submission_dir, anon_csv, auxiliary_csv, schema_path, output_dir
    )

    if result.status != "success":
        console.print(f"[red]実行失敗 ({result.status}): {result.stderr[:300]}[/red]")
        _update_status_error(sub_id, result.stderr[:500], db_path, result.execution_time_sec)
        raise typer.Exit(1)

    mappings_path = output_dir / "mappings.json"
    if not mappings_path.exists():
        console.print("[red]マッピング出力が生成されませんでした[/red]")
        _update_status_error(sub_id, "出力なし", db_path, result.execution_time_sec)
        raise typer.Exit(1)

    mappings = load_mappings(mappings_path)
    ground_truth = load_ground_truth(ground_truth_path)

    orch = PipelineOrchestrator(schema_path, config)  # type: ignore[arg-type]
    eval_result = orch.evaluate_reidentification_direct(mappings, ground_truth)

    if not eval_result.success:
        console.print(f"[red]評価失敗: {eval_result.error}[/red]")
        _update_status_error(sub_id, eval_result.error or "", db_path, result.execution_time_sec)
        raise typer.Exit(1)

    with get_session(db_path) as session:
        update_submission_status(
            session, sub_id, SubmissionStatus.COMPLETED,
            execution_time_sec=result.execution_time_sec,
        )
        assert eval_result.result is not None
        save_reid_evaluation(
            session,
            ReidentificationEvaluation(
                submission_id=sub_id,
                target_submission_id=0,  # TODO: 実際の対象提出IDを設定
                precision=eval_result.result.precision,
                recall=eval_result.result.recall,
                f1=eval_result.result.f1,
                difficulty_weighted_score=eval_result.result.difficulty_weighted_score,
            ),
        )

    console.print()
    console.print("[bold green]提出完了[/bold green]")
    table = Table(show_header=True)
    table.add_column("指標", style="cyan")
    table.add_column("値", justify="right")
    table.add_row("提出ID", str(sub_id))
    table.add_row("Precision", f"{eval_result.result.precision:.3f}")
    table.add_row("Recall", f"{eval_result.result.recall:.3f}")
    table.add_row("F1", f"{eval_result.result.f1:.3f}")
    table.add_row("実行時間", f"{result.execution_time_sec:.1f}秒")
    console.print(table)


def _update_status_error(
    sub_id: int, error: str, db_path: Path, exec_time: float = 0.0
) -> None:
    with get_session(db_path) as session:
        update_submission_status(
            session, sub_id, SubmissionStatus.ERROR,
            error_message=error, execution_time_sec=exec_time,
        )
