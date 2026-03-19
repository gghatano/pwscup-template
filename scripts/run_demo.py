#!/usr/bin/env python3
"""PWSCUP デモスクリプト.

全パイプラインを1コマンドで体験できるインタラクティブなデモ。
Usage: uv run python scripts/run_demo.py [--no-interactive]
"""

from __future__ import annotations

import argparse
import importlib
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# ---------- プロジェクトルート解決 ----------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

console = Console()


# ============================================================
# ユーティリティ
# ============================================================

def wait_for_enter(interactive: bool) -> None:
    """インタラクティブモードの場合 Enter 待ち."""
    if interactive:
        console.print("\n[dim]Enter で次へ...[/dim]")
        input()


def score_bar(label: str, value: float, width: int = 30, color: str = "green") -> str:
    """スコアをプログレスバー風文字列で返す."""
    filled = int(value * width)
    bar = "[" + "#" * filled + "-" * (width - filled) + "]"
    return f"{label}: {bar} {value:.3f}"


def import_algorithm(module_dir: Path, module_name: str = "algorithm") -> object:
    """examples 配下の algorithm.py を動的にインポートする."""
    module_dir_str = str(module_dir)
    if module_dir_str not in sys.path:
        sys.path.insert(0, module_dir_str)

    # キャッシュクリア
    if module_name in sys.modules:
        del sys.modules[module_name]

    return importlib.import_module(module_name)


# ============================================================
# 各ステップ
# ============================================================

def step_intro(interactive: bool) -> None:
    """ステップ1: イントロ表示."""
    console.print()
    intro_text = (
        "[bold cyan]PWSCUP データ匿名化・再識別コンテスト[/bold cyan]\n\n"
        "このデモでは、匿名化・再識別パイプラインの全工程を体験できます。\n\n"
        "  [bold]1.[/bold] サンプルデータの確認\n"
        "  [bold]2.[/bold] ベースライン匿名化の実行\n"
        "  [bold]3.[/bold] 匿名化の評価（有用性 / 安全性 / スコア）\n"
        "  [bold]4.[/bold] ベースライン再識別の実行\n"
        "  [bold]5.[/bold] 再識別の評価（precision / recall / F1）\n"
        "  [bold]6.[/bold] 総合サマリ\n\n"
        "コンテストでは [bold]匿名化[/bold] と [bold]再識別[/bold] の2部門があり、\n"
        "匿名化は「有用性 x 安全性」、再識別は「精度 x 難易度」で競います。"
    )
    console.print(Panel(intro_text, title="PWSCUP デモ", border_style="bright_blue"))
    wait_for_enter(interactive)


def step_data_check(
    original_csv: Path, schema_path: Path, interactive: bool,
) -> tuple[pd.DataFrame, object]:
    """ステップ2: データ確認."""
    from pwscup.schema import load_schema

    console.rule("[bold yellow]Step 1: サンプルデータの確認[/bold yellow]")

    original_df = pd.read_csv(original_csv)
    schema = load_schema(schema_path)

    console.print(f"\n[bold]データファイル:[/bold] {original_csv}")
    console.print(f"[bold]レコード数:[/bold]    {len(original_df)}")
    console.print(f"[bold]カラム数:[/bold]      {len(original_df.columns)}")

    # スキーマの役割別カラム一覧
    console.print("\n[bold]カラム一覧（役割別）:[/bold]")
    role_table = Table(show_header=True, header_style="bold")
    role_table.add_column("カラム名", style="white")
    role_table.add_column("型")
    role_table.add_column("役割")

    role_colors = {
        "identifier": "dim",
        "quasi_identifier": "yellow",
        "sensitive_attribute": "red",
        "non_sensitive": "green",
    }
    role_labels = {
        "identifier": "識別子",
        "quasi_identifier": "準識別子 (QI)",
        "sensitive_attribute": "機微属性 (SA)",
        "non_sensitive": "非機微",
    }

    for col in schema.columns:
        color = role_colors.get(col.role, "white")
        label = role_labels.get(col.role, col.role)
        role_table.add_row(
            f"[{color}]{col.name}[/{color}]",
            col.type,
            f"[{color}]{label}[/{color}]",
        )
    console.print(role_table)

    # 最初の5行
    console.print("\n[bold]先頭5行:[/bold]")
    head_table = Table(show_header=True, header_style="bold magenta", show_lines=True)
    for col_name in original_df.columns:
        head_table.add_column(col_name)
    for _, row in original_df.head(5).iterrows():
        head_table.add_row(*[str(v) for v in row])
    console.print(head_table)

    wait_for_enter(interactive)
    return original_df, schema


def step_anonymize(
    original_df: pd.DataFrame,
    original_csv: Path,
    schema_path: Path,
    output_dir: Path,
    interactive: bool,
) -> Optional[pd.DataFrame]:
    """ステップ3: ベースライン匿名化の実行."""
    console.rule("[bold yellow]Step 2: ベースライン匿名化の実行[/bold yellow]")

    anon_example_dir = PROJECT_ROOT / "examples" / "anonymize_example"
    output_csv = output_dir / "anonymized.csv"

    console.print(f"\n[bold]匿名化アルゴリズム:[/bold] {anon_example_dir / 'algorithm.py'}")
    console.print("[dim]実行中...[/dim]")

    try:
        algo = import_algorithm(anon_example_dir)
        start = time.time()
        algo.anonymize(str(original_csv), str(schema_path), str(output_csv))  # type: ignore[attr-defined]
        elapsed = time.time() - start
    except Exception as e:
        console.print(f"[bold red]匿名化エラー:[/bold red] {e}")
        return None

    console.print(f"[bold green]完了![/bold green] 実行時間: {elapsed:.2f} 秒")

    anon_df = pd.read_csv(output_csv)

    console.print(f"\n[bold]レコード数の変化:[/bold] {len(original_df)} -> {len(anon_df)}"
                  f"  (差: {len(original_df) - len(anon_df)})")

    # 匿名化前後のデータ比較
    console.print("\n[bold]匿名化前（先頭5行）:[/bold]")
    # 識別子なしのカラムで表示
    display_cols = [c for c in anon_df.columns]
    before_table = Table(show_header=True, header_style="bold blue", show_lines=True)
    for col_name in display_cols:
        before_table.add_column(col_name)
    for _, row in original_df[display_cols].head(5).iterrows():
        before_table.add_row(*[str(v) for v in row])
    console.print(before_table)

    console.print("\n[bold]匿名化後（先頭5行）:[/bold]")
    after_table = Table(show_header=True, header_style="bold red", show_lines=True)
    for col_name in display_cols:
        after_table.add_column(col_name)
    for _, row in anon_df.head(5).iterrows():
        after_table.add_row(*[str(v) for v in row])
    console.print(after_table)

    wait_for_enter(interactive)
    return anon_df


def step_evaluate_anonymization(
    original_df: pd.DataFrame,
    anon_df: pd.DataFrame,
    schema_path: Path,
    config_path: Path,
    interactive: bool,
) -> Optional[object]:
    """ステップ4: 匿名化の評価."""
    from pwscup.config import load_contest_config
    from pwscup.pipeline.orchestrator import PipelineOrchestrator

    console.rule("[bold yellow]Step 3: 匿名化の評価[/bold yellow]")

    config = load_contest_config(config_path)
    orchestrator = PipelineOrchestrator(schema_path=schema_path, config=config)

    console.print("\n[dim]評価中...[/dim]")
    start = time.time()
    result = orchestrator.evaluate_anonymization_direct(original_df, anon_df)
    elapsed = time.time() - start
    console.print(f"[dim]評価時間: {elapsed:.2f} 秒[/dim]")

    if not result.success:
        console.print(f"[bold red]評価エラー:[/bold red] {result.error}")
        return None

    # 有用性スコア
    u = result.utility
    assert u is not None
    console.print("\n[bold cyan]--- 有用性スコア (U) ---[/bold cyan]")
    console.print(score_bar("  分布距離       ", u.distribution_distance, color="cyan"))
    console.print(score_bar("  相関保持       ", u.correlation_preservation, color="cyan"))
    console.print(score_bar("  クエリ精度     ", u.query_accuracy, color="cyan"))
    console.print(score_bar("  ML有用性       ", u.ml_utility, color="cyan"))
    console.print(score_bar("  [bold]総合 U[/bold]         ", u.utility_score, color="bright_cyan"))

    # 安全性スコア
    s = result.safety
    assert s is not None
    console.print("\n[bold magenta]--- 安全性スコア (S_auto) ---[/bold magenta]")
    console.print(f"  k-匿名性: k = {s.k_anonymity}")
    console.print(score_bar("    k スコア     ", s.k_score, color="magenta"))
    console.print(f"  l-多様性: l = {s.l_diversity}")
    console.print(score_bar("    l スコア     ", s.l_score, color="magenta"))
    console.print(f"  t-近接性: t = {s.t_closeness:.4f}")
    console.print(score_bar("    t スコア     ", s.t_score, color="magenta"))
    console.print(score_bar("  [bold]総合 S_auto[/bold]    ", s.safety_score_auto, color="bright_magenta"))

    # 暫定スコア
    console.print("\n[bold green]--- 暫定スコア (U x S_auto) ---[/bold green]")
    assert result.anon_score is not None
    console.print(score_bar("  [bold]Score[/bold]          ", result.anon_score, color="bright_green"))

    wait_for_enter(interactive)
    return result


def step_reidentify(
    anon_csv: Path,
    auxiliary_csv: Path,
    schema_path: Path,
    output_dir: Path,
    interactive: bool,
) -> Optional[Path]:
    """ステップ5: ベースライン再識別の実行."""
    console.rule("[bold yellow]Step 4: ベースライン再識別の実行[/bold yellow]")

    reid_example_dir = PROJECT_ROOT / "examples" / "reidentify_example"
    output_json = output_dir / "mappings.json"

    console.print(f"\n[bold]再識別アルゴリズム:[/bold] {reid_example_dir / 'algorithm.py'}")
    console.print(f"[bold]補助知識データ:[/bold]   {auxiliary_csv}")
    console.print("[dim]実行中...[/dim]")

    try:
        algo = import_algorithm(reid_example_dir)
        start = time.time()
        algo.reidentify(  # type: ignore[attr-defined]
            str(anon_csv), str(auxiliary_csv), str(schema_path), str(output_json),
        )
        elapsed = time.time() - start
    except Exception as e:
        console.print(f"[bold red]再識別エラー:[/bold red] {e}")
        return None

    console.print(f"[bold green]完了![/bold green] 実行時間: {elapsed:.2f} 秒")

    wait_for_enter(interactive)
    return output_json


def step_evaluate_reidentification(
    mappings_json: Path,
    ground_truth_path: Path,
    s_auto: float,
    interactive: bool,
) -> Optional[object]:
    """ステップ6: 再識別の評価."""
    from pwscup.pipeline.reidentify import (
        evaluate_reidentification,
        load_ground_truth,
        load_mappings,
    )

    console.rule("[bold yellow]Step 5: 再識別の評価[/bold yellow]")

    mappings = load_mappings(mappings_json)
    ground_truth = load_ground_truth(ground_truth_path)

    console.print(f"\n[bold]正解データ:[/bold]     {ground_truth_path}")
    console.print(f"[bold]予測マッピング数:[/bold] {len(mappings)}")
    console.print(f"[bold]正解レコード数:[/bold]   {len(ground_truth)}")

    result = evaluate_reidentification(mappings, ground_truth, s_auto)

    console.print(f"\n[bold]マッチング結果:[/bold]")
    console.print(f"  正解数:   {result.n_correct} / {result.n_predicted} 予測")
    console.print(f"  対象総数: {result.n_total}")

    console.print("\n[bold cyan]--- 再識別スコア ---[/bold cyan]")
    console.print(score_bar("  Precision      ", result.precision))
    console.print(score_bar("  Recall         ", result.recall))
    console.print(score_bar("  F1             ", result.f1))
    console.print(f"  難易度加重スコア: {result.difficulty_weighted_score:.4f}")

    wait_for_enter(interactive)
    return result


def step_summary(
    anon_result: Optional[object],
    reid_result: Optional[object],
) -> None:
    """ステップ7: 総合サマリ."""
    console.rule("[bold yellow]Step 6: 総合サマリ[/bold yellow]")

    summary_table = Table(title="スコア一覧", show_header=True, header_style="bold")
    summary_table.add_column("部門", style="bold")
    summary_table.add_column("指標")
    summary_table.add_column("値", justify="right")

    if anon_result is not None and anon_result.success:  # type: ignore[union-attr]
        u = anon_result.utility  # type: ignore[union-attr]
        s = anon_result.safety  # type: ignore[union-attr]
        score = anon_result.anon_score  # type: ignore[union-attr]
        summary_table.add_row("匿名化", "有用性 (U)", f"{u.utility_score:.3f}")
        summary_table.add_row("", "安全性 (S_auto)", f"{s.safety_score_auto:.3f}")
        summary_table.add_row("", "[bold]暫定スコア U x S_auto[/bold]", f"[bold]{score:.3f}[/bold]")
    else:
        summary_table.add_row("匿名化", "評価", "[red]失敗[/red]")

    summary_table.add_row("---", "---", "---")

    if reid_result is not None:
        summary_table.add_row("再識別", "Precision", f"{reid_result.precision:.3f}")  # type: ignore[union-attr]
        summary_table.add_row("", "Recall", f"{reid_result.recall:.3f}")  # type: ignore[union-attr]
        summary_table.add_row("", "[bold]F1[/bold]", f"[bold]{reid_result.f1:.3f}[/bold]")  # type: ignore[union-attr]
    else:
        summary_table.add_row("再識別", "評価", "[red]失敗[/red]")

    console.print()
    console.print(summary_table)

    # 改善のヒント
    hints = [
        "[bold]1.[/bold] 匿名化の有用性を高めるには、汎化の粒度を細かくし、"
        "ノイズ量を最小限にしましょう。ただし安全性とのバランスが重要です。",
        "[bold]2.[/bold] 安全性を高めるには k-匿名性を大きくするだけでなく、"
        "l-多様性や t-近接性も意識しましょう。サプレッションは最終手段です。",
        "[bold]3.[/bold] 再識別の精度を上げるには、補助知識の活用方法を工夫しましょう。"
        "距離関数の設計やマッチングアルゴリズムの改良が鍵です。",
    ]
    hint_text = "\n\n".join(hints)
    console.print()
    console.print(Panel(hint_text, title="改善のヒント", border_style="bright_yellow"))


# ============================================================
# メイン
# ============================================================

def resolve_schema_path() -> Path:
    """スキーマファイルのパスを解決する."""
    candidates = [
        PROJECT_ROOT / "data" / "schema" / "schema.json",
        PROJECT_ROOT / "data" / "sample" / "sample_schema.json",
    ]
    for p in candidates:
        if p.exists():
            return p
    console.print("[bold red]スキーマファイルが見つかりません[/bold red]")
    sys.exit(1)


def main() -> None:
    """デモのメインエントリポイント."""
    parser = argparse.ArgumentParser(description="PWSCUP デモスクリプト")
    parser.add_argument(
        "--no-interactive",
        action="store_true",
        help="Enter 待ちを省略して連続実行する",
    )
    args = parser.parse_args()
    interactive = not args.no_interactive

    # ---- パス解決 ----
    original_csv = PROJECT_ROOT / "data" / "sample" / "sample_original.csv"
    schema_path = resolve_schema_path()
    auxiliary_csv = PROJECT_ROOT / "data" / "auxiliary" / "sample_auxiliary.csv"
    ground_truth_path = PROJECT_ROOT / "data" / "auxiliary" / "sample_ground_truth.json"
    config_path = PROJECT_ROOT / "configs" / "contest.yaml"

    # 必須ファイル存在チェック
    missing: list[str] = []
    for label, p in [
        ("元データ", original_csv),
        ("スキーマ", schema_path),
        ("補助知識", auxiliary_csv),
        ("正解データ", ground_truth_path),
        ("コンテスト設定", config_path),
    ]:
        if not p.exists():
            missing.append(f"  {label}: {p}")
    if missing:
        console.print("[bold red]必要なファイルが見つかりません:[/bold red]")
        for m in missing:
            console.print(m)
        sys.exit(1)

    # ---- 一時ディレクトリ ----
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)

        # Step 1: イントロ
        step_intro(interactive)

        # Step 2: データ確認
        original_df, _schema = step_data_check(original_csv, schema_path, interactive)

        # Step 3: 匿名化
        anon_df = step_anonymize(
            original_df, original_csv, schema_path, output_dir, interactive,
        )
        if anon_df is None:
            console.print("[bold red]匿名化に失敗したためデモを中断します[/bold red]")
            sys.exit(1)

        # Step 4: 匿名化評価
        anon_result = step_evaluate_anonymization(
            original_df, anon_df, schema_path, config_path, interactive,
        )

        # Step 5: 再識別
        anon_csv_path = output_dir / "anonymized.csv"
        mappings_path = step_reidentify(
            anon_csv_path, auxiliary_csv, schema_path, output_dir, interactive,
        )

        # Step 6: 再識別評価
        s_auto = 0.0
        if anon_result is not None and anon_result.success:  # type: ignore[union-attr]
            s_auto = anon_result.safety.safety_score_auto  # type: ignore[union-attr]

        reid_result = None
        if mappings_path is not None:
            reid_result = step_evaluate_reidentification(
                mappings_path, ground_truth_path, s_auto, interactive,
            )

        # Step 7: 総合サマリ
        step_summary(anon_result, reid_result)

    console.print("\n[bold bright_blue]デモ完了![/bold bright_blue]\n")


if __name__ == "__main__":
    main()
