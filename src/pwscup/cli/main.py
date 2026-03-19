"""PWSCUP CLI エントリーポイント."""

import typer

from pwscup import __version__
from pwscup.cli.evaluate import evaluate_app
from pwscup.cli.leaderboard import leaderboard_command
from pwscup.cli.status import status_command
from pwscup.cli.submit import submit_app

app = typer.Typer(
    name="pwscup",
    help="PWSCUP データ匿名化・再識別コンテスト環境",
    no_args_is_help=True,
)

app.add_typer(evaluate_app, name="evaluate")
app.add_typer(submit_app, name="submit")
app.command("leaderboard")(leaderboard_command)
app.command("status")(status_command)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"pwscup {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="バージョンを表示",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """PWSCUP データ匿名化・再識別コンテスト環境."""


if __name__ == "__main__":
    app()
