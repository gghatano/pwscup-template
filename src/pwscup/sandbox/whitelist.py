"""ホワイトリスト検証モジュール."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pwscup.config import WhitelistConfig, load_whitelist_config


@dataclass
class ValidationResult:
    """ホワイトリスト検証結果."""

    is_valid: bool
    allowed: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)


def validate_requirements(
    requirements_path: Path,
    whitelist_config: Optional[WhitelistConfig] = None,
    whitelist_path: Optional[Path] = None,
) -> ValidationResult:
    """requirements.txt がホワイトリストに適合するか検証する.

    Args:
        requirements_path: requirements.txt のパス
        whitelist_config: ホワイトリスト設定（指定時はこちらを使用）
        whitelist_path: ホワイトリストYAMLのパス

    Returns:
        検証結果
    """
    if whitelist_config is None:
        whitelist_config = load_whitelist_config(whitelist_path)

    if not requirements_path.exists():
        return ValidationResult(
            is_valid=True,
            messages=["requirements.txt が存在しません（追加ライブラリなし）"],
        )

    allowed_set = set(whitelist_config.allowed_libraries)

    with open(requirements_path) as f:
        lines = f.readlines()

    requested: list[str] = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # パッケージ名を抽出（バージョン指定を除去）
        pkg_name = line.split(">=")[0].split("<=")[0].split("==")[0].split("!=")[0]
        pkg_name = pkg_name.split("[")[0]  # extras を除去
        pkg_name = pkg_name.strip().lower()
        if pkg_name:
            requested.append(pkg_name)

    allowed: list[str] = []
    rejected: list[str] = []
    messages: list[str] = []

    for pkg in requested:
        # 正規化して比較（ハイフンとアンダースコアを統一）
        normalized = pkg.replace("-", "_").replace(".", "_")
        allowed_normalized = {a.replace("-", "_").replace(".", "_") for a in allowed_set}

        if normalized in allowed_normalized:
            allowed.append(pkg)
        else:
            rejected.append(pkg)
            messages.append(f"許可されていないライブラリ: {pkg}")

    is_valid = len(rejected) == 0

    if not is_valid:
        messages.append(
            f"許可ライブラリ: {', '.join(sorted(allowed_set))}"
        )

    return ValidationResult(
        is_valid=is_valid,
        allowed=allowed,
        rejected=rejected,
        messages=messages,
    )
