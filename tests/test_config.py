"""設定管理のテスト."""

from pathlib import Path

import pytest

from pwscup.config import (
    ContestConfig,
    WhitelistConfig,
    load_contest_config,
    load_whitelist_config,
)

CONFIGS_DIR = Path(__file__).parent.parent / "configs"


def test_default_contest_config() -> None:
    config = ContestConfig()
    assert config.contest.name == "PWSCUP データ匿名化・再識別コンテスト"
    assert config.scoring.utility_weights.distribution_distance == 0.3
    assert config.constraints.min_k_anonymity == 2
    assert config.constraints.anonymize.time_limit_sec == 300
    assert config.constraints.reidentify.time_limit_sec == 600
    assert config.submission.daily_limit == 5


def test_load_contest_config_from_file() -> None:
    config = load_contest_config(CONFIGS_DIR / "contest.yaml")
    assert config.contest.name == "PWSCUP データ匿名化・再識別コンテスト"
    assert config.scoring.utility_weights.ml_utility == 0.2
    assert config.scoring.safety.s_auto_weight == 0.4
    assert config.scoring.safety.s_reid_weight == 0.6
    assert config.scoring.total.anon_weight == 0.5


def test_load_contest_config_none_returns_default() -> None:
    config = load_contest_config(None)
    assert config.constraints.min_k_anonymity == 2


def test_load_whitelist_config_from_file() -> None:
    config = load_whitelist_config(CONFIGS_DIR / "whitelist.yaml")
    assert "numpy" in config.allowed_libraries
    assert "pandas" in config.allowed_libraries
    assert "scikit-learn" in config.allowed_libraries
    assert len(config.allowed_libraries) == 5


def test_load_whitelist_config_none_returns_default() -> None:
    config = load_whitelist_config(None)
    assert config.allowed_libraries == []


def test_invalid_weight_range() -> None:
    with pytest.raises(Exception):
        ContestConfig.model_validate(
            {
                "scoring": {
                    "utility_weights": {
                        "distribution_distance": 1.5,
                    }
                }
            }
        )


def test_default_whitelist_config() -> None:
    config = WhitelistConfig()
    assert config.allowed_libraries == []
