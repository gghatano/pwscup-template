"""ホワイトリスト検証のテスト."""

import tempfile
from pathlib import Path

from pwscup.config import WhitelistConfig
from pwscup.sandbox.whitelist import validate_requirements

WHITELIST_PATH = Path(__file__).parent.parent.parent / "configs" / "whitelist.yaml"


def _write_requirements(content: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    tmp.write(content)
    tmp.close()
    return Path(tmp.name)


class TestValidateRequirements:
    def test_all_allowed(self) -> None:
        path = _write_requirements("numpy\npandas\n")
        config = WhitelistConfig(allowed_libraries=["numpy", "pandas", "scipy"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid
        assert len(result.rejected) == 0
        assert "numpy" in result.allowed

    def test_rejected_library(self) -> None:
        path = _write_requirements("numpy\ntorch\n")
        config = WhitelistConfig(allowed_libraries=["numpy", "pandas"])
        result = validate_requirements(path, whitelist_config=config)
        assert not result.is_valid
        assert "torch" in result.rejected

    def test_empty_requirements(self) -> None:
        path = _write_requirements("")
        config = WhitelistConfig(allowed_libraries=["numpy"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid

    def test_comments_and_blank_lines(self) -> None:
        path = _write_requirements("# comment\n\nnumpy\n  # another comment\n")
        config = WhitelistConfig(allowed_libraries=["numpy"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid
        assert len(result.allowed) == 1

    def test_version_specifiers(self) -> None:
        path = _write_requirements("numpy>=1.24.0\npandas==2.0.0\nscipy!=1.0\n")
        config = WhitelistConfig(allowed_libraries=["numpy", "pandas", "scipy"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid

    def test_hyphen_underscore_normalization(self) -> None:
        path = _write_requirements("scikit-learn\n")
        config = WhitelistConfig(allowed_libraries=["scikit-learn"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid

    def test_nonexistent_file(self) -> None:
        path = Path("/nonexistent/requirements.txt")
        config = WhitelistConfig(allowed_libraries=["numpy"])
        result = validate_requirements(path, whitelist_config=config)
        assert result.is_valid

    def test_from_whitelist_yaml(self) -> None:
        path = _write_requirements("numpy\npandas\nscipy\n")
        result = validate_requirements(path, whitelist_path=WHITELIST_PATH)
        assert result.is_valid
