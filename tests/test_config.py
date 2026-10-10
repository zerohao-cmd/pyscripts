from __future__ import annotations

from pathlib import Path

import pytest

from pyscripts.config import Settings


def test_contract_cache_root_has_local_cache_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYSCRIPTS_CONTRACT_CACHE_ROOT", raising=False)

    settings = Settings(_env_file=None)

    assert settings.contract_cache_root == Path("/tmp/pyscripts-contracts")


def test_contract_cache_root_reads_environment_variable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_root = tmp_path / "contract-cache"
    monkeypatch.setenv("PYSCRIPTS_CONTRACT_CACHE_ROOT", str(cache_root))

    settings = Settings(_env_file=None)

    assert settings.contract_cache_root == cache_root


def test_contract_cache_root_reads_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYSCRIPTS_CONTRACT_CACHE_ROOT", raising=False)
    cache_root = tmp_path / "contract-cache"
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        f"PYSCRIPTS_CONTRACT_CACHE_ROOT={cache_root}\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=dotenv_path)

    assert settings.contract_cache_root == cache_root
