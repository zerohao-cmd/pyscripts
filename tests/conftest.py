from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_tests_from_external_artifact_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep a developer's .env from sending test artifacts to real storage."""
    monkeypatch.setenv("PYSCRIPTS_OBJECT_STORE_ENABLED", "false")
