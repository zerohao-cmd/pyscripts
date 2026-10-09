from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def configured_admin(monkeypatch: pytest.MonkeyPatch) -> None:
    # Tests opt into object storage explicitly; a developer's local .env must
    # not change filesystem-backed test expectations.
    monkeypatch.setenv("PYSCRIPTS_OBJECT_STORE_ENABLED", "false")
    monkeypatch.setenv("PYSCRIPTS_ADMIN_USERNAME", "admin")
    monkeypatch.setenv("PYSCRIPTS_ADMIN_PASSWORD", "test-admin-password")
    monkeypatch.setenv(
        "PYSCRIPTS_ADMIN_SESSION_SECRET",
        "test-session-secret-with-at-least-32-characters",
    )
