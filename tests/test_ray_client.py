from __future__ import annotations

from pydantic import SecretStr

from pyscripts import ray_client
from pyscripts.config import Settings


def test_connect_ray_uploads_package_at_job_level(monkeypatch) -> None:
    captured: dict = {}

    monkeypatch.setattr(ray_client.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(
        ray_client.ray,
        "init",
        lambda **options: captured.update(options),
    )
    ray_client.connect_ray(
        Settings(database_url=SecretStr("sqlite+aiosqlite:///:memory:"))
    )

    modules = captured["runtime_env"]["py_modules"]
    assert len(modules) == 1
    assert isinstance(modules[0], str)
    assert modules[0].endswith("/pyscripts")
