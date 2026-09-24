from __future__ import annotations

from pydantic import SecretStr

from pyscripts import ray_client
from pyscripts.config import Settings


def test_connect_ray_uploads_package_at_job_level(monkeypatch) -> None:
    captured: dict = {}
    shutdown_calls = 0

    def shutdown() -> None:
        nonlocal shutdown_calls
        shutdown_calls += 1

    monkeypatch.setattr(ray_client.ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ray_client.ray, "shutdown", shutdown)
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
    assert captured["runtime_env"]["config"] == {"eager_install": False}
    assert shutdown_calls == 1


def test_connect_ray_keeps_healthy_connection(monkeypatch) -> None:
    monkeypatch.setattr(ray_client.ray, "is_initialized", lambda: True)
    monkeypatch.setattr(
        ray_client.ray,
        "shutdown",
        lambda: (_ for _ in ()).throw(
            AssertionError("healthy connection must not be shut down")
        ),
    )
    monkeypatch.setattr(
        ray_client.ray,
        "init",
        lambda **_options: (_ for _ in ()).throw(
            AssertionError("healthy connection must not reconnect")
        ),
    )

    ray_client.connect_ray(
        Settings(database_url=SecretStr("sqlite+aiosqlite:///:memory:"))
    )
