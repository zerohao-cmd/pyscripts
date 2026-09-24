from __future__ import annotations

import hashlib
import subprocess
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import SecretStr

from pyscripts.api import create_app
from pyscripts.config import Settings
from pyscripts.runtime.output import CapturedLogChunk, ExecutionOutcome


class FakeArtifactStore:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, source_uri: str, expected_digest: str) -> str:
        self.published.append((source_uri, expected_digest))
        return f"s3://pyscripts/sha256/{expected_digest}.zip"

    def publish_blob(
        self,
        source_uri: str,
        expected_digest: str,
        *,
        category: str,
        suffix: str,
        content_type: str,
    ) -> str:
        del content_type
        self.published.append((source_uri, expected_digest))
        digest = expected_digest.removeprefix("sha256:")
        return f"s3://pyscripts/{category}/sha256/{digest}{suffix}"

    def distribution_uri(self, stored_uri: str) -> str:
        return stored_uri


def build_artifact(
    tmp_path: Path,
    *,
    name: str = "service",
    task_type: str = "io",
) -> tuple[Path, str]:
    artifact = tmp_path / f"{name}.zip"
    pyproject = (
        "[project]\n"
        f'name = "{name}"\n'
        'version = "1.0.0"\n'
        'requires-python = ">=3.12,<3.13"\n'
        "\n[tool.pyscript]\n"
        "spec_version = 1\n"
        'runtime = "py312-test"\n'
        "\n[[tool.pyscript.endpoints]]\n"
        'id = "run"\n'
        f'task_type = "{task_type}"\n'
        'entrypoint = "service:run"\n'
        'para = { value = "Int64" }\n'
        'return = "Int64"\n'
    )
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("pyproject.toml", pyproject)
        archive.writestr("service.py", "def run(context, value): return value\n")
    return artifact, hashlib.sha256(artifact.read_bytes()).hexdigest()


def test_control_plane_vertical_slice(tmp_path: Path) -> None:
    database = tmp_path / "api.db"
    artifact, digest = build_artifact(
        tmp_path,
        name="math-service",
        task_type="compute",
    )
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{database}"),
            auto_create_schema=True,
            ray_use_label_selector=False,
            require_registered_runtime_profiles=False,
            grpc_host="127.0.0.1",
            grpc_port=0,
        )
    )

    with TestClient(app) as client:
        assert app.state.grpc_gateway.bound_port is not None
        service_response = client.post(
            "/admin/services",
            json={
                "name": "math-service",
                "git_url": "https://example.invalid/math.git",
                "git_branch": "release/v1",
                "tracking_mode": "manual",
            },
        )
        assert service_response.status_code == 201
        service = service_response.json()
        assert service["created_at"]
        assert service["git_branch"] == "release/v1"

        revision_response = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "0123456789abcdef",
                "artifact_uri": artifact.as_uri(),
                "artifact_digest": digest,
            },
        )
        assert revision_response.status_code == 201
        revision = revision_response.json()
        assert revision["status"] == "ACTIVE"

        activation_response = client.post(
            f"/admin/services/{service['id']}/revisions/{revision['id']}/activate"
        )
        assert activation_response.status_code == 200
        assert activation_response.json()["status"] == "ACTIVE"

        services = client.get("/admin/services")
        assert services.status_code == 200
        assert services.json()[0]["active_revision_id"] == revision["id"]

        revisions = client.get(f"/admin/services/{service['id']}/revisions")
        assert revisions.status_code == 200
        assert revisions.json()[0]["revision"] == "0123456789abcdef"
        assert revisions.json()[0]["endpoints"][0]["task_type"] == "compute"
        endpoint = revisions.json()[0]["endpoints"][0]
        assert endpoint["value"] == {"type": "Int64"}
        assert "request_schema" not in endpoint

        detail = client.get(f"/admin/services/{service['id']}")
        assert detail.status_code == 200
        assert detail.json()["revision_count"] == 1
        assert detail.json()["active_revision"]["id"] == revision["id"]
        assert detail.json()["endpoints"][0]["id"] == "run"

        missing_interval = client.patch(
            f"/admin/services/{service['id']}",
            json={"tracking_mode": "poll"},
        )
        assert missing_interval.status_code == 422
        updated = client.patch(
            f"/admin/services/{service['id']}",
            json={
                "tracking_mode": "poll",
                "check_interval_seconds": 90,
                "git_url": "https://example.invalid/math-v2.git",
                "git_branch": "develop",
            },
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["tracking_mode"] == "poll"
        assert updated.json()["check_interval_seconds"] == 90
        assert updated.json()["git_url"].endswith("math-v2.git")
        assert updated.json()["git_branch"] == "develop"
        assert updated.json()["endpoints"][0]["id"] == "run"

        default_branch = client.patch(
            f"/admin/services/{service['id']}",
            json={"git_branch": None},
        )
        assert default_branch.status_code == 200
        assert default_branch.json()["git_branch"] is None

        manual = client.patch(
            f"/admin/services/{service['id']}",
            json={"tracking_mode": "manual"},
        )
        assert manual.status_code == 200
        assert manual.json()["check_interval_seconds"] is None

        invalid_branch = client.patch(
            f"/admin/services/{service['id']}",
            json={"git_branch": "../invalid"},
        )
        assert invalid_branch.status_code == 422

        stopped = client.post(f"/admin/services/{service['id']}/stop")
        assert stopped.status_code == 200
        assert stopped.json()["status"] == "STOPPED"
        assert stopped.json()["active_revision_id"] == revision["id"]

        unavailable = client.post(
            "/v1/services/math-service/run",
            json={"value": 42},
        )
        assert unavailable.status_code == 404

        selected_while_stopped = client.post(
            f"/admin/services/{service['id']}/revisions/{revision['id']}/activate"
        )
        assert selected_while_stopped.status_code == 200
        still_stopped = client.get(f"/admin/services/{service['id']}")
        assert still_stopped.json()["status"] == "STOPPED"

        started = client.post(f"/admin/services/{service['id']}/start")
        assert started.status_code == 200
        assert started.json()["status"] == "ACTIVE"
        assert started.json()["active_revision_id"] == revision["id"]

        duplicate_start = client.post(f"/admin/services/{service['id']}/start")
        assert duplicate_start.status_code == 409

        invocations = client.get("/admin/invocations")
        assert invocations.status_code == 200
        assert invocations.json() == []

        actor_pools = client.get("/admin/actor-pools")
        assert actor_pools.status_code == 200
        assert actor_pools.json() == []

        service_invocations = client.get(
            f"/admin/services/{service['id']}/invocations"
        )
        assert service_invocations.status_code == 200
        assert service_invocations.json() == []

        class FakeScheduler:
            async def execute(self, target, request_id, params):
                return ExecutionOutcome(
                    succeeded=True,
                    value=params["value"],
                    logs=(
                        CapturedLogChunk(0, "STDOUT", "hello from script\n"),
                    ),
                    log_bytes=18,
                )

        app.state.profile_scheduler = FakeScheduler()
        invoked = client.post(
            "/v1/services/math-service/run",
            json={"value": 42},
        )
        assert invoked.status_code == 200, invoked.text
        request_id = invoked.json()["request_id"]
        records = client.get("/admin/invocations").json()
        assert records[0]["transport"] == "REST"
        assert records[0]["has_logs"] is True
        assert records[0]["log_bytes"] == 18
        logs = client.get(f"/admin/invocations/{request_id}/logs")
        assert logs.status_code == 200
        assert logs.json()[0]["stream"] == "STDOUT"
        assert logs.json()[0]["content"] == "hello from script\n"
        assert logs.json()[0]["emitted_at"].endswith("Z")


def test_ui_is_served_when_distribution_exists(tmp_path: Path) -> None:
    database = tmp_path / "ui.db"
    ui_dist = tmp_path / "ui-dist"
    ui_dist.mkdir()
    (ui_dist / "index.html").write_text(
        "<!doctype html><title>pyscripts console</title>",
        encoding="utf-8",
    )
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{database}"),
            auto_create_schema=True,
            grpc_enabled=False,
            ray_use_label_selector=False,
            ui_dist_path=ui_dist,
        )
    )

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "pyscripts console" in response.text


def test_webhook_token_lifecycle_and_provider_events(tmp_path: Path) -> None:
    app = create_app(
        Settings(
            database_url=SecretStr(
                f"sqlite+aiosqlite:///{tmp_path / 'webhook.db'}"
            ),
            grpc_enabled=False,
            ray_use_label_selector=False,
            public_base_url="https://pyscripts.example.com/control",
            webhook_max_body_bytes=1024,
        )
    )

    class FailingGitBuilder:
        def build(self, _git_url: str, _git_branch: str | None = None):
            raise RuntimeError("expected test failure")

    with TestClient(app) as client:
        app.state.git_artifact_builder = FailingGitBuilder()
        service = client.post(
            "/admin/services",
            json={
                "name": "webhook-service",
                "git_url": "https://example.invalid/webhook.git",
                "git_branch": "main",
                "tracking_mode": "webhook",
            },
        ).json()

        initial = client.get(f"/admin/services/{service['id']}/webhook")
        assert initial.status_code == 200
        assert initial.json() == {
            "enabled": False,
            "url": None,
            "configured_at": None,
        }

        rotated = client.post(
            f"/admin/services/{service['id']}/webhook/rotate"
        )
        assert rotated.status_code == 200
        webhook_url = rotated.json()["url"]
        assert webhook_url.startswith(
            f"https://pyscripts.example.com/control/hooks/services/{service['id']}/"
        )
        hook_path = webhook_url.removeprefix("https://pyscripts.example.com/control")

        hidden = client.get(f"/admin/services/{service['id']}/webhook").json()
        assert hidden["enabled"] is True
        assert hidden["url"] is None
        assert hidden["configured_at"] is not None
        assert "webhook_token_hash" not in client.get("/admin/services").text

        assert client.post(
            f"/hooks/services/{service['id']}/wrong-token",
            headers={"X-Gitea-Event": "push"},
            content=b"{}",
        ).status_code == 404

        ignored = client.post(
            hook_path,
            headers={"X-Gitea-Event": "ping"},
            content=b"{}",
        )
        assert ignored.status_code == 202
        assert ignored.json() == {"status": "ignored", "provider": "gitea"}

        too_large = client.post(
            hook_path,
            headers={"X-Gitlab-Event": "Push Hook"},
            content=b"x" * 1025,
        )
        assert too_large.status_code == 413

        wrong_branch = client.post(
            hook_path,
            headers={"X-Gitlab-Event": "Push Hook"},
            json={"ref": "refs/heads/develop"},
        )
        assert wrong_branch.status_code == 202
        assert wrong_branch.json() == {"status": "ignored", "provider": "gitlab"}

        accepted = client.post(
            hook_path,
            headers={"X-Gitlab-Event": "Push Hook"},
            json={"ref": "refs/heads/main"},
        )
        assert accepted.status_code == 202
        assert accepted.json() == {"status": "accepted", "provider": "gitlab"}

        disabled = client.delete(f"/admin/services/{service['id']}/webhook")
        assert disabled.status_code == 200
        assert disabled.json()["enabled"] is False
        assert client.post(
            hook_path,
            headers={"X-Gitea-Event": "push"},
            content=b"{}",
        ).status_code == 404


def test_revision_persists_object_store_reference(tmp_path: Path) -> None:
    artifact_store = FakeArtifactStore()
    artifact, digest = build_artifact(tmp_path, name="stored-service")
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"),
            grpc_enabled=False,
            ray_use_label_selector=False,
            require_registered_runtime_profiles=False,
        ),
        artifact_store=artifact_store,
    )

    with TestClient(app) as client:
        service = client.post(
            "/admin/services",
            json={
                "name": "stored-service",
                "git_url": "https://example.invalid/stored.git",
            },
        ).json()
        created = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-1",
                "artifact_uri": artifact.as_uri(),
                "artifact_digest": digest,
            },
        )
        assert created.status_code == 201, created.text
        revisions = client.get(
            f"/admin/services/{service['id']}/revisions"
        ).json()

    assert artifact_store.published == [
        (artifact.as_uri(), digest)
    ]
    assert revisions[0]["artifact_uri"] == (
        f"s3://pyscripts/sha256/{digest}.zip"
    )


def test_manual_publish_builds_revision_from_git_head(tmp_path: Path) -> None:
    repository = tmp_path / "source"
    repository.mkdir()
    pyproject = (
        "[project]\n"
        'name = "git-service"\n'
        'version = "1.0.0"\n'
        'requires-python = ">=3.12,<3.13"\n'
        "dependencies = []\n"
        "\n[tool.pyscript]\n"
        "spec_version = 1\n"
        'runtime = "py312-test@latest"\n'
        "\n[[tool.pyscript.endpoints]]\n"
        'id = "run"\n'
        'task_type = "io"\n'
        'entrypoint = "service:run"\n'
        'para = { value = "String" }\n'
        'return = "String"\n'
    )
    (repository / "pyproject.toml").write_text(pyproject, encoding="utf-8")
    (repository / "service.py").write_text(
        "def run(context, value): return value\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "-q"], cwd=repository, check=True)
    subprocess.run(["git", "add", "."], cwd=repository, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-qm",
            "initial",
        ],
        cwd=repository,
        check=True,
    )
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    branch = subprocess.run(
        ["git", "branch", "--show-current"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    app = create_app(
        Settings(
            database_url=SecretStr(
                f"sqlite+aiosqlite:///{tmp_path / 'git-sync.db'}"
            ),
            grpc_enabled=False,
            ray_use_label_selector=False,
            require_registered_runtime_profiles=False,
            actor_cache_root=tmp_path / "cache",
        )
    )

    with TestClient(app) as client:
        service = client.post(
            "/admin/services",
            json={
                "name": "git-service",
                "git_url": str(repository),
                "git_branch": branch,
            },
        ).json()
        assert service["git_branch"] == branch
        first = client.post(f"/admin/services/{service['id']}/revisions")
        assert first.status_code == 201, first.text
        body = first.json()
        assert body["revision"] == revision
        assert body["status"] == "ACTIVE"
        assert body["runtime_profile"] == "py312-test@latest"
        stored = client.get(
            f"/admin/services/{service['id']}/revisions"
        ).json()[0]
        assert stored["artifact_digest"]
        assert Path(stored["artifact_uri"].removeprefix("file://")).is_file()

        repeated = client.post(f"/admin/services/{service['id']}/revisions")
        assert repeated.status_code == 201
        assert repeated.json()["id"] == body["id"]
