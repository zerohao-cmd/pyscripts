from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name
from pydantic import SecretStr

from pyscripts.api import create_app
from pyscripts.config import Settings
from pyscripts.runtime.profiles import (
    RuntimeEnvironmentValidation,
    RuntimeProfileValidationError,
    build_runtime_env,
    materialize_runtime_env,
)


class FakeRuntimeValidator:
    async def validate(
        self,
        *,
        profile_ref: str,
        python_version: str,
        worker_pool: str,
        dependencies: list[str],
        import_checks: list[str],
        pip_source: str,
    ) -> RuntimeEnvironmentValidation:
        resolved = {
            canonicalize_name(requirement.name): next(
                item.version
                for item in requirement.specifier
                if item.operator in {"==", "==="}
            )
            for requirement in map(Requirement, dependencies)
        }
        return RuntimeEnvironmentValidation(
            resolved_dependencies=resolved,
            validation_result={
                "python": f"{python_version}.0",
                "worker_pool": worker_pool,
                "imports": import_checks,
                "pip_check": "OK",
            },
            runtime_env=build_runtime_env(dependencies, 900),
            environment_digest=hashlib.sha256(profile_ref.encode()).hexdigest(),
        )


class FakeProfileScheduler:
    def __init__(self) -> None:
        self.retired: list[str] = []

    async def retire_profile(
        self, environment_digest: str, timeout_seconds: float = 300.0
    ) -> bool:
        self.retired.append(environment_digest)
        return True


def test_private_pypi_options_are_materialized_without_mutating_template() -> None:
    template = build_runtime_env(["example==1.2.3"], 900)
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        private_pip_index_url=SecretStr(
            "https://user:secret@packages.example.com/simple"
        ),
        private_pip_extra_index_urls=[
            SecretStr("https://mirror.example.com/simple")
        ],
        private_pip_trusted_hosts=["packages.example.com"],
    )

    materialized = materialize_runtime_env(template, "private", settings)

    assert "pip_install_options" not in template["pip"]
    assert materialized["pip"]["pip_install_options"] == [
        "--disable-pip-version-check",
        "--no-cache-dir",
        "--index-url",
        "https://user:secret@packages.example.com/simple",
        "--extra-index-url",
        "https://mirror.example.com/simple",
        "--trusted-host",
        "packages.example.com",
    ]


def test_private_pypi_requires_control_plane_configuration() -> None:
    settings = Settings(database_url=SecretStr("sqlite+aiosqlite:///:memory:"))

    try:
        materialize_runtime_env(build_runtime_env([], 900), "private", settings)
    except RuntimeProfileValidationError as error:
        assert "not configured" in str(error)
    else:
        raise AssertionError("private source without an index URL must fail")


def build_artifact(
    tmp_path: Path,
    dependency: str,
    profile_ref: str,
) -> tuple[str, str]:
    artifact = tmp_path / f"service-{profile_ref}-{dependency.split('=')[0]}.zip"
    pyproject = (
        "[project]\n"
        'name = "example-service"\n'
        'version = "1.0.0"\n'
        'requires-python = ">=3.12,<3.13"\n'
        f'dependencies = ["{dependency}"]\n'
        "\n[tool.pyscript]\n"
        "spec_version = 1\n"
        "\n[tool.pyscript.runtime]\n"
        f'label = "{profile_ref}"\n'
        "\n[[tool.pyscript.endpoints]]\n"
        'id = "run"\n'
        'task_type = "io"\n'
        'entrypoint = "service:run"\n'
    )
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("pyproject.toml", pyproject)
        archive.writestr("service.py", "def run(context, params): return params\n")
    return artifact.as_uri(), hashlib.sha256(artifact.read_bytes()).hexdigest()


def test_runtime_profile_lifecycle_and_service_binding(tmp_path: Path) -> None:
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'api.db'}"),
            grpc_enabled=False,
            ray_use_label_selector=False,
        ),
        runtime_profile_validator=FakeRuntimeValidator(),
    )
    compatible_uri, compatible_digest = build_artifact(
        tmp_path, "requests>=2", "data-default"
    )
    incompatible_uri, incompatible_digest = build_artifact(
        tmp_path, "numpy>=2", "data-default@latest"
    )
    latest_uri, latest_digest = build_artifact(
        tmp_path, "requests>=2", "data-default@latest"
    )

    with TestClient(app) as client:
        scheduler = FakeProfileScheduler()
        app.state.profile_scheduler = scheduler
        worker_pools = client.get("/admin/worker-pools")
        assert worker_pools.status_code == 200
        assert worker_pools.json() == [
            {
                "name": "default",
                "label_key": "pyscripts.worker-pool",
                "node_count": 0,
                "source": "CONFIG",
                "mutable": False,
            }
        ]
        unknown_pool = client.post(
            "/admin/runtime-labels",
            json={
                "name": "unknown-pool",
                "python_version": "3.12",
                "worker_pool": "not-provided-by-k8s",
                "dependencies": [],
                "import_checks": [],
            },
        )
        assert unknown_pool.status_code == 422

        created = client.post(
            "/admin/runtime-labels",
            json={
                "name": "data-default",
                "python_version": "3.12",
                "worker_pool": "default",
                "dependencies": ["requests==2.32.3"],
                "import_checks": ["requests"],
            },
        )
        assert created.status_code == 201, created.text
        version_1 = created.json()
        assert version_1["profile_ref"] == "data-default@v1"
        assert version_1["status"] == "ACTIVE"
        assert version_1["worker_pool"] == "default"
        assert version_1["pip_source"] == "default"

        service = client.post(
            "/admin/services",
            json={
                "name": "data-service",
                "git_url": "https://example.invalid/data.git",
            },
        ).json()
        revision = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-1",
                "artifact_uri": latest_uri,
                "artifact_digest": latest_digest,
            },
        )
        assert revision.status_code == 201, revision.text
        assert revision.json()["runtime_profile"] == "data-default@v1"

        rejected = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-bad",
                "artifact_uri": incompatible_uri,
                "artifact_digest": incompatible_digest,
            },
        )
        assert rejected.status_code == 422, rejected.text

        second_service = client.post(
            "/admin/services",
            json={
                "name": "second-data-service",
                "git_url": "https://example.invalid/second-data.git",
            },
        ).json()
        second_revision = client.post(
            f"/admin/services/{second_service['id']}/revisions/import",
            json={
                "revision": "rev-1",
                "artifact_uri": compatible_uri,
                "artifact_digest": compatible_digest,
            },
        )
        assert second_revision.status_code == 201, second_revision.text
        assert second_revision.json()["runtime_profile"] == "data-default@v1"

        another_revision = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-2",
                "artifact_uri": latest_uri,
                "artifact_digest": latest_digest,
            },
        )
        assert another_revision.status_code == 201, another_revision.text
        assert another_revision.json()["runtime_profile"] == "data-default@v1"

        version_2_response = client.post(
            f"/admin/runtime-labels/{version_1['label_id']}/versions",
            json={
                "python_version": "3.12",
                "worker_pool": "default",
                "dependencies": ["requests==2.32.4"],
                "import_checks": ["requests"],
            },
        )
        assert version_2_response.status_code == 201
        version_2 = version_2_response.json()
        assert version_2["status"] == "READY"

        activated = client.post(
            f"/admin/runtime-profile-versions/{version_2['id']}/activate"
        )
        assert activated.status_code == 200, activated.text
        assert activated.json()["status"] == "ACTIVE"
        # One service can retain several revisions on the same runtime. It must
        # still contribute exactly one reference to the environment version.
        assert activated.json()["reference_count"] == 2

        active_retire = client.post(
            f"/admin/runtime-profile-versions/{version_2['id']}/retire"
        )
        assert active_retire.status_code == 409

        labels = client.get("/admin/runtime-labels").json()
        versions = {item["version"]: item for item in labels[0]["versions"]}
        assert versions[2]["status"] == "ACTIVE"
        assert versions[2]["reference_count"] == 2

        revisions = client.get(
            f"/admin/services/{service['id']}/revisions"
        ).json()
        assert {item["runtime_profile"] for item in revisions} == {
            "data-default@v2"
        }

        version_3 = client.post(
            f"/admin/runtime-labels/{version_1['label_id']}/versions",
            json={
                "python_version": "3.12",
                "worker_pool": "default",
                "dependencies": ["numpy==2.1.0"],
                "import_checks": ["numpy"],
            },
        ).json()
        activated_v3 = client.post(
            f"/admin/runtime-profile-versions/{version_3['id']}/activate"
        )
        assert activated_v3.status_code == 409, activated_v3.text
        labels = client.get("/admin/runtime-labels").json()
        assert labels[0]["active_version_id"] == version_2["id"]
        revisions = client.get(
            f"/admin/services/{service['id']}/revisions"
        ).json()
        assert {item["runtime_profile"] for item in revisions} == {
            "data-default@v2"
        }
        second_revisions = client.get(
            f"/admin/services/{second_service['id']}/revisions"
        ).json()
        assert second_revisions[0]["runtime_profile"] == "data-default@v2"
