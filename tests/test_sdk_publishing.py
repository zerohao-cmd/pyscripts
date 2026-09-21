from __future__ import annotations

import hashlib
import shutil
import zipfile
from pathlib import Path

from fastapi.testclient import TestClient
from grpc_tools import protoc
from pydantic import SecretStr

from pyscripts.api import create_app
from pyscripts.config import Settings

PROTO = """syntax = "proto3";
package examples.math.v1;

service MathService {
  rpc Add(AddRequest) returns (AddResponse);
}

message AddRequest {
  int64 left = 1;
  int64 right = 2;
}

message AddResponse {
  int64 value = 1;
}
"""


def _build_artifact(
    tmp_path: Path,
    *,
    name: str = "math",
    include_contract: bool = True,
) -> tuple[Path, str]:
    source = tmp_path / f"source-{name}"
    proto_root = source / "proto"
    proto_file = proto_root / "examples" / "math" / "v1" / "math.proto"
    proto_file.parent.mkdir(parents=True)
    proto_file.write_text(PROTO)
    descriptor = source / "descriptor.pb"
    result = protoc.main(
        [
            "grpc_tools.protoc",
            f"-I{proto_root}",
            "--include_imports",
            f"--descriptor_set_out={descriptor}",
            str(proto_file.relative_to(proto_root)),
        ]
    )
    assert result == 0

    contract = (
        "\n[tool.pyscript.grpc_contract]\n"
        'version = "1.0.0"\n'
        'proto_root = "proto"\n'
        if include_contract
        else ""
    )
    pyproject = (
        "[project]\n"
        f'name = "{name}"\n'
        'version = "1.0.0"\n'
        'requires-python = ">=3.12,<3.13"\n'
        "\n[tool.pyscript]\n"
        "spec_version = 1\n"
        "\n[tool.pyscript.runtime]\n"
        'label = "py312-test"\n'
        f"{contract}"
        "\n[[tool.pyscript.endpoints]]\n"
        'id = "add"\n'
        'task_type = "compute"\n'
        'entrypoint = "service:add"\n'
        "\n[tool.pyscript.endpoints.grpc]\n"
        'service = "examples.math.v1.MathService"\n'
        'method = "Add"\n'
        'descriptor_path = "descriptor.pb"\n'
    )
    artifact = tmp_path / f"{name}.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("pyproject.toml", pyproject)
        archive.write(
            proto_file,
            "proto/examples/math/v1/math.proto",
        )
        archive.write(descriptor, "descriptor.pb")
        archive.writestr(
            "service.py",
            "def add(context, request):\n"
            "    return {'value': request.left + request.right}\n",
        )
    return artifact, hashlib.sha256(artifact.read_bytes()).hexdigest()


def _build_generated_contract_artifact(tmp_path: Path) -> tuple[Path, str]:
    pyproject = """[project]
name = "auto-math"
version = "1.0.0"
requires-python = ">=3.12,<3.13"

[tool.pyscript]
spec_version = 1

[tool.pyscript.runtime]
label = "py312-test"

[[tool.pyscript.endpoints]]
id = "add"
task_type = "compute"
entrypoint = "service:add"
io_type = ["rest", "grpc"]

[tool.pyscript.endpoints.x]
type = "Int64"

[tool.pyscript.endpoints.y]
type = "Int64"

[tool.pyscript.endpoints.response_schema]
type = "Int64"
"""
    artifact = tmp_path / "auto-math.zip"
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("pyproject.toml", pyproject)
        archive.writestr("service.py", "def add(x, y):\n    return x + y\n")
    return artifact, hashlib.sha256(artifact.read_bytes()).hexdigest()


def test_grpc_contract_publishes_persistent_proto_bundle(
    tmp_path: Path,
) -> None:
    artifact, digest = _build_artifact(tmp_path)
    invalid_artifact, invalid_digest = _build_artifact(
        tmp_path,
        name="math-missing-contract",
        include_contract=False,
    )
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'api.db'}"),
            contract_artifact_root=tmp_path / "contracts",
            actor_cache_root=tmp_path / "runtime",
            grpc_host="127.0.0.1",
            grpc_port=0,
            ray_use_label_selector=False,
            require_registered_runtime_profiles=False,
        )
    )
    revision_body = {
        "artifact_uri": artifact.as_uri(),
        "artifact_digest": digest,
    }

    with TestClient(app) as client:
        service_response = client.post(
            "/admin/services",
            json={
                "name": "math-service",
                "git_url": "https://example.invalid/math.git",
            },
        )
        service_id = service_response.json()["id"]

        invalid_response = client.post(
            f"/admin/services/{service_id}/revisions/import",
            json={
                "revision": "invalid",
                "artifact_uri": invalid_artifact.as_uri(),
                "artifact_digest": invalid_digest,
            },
        )
        assert invalid_response.status_code == 422

        first_response = client.post(
            f"/admin/services/{service_id}/revisions/import",
            json={"revision": "rev-1", **revision_body},
        )
        assert first_response.status_code == 201, first_response.text
        first_revision = first_response.json()
        activation = client.post(
            f"/admin/services/{service_id}/revisions/{first_revision['id']}/activate"
        )
        assert activation.status_code == 200

        contract_response = client.get("/v1/services/math-service/grpc-contract")
        assert contract_response.status_code == 200
        contract = contract_response.json()
        assert contract["contract_version"] == "1.0.0"
        assert contract["revision"] == "rev-1"
        assert contract["methods"] == ["/examples.math.v1.MathService/Add"]
        assert "python_sdk" not in contract
        assert "descriptor_url" not in contract
        assert contract["proto_bundle_digest"].startswith("sha256:")

        first_proto = client.get(contract["proto_bundle_url"])
        assert first_proto.status_code == 200
        shutil.rmtree(tmp_path / "contracts")
        recovered = client.get("/v1/services/math-service/grpc-contract")
        assert recovered.status_code == 200, recovered.text
        contract = recovered.json()

        proto_response = client.get(contract["proto_bundle_url"])
        assert proto_response.status_code == 200
        assert proto_response.content == first_proto.content
        proto_bundle = tmp_path / "proto.zip"
        proto_bundle.write_bytes(proto_response.content)
        with zipfile.ZipFile(proto_bundle) as archive:
            names = set(archive.namelist())
        assert names == {"examples/math/v1/math.proto"}

        second_response = client.post(
            f"/admin/services/{service_id}/revisions/import",
            json={"revision": "rev-2", **revision_body},
        )
        assert second_response.status_code == 201, second_response.text
        second_revision = second_response.json()
        activation = client.post(
            f"/admin/services/{service_id}/revisions/{second_revision['id']}/activate"
        )
        assert activation.status_code == 200
        second_contract = client.get("/v1/services/math-service/grpc-contract").json()
        assert second_contract["id"] == contract["id"]
        assert second_contract["revision"] == "rev-2"


def test_interface_metadata_generates_proto_bundle(tmp_path: Path) -> None:
    artifact, digest = _build_generated_contract_artifact(tmp_path)
    app = create_app(
        Settings(
            database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'auto.db'}"),
            contract_artifact_root=tmp_path / "contracts-auto",
            actor_cache_root=tmp_path / "runtime-auto",
            grpc_host="127.0.0.1",
            grpc_port=0,
            ray_use_label_selector=False,
            require_registered_runtime_profiles=False,
        )
    )

    with TestClient(app) as client:
        service = client.post(
            "/admin/services",
            json={
                "name": "auto-math",
                "git_url": "https://example.invalid/auto-math.git",
            },
        ).json()
        response = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-1",
                "artifact_uri": artifact.as_uri(),
                "artifact_digest": digest,
            },
        )
        assert response.status_code == 201, response.text
        revision = response.json()
        detail = client.get(f"/admin/services/{service['id']}").json()
        endpoint = detail["endpoints"][0]
        assert endpoint["io_type"] == ["rest", "grpc"]
        assert endpoint["grpc"]["generated"] is True

        activation = client.post(
            f"/admin/services/{service['id']}/revisions/{revision['id']}/activate"
        )
        assert activation.status_code == 200, activation.text
        contract_response = client.get("/v1/services/auto-math/grpc-contract")
        assert contract_response.status_code == 200, contract_response.text
        contract = contract_response.json()
        assert contract["contract_version"] == "1.0.0"
        assert contract["methods"] == [
            "/pyscripts.generated.auto_math.v1.AutoMathService/Add"
        ]

        proto_response = client.get(contract["proto_bundle_url"])
        assert proto_response.status_code == 200
        proto_bundle = tmp_path / "auto-proto.zip"
        proto_bundle.write_bytes(proto_response.content)
        with zipfile.ZipFile(proto_bundle) as archive:
            proto_name = next(
                name for name in archive.namelist() if name.endswith("/service.proto")
            )
            proto_source = archive.read(proto_name).decode()
        assert "rpc Add(AddRequest) returns (AddResponse);" in proto_source
        assert "int64 x = 1;" in proto_source
        assert "int64 y = 2;" in proto_source
        assert "python_sdk" not in contract

        second = client.post(
            f"/admin/services/{service['id']}/revisions/import",
            json={
                "revision": "rev-2",
                "artifact_uri": artifact.as_uri(),
                "artifact_digest": digest,
            },
        )
        assert second.status_code == 201, second.text
        second_contract = client.get(
            "/v1/services/auto-math/grpc-contract"
        ).json()
        assert second_contract["id"] == contract["id"]
        assert second_contract["contract_version"] == "1.0.0"
        assert second_contract["revision"] == "rev-2"
