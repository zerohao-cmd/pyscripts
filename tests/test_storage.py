from __future__ import annotations

import hashlib
from pathlib import Path

from botocore.exceptions import ClientError
from pydantic import SecretStr

from pyscripts.config import Settings
from pyscripts.storage import S3ArtifactStore


class FakeS3Client:
    def __init__(self) -> None:
        self.uploads: list[tuple[bytes, str, str, dict]] = []

    def head_object(self, *, Bucket: str, Key: str):
        raise ClientError(
            {"Error": {"Code": "404", "Message": "not found"}},
            "HeadObject",
        )

    def upload_fileobj(self, source, bucket: str, key: str, ExtraArgs: dict):
        self.uploads.append((source.read(), bucket, key, ExtraArgs))

    def generate_presigned_url(self, operation: str, *, Params: dict, ExpiresIn: int):
        assert operation == "get_object"
        return (
            f"http://objects.internal/{Params['Bucket']}/{Params['Key']}"
            f"?expires={ExpiresIn}"
        )


def test_s3_artifact_store_publishes_by_content_digest(tmp_path: Path) -> None:
    artifact = tmp_path / "service.zip"
    artifact.write_bytes(b"immutable artifact")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    client = FakeS3Client()
    store = S3ArtifactStore(
        Settings(
            database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
            object_store_enabled=True,
            object_store_bucket="pyscripts",
            object_store_artifact_prefix="business/artifacts",
        ),
        client=client,
    )

    stored = store.publish(artifact.as_uri(), digest)

    expected_key = f"business/artifacts/sha256/{digest[:2]}/{digest}.zip"
    assert stored == f"s3://pyscripts/{expected_key}"
    assert client.uploads == [
        (
            b"immutable artifact",
            "pyscripts",
            expected_key,
            {
                "ContentType": "application/zip",
                "Metadata": {"sha256": digest},
            },
        )
    ]
    assert store.distribution_uri(stored) == (
        f"http://objects.internal/pyscripts/{expected_key}?expires=3600"
    )
