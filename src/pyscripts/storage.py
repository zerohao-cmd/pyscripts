from __future__ import annotations

import hashlib
import shutil
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Protocol

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from pyscripts.config import Settings


class ArtifactStoreError(RuntimeError):
    pass


class ArtifactStore(Protocol):
    def publish(self, source_uri: str, expected_digest: str) -> str: ...

    def publish_blob(
        self,
        source_uri: str,
        expected_digest: str,
        *,
        category: str,
        suffix: str,
        content_type: str,
    ) -> str: ...

    def distribution_uri(self, stored_uri: str) -> str: ...


class PassthroughArtifactStore:
    """Content-addressed local store used when S3 is disabled."""

    def __init__(self, root: Path | None = None):
        self.root = root or Path("/tmp/pyscripts-artifacts")

    def publish(self, source_uri: str, expected_digest: str) -> str:
        return self.publish_blob(
            source_uri,
            expected_digest,
            category="artifacts",
            suffix=".zip",
            content_type="application/zip",
        )

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
        digest = _normalize_digest(expected_digest)
        safe_category = _normalize_category(category)
        safe_suffix = _normalize_suffix(suffix)
        category_root = (
            self.root if safe_category == "artifacts" else self.root / safe_category
        )
        destination = category_root / "sha256" / digest[:2] / f"{digest}{safe_suffix}"
        if destination.is_file() and _file_digest(destination) == digest:
            return destination.as_uri()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            suffix=safe_suffix,
            dir=destination.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        try:
            _download_file_or_http(source_uri, temporary_path)
            actual = _file_digest(temporary_path)
            if actual != digest:
                raise ArtifactStoreError(
                    f"artifact digest mismatch: expected {digest}, got {actual}"
                )
            temporary_path.replace(destination)
        finally:
            temporary_path.unlink(missing_ok=True)
        return destination.as_uri()

    def distribution_uri(self, stored_uri: str) -> str:
        return stored_uri


class S3ArtifactStore:
    """Content-addressed S3-compatible storage for immutable business ZIPs."""

    def __init__(self, settings: Settings, client: Any | None = None):
        if not settings.object_store_bucket:
            raise ArtifactStoreError(
                "PYSCRIPTS_OBJECT_STORE_BUCKET is required when object storage is enabled"
            )
        self.bucket = settings.object_store_bucket
        self.prefix = settings.object_store_artifact_prefix.strip("/")
        self.presign_ttl_seconds = settings.object_store_presign_ttl_seconds
        if client is None:
            client_options: dict[str, Any] = {
                "service_name": "s3",
                "region_name": settings.object_store_region,
                "endpoint_url": settings.object_store_endpoint_url,
                "verify": settings.object_store_verify_ssl,
                "config": Config(
                    signature_version="s3v4",
                    s3={"addressing_style": settings.object_store_addressing_style},
                    connect_timeout=settings.object_store_connect_timeout_seconds,
                    read_timeout=settings.object_store_read_timeout_seconds,
                    retries={"max_attempts": 3, "mode": "standard"},
                ),
            }
            if settings.object_store_access_key_id is not None:
                client_options["aws_access_key_id"] = (
                    settings.object_store_access_key_id.get_secret_value()
                )
            if settings.object_store_secret_access_key is not None:
                client_options["aws_secret_access_key"] = (
                    settings.object_store_secret_access_key.get_secret_value()
                )
            if settings.object_store_session_token is not None:
                client_options["aws_session_token"] = (
                    settings.object_store_session_token.get_secret_value()
                )
            client = boto3.client(**client_options)
        self.client = client

    def publish(self, source_uri: str, expected_digest: str) -> str:
        return self.publish_blob(
            source_uri,
            expected_digest,
            category="artifacts",
            suffix=".zip",
            content_type="application/zip",
        )

    def publish_blob(
        self,
        source_uri: str,
        expected_digest: str,
        *,
        category: str,
        suffix: str,
        content_type: str,
    ) -> str:
        digest = _normalize_digest(expected_digest)
        key = self._blob_key(category, digest, suffix)
        existing = self._head(key)
        if (
            existing is not None
            and existing.get("Metadata", {}).get("sha256") == digest
        ):
            return self._stored_uri(key)

        with tempfile.NamedTemporaryFile(
            suffix=_normalize_suffix(suffix)
        ) as temporary:
            self._download_source(source_uri, Path(temporary.name))
            actual = _file_digest(Path(temporary.name))
            if actual != digest:
                raise ArtifactStoreError(
                    f"artifact digest mismatch: expected {digest}, got {actual}"
                )
            temporary.seek(0)
            try:
                self.client.upload_fileobj(
                    temporary,
                    self.bucket,
                    key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "Metadata": {"sha256": digest},
                    },
                )
            except (BotoCoreError, ClientError, OSError) as error:
                raise ArtifactStoreError(
                    f"failed to upload artifact to s3://{self.bucket}/{key}: {error}"
                ) from error
        return self._stored_uri(key)

    def distribution_uri(self, stored_uri: str) -> str:
        parsed = urllib.parse.urlparse(stored_uri)
        if parsed.scheme != "s3":
            return stored_uri
        bucket = parsed.netloc
        key = urllib.parse.unquote(parsed.path.lstrip("/"))
        if bucket != self.bucket or not key:
            raise ArtifactStoreError(
                f"artifact reference must belong to configured bucket {self.bucket!r}"
            )
        try:
            return str(
                self.client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=self.presign_ttl_seconds,
                )
            )
        except (BotoCoreError, ClientError) as error:
            raise ArtifactStoreError(
                f"failed to sign artifact download URL for {stored_uri}: {error}"
            ) from error

    def _head(self, key: str) -> dict[str, Any] | None:
        try:
            return dict(self.client.head_object(Bucket=self.bucket, Key=key))
        except ClientError as error:
            code = str(error.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise ArtifactStoreError(
                f"failed to inspect s3://{self.bucket}/{key}: {error}"
            ) from error
        except BotoCoreError as error:
            raise ArtifactStoreError(
                f"failed to inspect s3://{self.bucket}/{key}: {error}"
            ) from error

    def _blob_key(self, category: str, digest: str, suffix: str) -> str:
        safe_category = _normalize_category(category)
        safe_suffix = _normalize_suffix(suffix)
        category_prefix = "" if safe_category == "artifacts" else f"{safe_category}/"
        path = f"{category_prefix}sha256/{digest[:2]}/{digest}{safe_suffix}"
        return f"{self.prefix}/{path}" if self.prefix else path

    def _stored_uri(self, key: str) -> str:
        return f"s3://{self.bucket}/{urllib.parse.quote(key, safe='/')}"

    def _download_source(self, source_uri: str, destination: Path) -> None:
        parsed = urllib.parse.urlparse(source_uri)
        if parsed.scheme in {"", "file"}:
            source = Path(urllib.request.url2pathname(parsed.path or source_uri))
            if not source.is_file():
                raise ArtifactStoreError(f"artifact does not exist: {source}")
            shutil.copyfile(source, destination)
            return
        if parsed.scheme in {"http", "https"}:
            try:
                with (
                    urllib.request.urlopen(source_uri, timeout=120) as response,
                    destination.open("wb") as output,
                ):
                    shutil.copyfileobj(response, output)
            except OSError as error:
                raise ArtifactStoreError(
                    f"failed to download source artifact: {error}"
                ) from error
            return
        if parsed.scheme == "s3":
            signed = self.distribution_uri(source_uri)
            self._download_source(signed, destination)
            return
        raise ArtifactStoreError(
            "source artifact URI must use file, http, https, or the configured s3 bucket"
        )


def create_artifact_store(settings: Settings) -> ArtifactStore:
    if not settings.object_store_enabled:
        return PassthroughArtifactStore(settings.actor_cache_root / "artifacts")
    return S3ArtifactStore(settings)


def _normalize_digest(value: str) -> str:
    digest = value.removeprefix("sha256:").lower()
    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise ArtifactStoreError("artifact digest must be a SHA-256 hex digest")
    return digest


def _normalize_category(value: str) -> str:
    parts = value.strip("/").split("/")
    if not parts or any(
        not part or part in {".", ".."} or not part.replace("-", "").isalnum()
        for part in parts
    ):
        raise ArtifactStoreError("blob category must contain safe path segments")
    return "/".join(parts)


def _normalize_suffix(value: str) -> str:
    if (
        not value.startswith(".")
        or "/" in value
        or "\\" in value
        or len(value) > 16
        or not value[1:].replace(".", "").isalnum()
    ):
        raise ArtifactStoreError("blob suffix is invalid")
    return value


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _download_file_or_http(source_uri: str, destination: Path) -> None:
    parsed = urllib.parse.urlparse(source_uri)
    if parsed.scheme in {"", "file"}:
        source = Path(urllib.request.url2pathname(parsed.path or source_uri))
        if not source.is_file():
            raise ArtifactStoreError(f"artifact does not exist: {source}")
        shutil.copyfile(source, destination)
        return
    if parsed.scheme in {"http", "https"}:
        try:
            with (
                urllib.request.urlopen(source_uri, timeout=120) as response,
                destination.open("wb") as output,
            ):
                shutil.copyfileobj(response, output)
        except OSError as error:
            raise ArtifactStoreError(
                f"failed to download source artifact: {error}"
            ) from error
        return
    raise ArtifactStoreError("source artifact URI must use file, http, or https")
