from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path

import grpc_tools
from google.protobuf import descriptor_pb2
from google.protobuf.message import DecodeError

from pyscripts.schemas import EndpointSpec, GrpcContractSpec, ResolvedRevisionRequest
from pyscripts.contracts.generator import (
    GENERATED_DESCRIPTOR_PATH,
    ProtoGenerationError,
    generate_contract,
)


class ContractBuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PublishedContract:
    schema_digest: str
    source_digest: str
    contract_version: str
    descriptor_uri: str
    proto_bundle_uri: str
    proto_bundle_digest: str
    methods: list[str]


@dataclass(frozen=True, slots=True)
class PreparedGeneratedArtifact:
    artifact_uri: str
    artifact_digest: str
    endpoints: list[EndpointSpec]
    contract: GrpcContractSpec


class ProtoContractPublisher:
    """Build deterministic descriptor and protobuf source artifacts."""

    def __init__(self, artifact_root: Path):
        self.artifact_root = artifact_root.resolve()
        self.artifact_root.mkdir(parents=True, exist_ok=True)

    def publish(
        self,
        service_name: str,
        request: ResolvedRevisionRequest,
    ) -> PublishedContract | None:
        contract = request.grpc_contract
        grpc_endpoints = [endpoint for endpoint in request.endpoints if endpoint.grpc]
        if contract is None or not grpc_endpoints:
            return None

        with tempfile.TemporaryDirectory(prefix="pyscripts-contract-") as temp_name:
            temporary = Path(temp_name)
            archive = self._obtain_artifact(
                request.artifact_uri,
                request.artifact_digest,
                temporary,
            )
            source_root = temporary / "source"
            self._extract_archive(archive, source_root)
            proto_root = (source_root / contract.proto_root).resolve()
            if (
                not proto_root.is_relative_to(source_root.resolve())
                or not proto_root.is_dir()
            ):
                raise ContractBuildError(
                    f"proto_root does not exist in artifact: {contract.proto_root}"
                )
            proto_files = sorted(proto_root.rglob("*.proto"))
            if not proto_files:
                raise ContractBuildError("proto_root contains no .proto files")

            generated_descriptor = temporary / "descriptor.pb"
            self._run_protoc(
                proto_root, proto_files, generated_descriptor
            )

            canonical_descriptor = _canonical_descriptor(
                generated_descriptor.read_bytes()
            )
            descriptor_path = grpc_endpoints[0].grpc.descriptor_path
            supplied_descriptor_path = (source_root / descriptor_path).resolve()
            if (
                not supplied_descriptor_path.is_relative_to(source_root.resolve())
                or not supplied_descriptor_path.is_file()
            ):
                raise ContractBuildError(
                    f"descriptor does not exist in artifact: {descriptor_path}"
                )
            supplied_descriptor = _canonical_descriptor(
                supplied_descriptor_path.read_bytes()
            )
            if supplied_descriptor != canonical_descriptor:
                raise ContractBuildError(
                    "descriptor.pb does not match the bundled proto sources"
                )

            schema_digest = _sha256(canonical_descriptor)
            source_digest = _source_digest(proto_root, proto_files)
            target = (
                self.artifact_root
                / _safe_token(service_name)
                / schema_digest.removeprefix("sha256:")
            )
            target.mkdir(parents=True, exist_ok=True)

            descriptor_target = target / "descriptor.pb"
            proto_target = target / "proto.zip"

            if not descriptor_target.exists():
                _atomic_write(descriptor_target, canonical_descriptor)
            if not proto_target.exists():
                self._build_proto_bundle(proto_root, proto_files, proto_target)

            methods = sorted(
                endpoint.grpc.method_path
                for endpoint in grpc_endpoints
                if endpoint.grpc is not None
            )
            return PublishedContract(
                schema_digest=schema_digest,
                source_digest=source_digest,
                contract_version=contract.version,
                descriptor_uri=descriptor_target.resolve().as_uri(),
                proto_bundle_uri=proto_target.resolve().as_uri(),
                proto_bundle_digest=_sha256(proto_target.read_bytes()),
                methods=methods,
            )

    def prepare_generated_artifact(
        self,
        service_name: str,
        request: ResolvedRevisionRequest,
        *,
        previous_descriptor: bytes | None = None,
        previous_version: str | None = None,
    ) -> PreparedGeneratedArtifact | None:
        try:
            generated = generate_contract(
                service_name,
                request.endpoints,
                previous_descriptor=previous_descriptor,
                previous_version=previous_version,
            )
        except ProtoGenerationError as error:
            raise ContractBuildError(str(error)) from error
        if generated is None:
            return None

        with tempfile.TemporaryDirectory(prefix="pyscripts-generated-proto-") as name:
            temporary = Path(name)
            archive = self._obtain_artifact(
                request.artifact_uri,
                request.artifact_digest,
                temporary,
            )
            source_root = temporary / "source"
            self._extract_archive(archive, source_root)
            generated_root = source_root / ".pyscripts" / "generated"
            if generated_root.exists():
                raise ContractBuildError(
                    "artifact path .pyscripts/generated is reserved by the platform"
                )
            proto_path = source_root / generated.proto_path
            proto_path.parent.mkdir(parents=True)
            proto_path.write_text(generated.proto_source, encoding="utf-8")

            descriptor_path = source_root / GENERATED_DESCRIPTOR_PATH
            self._run_protoc(
                source_root / generated.contract.proto_root,
                [proto_path],
                descriptor_path,
            )
            canonical_descriptor = _canonical_descriptor(descriptor_path.read_bytes())
            descriptor_path.write_bytes(canonical_descriptor)

            contract = generated.contract
            if previous_descriptor is not None:
                previous_canonical = _canonical_descriptor(previous_descriptor)
                if previous_canonical == canonical_descriptor and previous_version:
                    contract = contract.model_copy(update={"version": previous_version})

            augmented = temporary / "artifact.zip"
            self._build_augmented_artifact(source_root, augmented)
            digest = _sha256(augmented.read_bytes())
            target_root = self.artifact_root / "generated-artifacts"
            target_root.mkdir(parents=True, exist_ok=True)
            target = target_root / f"{digest}.zip"
            if not target.exists():
                temporary_target = target.with_suffix(
                    f".zip.{os.getpid()}.{uuid.uuid4().hex}.tmp"
                )
                shutil.copyfile(augmented, temporary_target)
                os.replace(temporary_target, target)

            return PreparedGeneratedArtifact(
                artifact_uri=target.resolve().as_uri(),
                artifact_digest=digest.removeprefix("sha256:"),
                endpoints=generated.endpoints,
                contract=contract,
            )

    def resolve_local_uri(self, uri: str) -> Path:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme != "file":
            raise ContractBuildError("only local contract artifacts can be downloaded")
        path = Path(urllib.request.url2pathname(parsed.path)).resolve()
        if not path.is_relative_to(self.artifact_root) or not path.is_file():
            raise ContractBuildError("contract artifact is outside the configured root")
        return path

    @staticmethod
    def read_bytes(uri: str, max_bytes: int = 64 * 1024 * 1024) -> bytes:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in {"", "file"}:
            path = Path(urllib.request.url2pathname(parsed.path or uri))
            if not path.is_file():
                raise ContractBuildError(f"contract artifact does not exist: {path}")
            if path.stat().st_size > max_bytes:
                raise ContractBuildError("contract artifact exceeds the size limit")
            return path.read_bytes()
        if parsed.scheme in {"http", "https"}:
            with urllib.request.urlopen(uri, timeout=60) as response:
                payload = response.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise ContractBuildError("contract artifact exceeds the size limit")
            return payload
        raise ContractBuildError("contract artifact URI must use file, http, or https")

    @staticmethod
    def _build_augmented_artifact(source_root: Path, target: Path) -> None:
        files = sorted(path for path in source_root.rglob("*") if path.is_file())
        with zipfile.ZipFile(
            target, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            for path in files:
                relative = path.relative_to(source_root).as_posix()
                info = zipfile.ZipInfo(
                    relative,
                    date_time=(1980, 1, 1, 0, 0, 0),
                )
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, path.read_bytes())

    def _run_protoc(
        self,
        proto_root: Path,
        proto_files: list[Path],
        descriptor_path: Path,
    ) -> None:
        well_known = Path(grpc_tools.__file__).resolve().parent / "_proto"
        relative_files = [str(path.relative_to(proto_root)) for path in proto_files]
        command = [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I{proto_root}",
            f"-I{well_known}",
            "--include_imports",
            f"--descriptor_set_out={descriptor_path}",
            *relative_files,
        ]
        result = subprocess.run(
            command,
            cwd=proto_root,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise ContractBuildError(f"protoc failed: {detail[:4000]}")

    @staticmethod
    def _build_proto_bundle(
        proto_root: Path,
        proto_files: list[Path],
        target: Path,
    ) -> None:
        payload = {
            path.relative_to(proto_root).as_posix(): path.read_bytes()
            for path in proto_files
        }
        _atomic_zip(target, payload)

    @staticmethod
    def _obtain_artifact(uri: str, expected_digest: str, temporary: Path) -> Path:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in {"", "file"}:
            source = Path(urllib.request.url2pathname(parsed.path or uri))
            if not source.is_file():
                raise ContractBuildError(f"artifact does not exist: {source}")
            archive = source
        elif parsed.scheme in {"http", "https"}:
            archive = temporary / "artifact.zip"
            with (
                urllib.request.urlopen(uri, timeout=60) as response,
                archive.open("wb") as output,
            ):
                shutil.copyfileobj(response, output)
        else:
            raise ContractBuildError("artifact URI must use file, http, or https")
        actual = hashlib.sha256(archive.read_bytes()).hexdigest()
        expected = expected_digest.removeprefix("sha256:").lower()
        if actual != expected:
            raise ContractBuildError(
                f"artifact digest mismatch: expected {expected}, got {actual}"
            )
        return archive

    @staticmethod
    def _extract_archive(archive_path: Path, destination: Path) -> None:
        destination.mkdir()
        with zipfile.ZipFile(archive_path) as archive:
            for member in archive.infolist():
                target = (destination / member.filename).resolve()
                if not target.is_relative_to(destination.resolve()):
                    raise ContractBuildError(f"unsafe archive path: {member.filename}")
                file_mode = member.external_attr >> 16
                if file_mode & 0o170000 == 0o120000:
                    raise ContractBuildError(
                        f"symbolic links are not allowed: {member.filename}"
                    )
            archive.extractall(destination)


def _canonical_descriptor(payload: bytes) -> bytes:
    descriptor_set = descriptor_pb2.FileDescriptorSet()
    try:
        descriptor_set.ParseFromString(payload)
    except DecodeError as error:
        raise ContractBuildError("invalid protobuf FileDescriptorSet") from error
    if not descriptor_set.file:
        raise ContractBuildError("protobuf FileDescriptorSet contains no files")
    canonical = descriptor_pb2.FileDescriptorSet()
    for source in sorted(descriptor_set.file, key=lambda item: item.name):
        file_proto = canonical.file.add()
        file_proto.CopyFrom(source)
        file_proto.ClearField("source_code_info")
    return canonical.SerializeToString(deterministic=True)


def _source_digest(proto_root: Path, proto_files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in proto_files:
        relative = path.relative_to(proto_root).as_posix().encode()
        digest.update(len(relative).to_bytes(4, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return f"sha256:{digest.hexdigest()}"


def _sha256(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _safe_token(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:20]


def _atomic_write(path: Path, payload: bytes) -> None:
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _atomic_zip(path: Path, payload: dict[str, bytes]) -> None:
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.{uuid.uuid4().hex}.tmp")
    with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in sorted(payload.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, content)
    os.replace(temporary, path)
