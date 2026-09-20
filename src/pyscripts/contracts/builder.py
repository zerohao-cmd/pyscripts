from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import os
import re
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

from pyscripts.schemas import ResolvedRevisionRequest


class ContractBuildError(RuntimeError):
    pass


SDK_TEMPLATE_VERSION = "1"


@dataclass(frozen=True, slots=True)
class PublishedContract:
    schema_digest: str
    source_digest: str
    contract_version: str
    descriptor_uri: str
    proto_bundle_uri: str
    methods: list[str]
    generator_version: str
    package_name: str
    package_version: str
    wheel_uri: str
    wheel_digest: str


class PythonSdkPublisher:
    """Builds deterministic local contract artifacts and a pure-Python wheel."""

    def __init__(self, artifact_root: Path, distribution_prefix: str = "pyscripts"):
        self.artifact_root = artifact_root.resolve()
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.distribution_prefix = distribution_prefix
        grpc_tools_version = importlib.metadata.version("grpcio-tools")
        self.generator_version = (
            f"pyscripts-{SDK_TEMPLATE_VERSION};grpcio-tools-{grpc_tools_version}"
        )

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

            generated_root = temporary / "generated"
            generated_root.mkdir()
            generated_descriptor = temporary / "descriptor.pb"
            self._run_protoc(
                proto_root, proto_files, generated_root, generated_descriptor
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
            package_name = contract.package_name or (
                f"{self.distribution_prefix}-{service_name}-sdk"
            )
            package_name = _normalize_distribution_name(package_name)
            package_version = contract.version
            target = (
                self.artifact_root
                / _safe_token(service_name)
                / schema_digest.removeprefix("sha256:")
            )
            target.mkdir(parents=True, exist_ok=True)

            descriptor_target = target / "descriptor.pb"
            proto_target = target / "proto.zip"
            wheel_name = _wheel_filename(package_name, package_version)
            wheel_target = target / wheel_name

            if not descriptor_target.exists():
                _atomic_write(descriptor_target, canonical_descriptor)
            if not proto_target.exists():
                self._build_proto_bundle(proto_root, proto_files, proto_target)
            if not wheel_target.exists():
                self._prepare_generated_packages(generated_root)
                self._build_wheel(
                    generated_root,
                    wheel_target,
                    package_name,
                    package_version,
                    service_name,
                )

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
                methods=methods,
                generator_version=self.generator_version,
                package_name=package_name,
                package_version=package_version,
                wheel_uri=wheel_target.resolve().as_uri(),
                wheel_digest=_sha256(wheel_target.read_bytes()),
            )

    def resolve_local_uri(self, uri: str) -> Path:
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme != "file":
            raise ContractBuildError("only local contract artifacts can be downloaded")
        path = Path(urllib.request.url2pathname(parsed.path)).resolve()
        if not path.is_relative_to(self.artifact_root) or not path.is_file():
            raise ContractBuildError("contract artifact is outside the configured root")
        return path

    def _run_protoc(
        self,
        proto_root: Path,
        proto_files: list[Path],
        generated_root: Path,
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
            f"--python_out={generated_root}",
            f"--pyi_out={generated_root}",
            f"--grpc_python_out={generated_root}",
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
    def _prepare_generated_packages(generated_root: Path) -> None:
        python_files = sorted(generated_root.rglob("*_pb2.py"))
        if not python_files:
            raise ContractBuildError("protoc generated no Python modules")
        directories = {generated_root}
        for path in python_files:
            directories.update(path.parents)
        for directory in directories:
            if not directory.is_relative_to(generated_root):
                continue
            relative = directory.relative_to(generated_root)
            if relative.parts and not all(
                part.isidentifier() for part in relative.parts
            ):
                raise ContractBuildError(
                    f"proto output path is not a Python package: {relative}"
                )
            (directory / "__init__.py").touch(exist_ok=True)

    def _build_wheel(
        self,
        generated_root: Path,
        target: Path,
        package_name: str,
        package_version: str,
        service_name: str,
    ) -> None:
        import_name = _sdk_import_name(package_name)
        wrapper = generated_root / import_name
        wrapper.mkdir(exist_ok=True)
        modules = sorted(
            path.relative_to(generated_root).with_suffix("")
            for path in generated_root.rglob("*_pb2.py")
        )
        grpc_modules = sorted(
            path.relative_to(generated_root).with_suffix("")
            for path in generated_root.rglob("*_pb2_grpc.py")
        )
        imports = [
            f"from {'.'.join(module.parts)} import *  # noqa: F403"
            for module in [*modules, *grpc_modules]
        ]
        (wrapper / "__init__.py").write_text(
            '"""Generated gRPC client SDK for '
            + service_name
            + '."""\n\n'
            + "\n".join(imports)
            + "\n",
            encoding="utf-8",
        )
        (wrapper / "__init__.pyi").write_text(
            "\n".join(imports) + "\n",
            encoding="utf-8",
        )
        (wrapper / "py.typed").touch()

        distribution = package_name.replace("-", "_")
        dist_info = f"{distribution}-{package_version}.dist-info"
        payload: dict[str, bytes] = {}
        for path in sorted(generated_root.rglob("*")):
            if path.is_file():
                payload[path.relative_to(generated_root).as_posix()] = path.read_bytes()
        payload[f"{dist_info}/METADATA"] = (
            "Metadata-Version: 2.4\n"
            f"Name: {package_name}\n"
            f"Version: {package_version}\n"
            f"Summary: Generated gRPC client SDK for {service_name}\n"
            "Requires-Python: >=3.12\n"
            "Requires-Dist: grpcio>=1.74.0\n"
            "Requires-Dist: protobuf>=6.31.0\n"
            "\n"
        ).encode()
        payload[f"{dist_info}/WHEEL"] = (
            "Wheel-Version: 1.0\n"
            f"Generator: pyscripts grpcio-tools {self.generator_version}\n"
            "Root-Is-Purelib: true\n"
            "Tag: py3-none-any\n\n"
        ).encode()
        record_path = f"{dist_info}/RECORD"
        records = [
            f"{name},sha256={_record_digest(content)},{len(content)}"
            for name, content in sorted(payload.items())
        ]
        records.append(f"{record_path},,")
        payload[record_path] = ("\n".join(records) + "\n").encode()
        _atomic_zip(target, payload)

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


def _record_digest(payload: bytes) -> str:
    return (
        base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
    )


def _safe_token(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:20]


def _normalize_distribution_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def _sdk_import_name(package_name: str) -> str:
    return re.sub(r"\W+", "_", package_name).strip("_").lower()


def _wheel_filename(package_name: str, version: str) -> str:
    distribution = package_name.replace("-", "_")
    return f"{distribution}-{version}-py3-none-any.whl"


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
