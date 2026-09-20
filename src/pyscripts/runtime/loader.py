from __future__ import annotations

import asyncio
import fcntl
import hashlib
import importlib
import importlib.machinery
import inspect
import shutil
import sys
import tempfile
import types
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from google.protobuf import descriptor_pool

from pyscripts.runtime.protobuf import (
    GrpcCodec,
    GrpcEndpointDefinition,
    load_grpc_codecs,
)

TaskType = Literal["io", "compute"]
VersionState = Literal["LOADING", "READY", "DRAINING"]


class RuntimeLoadError(RuntimeError):
    pass


class VersionDrainingError(RuntimeError):
    pass


class ArtifactArchiveCache:
    """Node-local, content-addressed archive cache shared by worker processes."""

    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def obtain(self, artifact_uri: str, expected_digest: str) -> Path:
        digest = expected_digest.removeprefix("sha256:").lower()
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise RuntimeLoadError("artifact digest must be a SHA-256 hex digest")
        archive_path = self.root / f"{digest}.zip"
        lock_path = self.root / f"{digest}.lock"
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            if archive_path.is_file() and self._digest(archive_path) == digest:
                return archive_path
            archive_path.unlink(missing_ok=True)
            temporary = self.root / f".{digest}.{uuid.uuid4().hex}.part"
            try:
                self._download(artifact_uri, temporary)
                actual = self._digest(temporary)
                if actual != digest:
                    raise RuntimeLoadError(
                        f"artifact digest mismatch: expected {digest}, got {actual}"
                    )
                temporary.replace(archive_path)
            finally:
                temporary.unlink(missing_ok=True)
            return archive_path

    @staticmethod
    def _download(artifact_uri: str, destination: Path) -> None:
        parsed = urllib.parse.urlparse(artifact_uri)
        if parsed.scheme in {"", "file"}:
            source = Path(urllib.request.url2pathname(parsed.path or artifact_uri))
            if not source.is_file():
                raise RuntimeLoadError(f"artifact does not exist: {source}")
            shutil.copyfile(source, destination)
            return
        if parsed.scheme in {"http", "https"}:
            with (
                urllib.request.urlopen(artifact_uri, timeout=60) as response,
                destination.open("wb") as output,
            ):
                shutil.copyfileobj(response, output)
            return
        raise RuntimeLoadError(
            "artifact URI must use file, http, or https; use a presigned URL for S3"
        )

    @staticmethod
    def _digest(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class EndpointDefinition:
    id: str
    task_type: TaskType
    entrypoint: str
    grpc: GrpcEndpointDefinition | None = None

    @classmethod
    def from_manifest(cls, value: Mapping[str, Any]) -> EndpointDefinition:
        grpc_value = value.get("grpc")
        grpc = (
            GrpcEndpointDefinition(
                service=grpc_value["service"],
                method=grpc_value["method"],
                descriptor_path=grpc_value.get("descriptor_path", "descriptor.pb"),
            )
            if grpc_value
            else None
        )
        return cls(
            id=value["id"],
            task_type=value["task_type"],
            entrypoint=value["entrypoint"],
            grpc=grpc,
        )


@dataclass(slots=True)
class LoadedVersion:
    service: str
    revision: str
    module_prefix: str
    root: Path
    runners: dict[str, tuple[TaskType, Callable[..., Any]]]
    grpc_codecs: dict[str, GrpcCodec]
    descriptor_pool: descriptor_pool.DescriptorPool | None = None
    state: VersionState = "READY"
    inflight: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _safe_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:20]


def _create_namespace(name: str, search_path: Path | None = None) -> None:
    if name in sys.modules:
        return
    module = types.ModuleType(name)
    module.__package__ = name
    module.__path__ = [] if search_path is None else [str(search_path)]
    module.__spec__ = importlib.machinery.ModuleSpec(name, loader=None, is_package=True)
    sys.modules[name] = module


def _validate_zip_member(member: zipfile.ZipInfo, destination: Path) -> None:
    target = (destination / member.filename).resolve()
    if destination.resolve() not in target.parents and target != destination.resolve():
        raise RuntimeLoadError(f"unsafe archive path: {member.filename}")
    file_mode = member.external_attr >> 16
    if file_mode & 0o170000 == 0o120000:
        raise RuntimeLoadError(f"symbolic links are not allowed: {member.filename}")


class VersionedRuntime:
    """Loads immutable service revisions into unique Python namespaces."""

    def __init__(
        self,
        cache_root: Path,
        max_io: int = 100,
        artifact_cache_root: Path | None = None,
        retain_extracted_on_unload: bool = False,
    ):
        self.cache_root = cache_root
        self.cache_root.mkdir(parents=True, exist_ok=True)
        self.artifact_cache = ArtifactArchiveCache(
            artifact_cache_root or cache_root.parent / "artifacts"
        )
        self.retain_extracted_on_unload = retain_extracted_on_unload
        self._versions: dict[tuple[str, str], LoadedVersion] = {}
        self._load_tasks: dict[tuple[str, str], asyncio.Task[LoadedVersion]] = {}
        self._registry_lock = asyncio.Lock()
        self._io_executor = ThreadPoolExecutor(max_workers=max_io)
        self._compute_executor = ThreadPoolExecutor(max_workers=1)

    async def ensure_loaded(
        self,
        service: str,
        revision: str,
        artifact_uri: str,
        artifact_digest: str,
        endpoints: list[EndpointDefinition],
    ) -> LoadedVersion:
        key = (service, revision)
        async with self._registry_lock:
            existing = self._versions.get(key)
            if existing is not None:
                return existing
            task = self._load_tasks.get(key)
            if task is None:
                task = asyncio.create_task(
                    asyncio.to_thread(
                        self._load_version,
                        service,
                        revision,
                        artifact_uri,
                        artifact_digest,
                        endpoints,
                    )
                )
                self._load_tasks[key] = task

        try:
            loaded = await task
        except BaseException:
            async with self._registry_lock:
                self._load_tasks.pop(key, None)
            raise

        async with self._registry_lock:
            self._versions[key] = loaded
            self._load_tasks.pop(key, None)
        return loaded

    def _load_version(
        self,
        service: str,
        revision: str,
        artifact_uri: str,
        artifact_digest: str,
        endpoints: list[EndpointDefinition],
    ) -> LoadedVersion:
        service_token = _safe_token(service)
        revision_token = _safe_token(revision)
        version_root = self.cache_root / service_token / revision_token
        normalized_digest = artifact_digest.removeprefix("sha256:").lower()
        digest_marker = version_root / ".pyscripts-artifact.sha256"
        reusable = (
            self.retain_extracted_on_unload
            and version_root.is_dir()
            and digest_marker.is_file()
            and digest_marker.read_text(encoding="ascii").strip() == normalized_digest
        )

        if not reusable:
            archive_path = self._obtain_artifact(artifact_uri, artifact_digest)
            if version_root.exists():
                shutil.rmtree(version_root)
            version_root.parent.mkdir(parents=True, exist_ok=True)
            temporary_root = Path(
                tempfile.mkdtemp(prefix=f".{revision_token}-", dir=version_root.parent)
            )
            try:
                with zipfile.ZipFile(archive_path) as archive:
                    for member in archive.infolist():
                        _validate_zip_member(member, temporary_root)
                    archive.extractall(temporary_root)
                (temporary_root / ".pyscripts-artifact.sha256").write_text(
                    normalized_digest,
                    encoding="ascii",
                )
                temporary_root.rename(version_root)
            except BaseException:
                shutil.rmtree(temporary_root, ignore_errors=True)
                raise

        root_namespace = "_pyscripts_runtime"
        service_namespace = f"{root_namespace}.s_{service_token}"
        module_prefix = f"{service_namespace}.r_{revision_token}"
        _create_namespace(root_namespace)
        _create_namespace(service_namespace)
        _create_namespace(module_prefix, version_root)

        runners: dict[str, tuple[TaskType, Callable[..., Any]]] = {}
        try:
            for endpoint in endpoints:
                module_name, separator, attribute = endpoint.entrypoint.partition(":")
                if not separator:
                    raise RuntimeLoadError(
                        f"invalid entrypoint {endpoint.entrypoint!r}"
                    )
                module = importlib.import_module(f"{module_prefix}.{module_name}")
                runner = getattr(module, attribute, None)
                if runner is None or not callable(runner):
                    raise RuntimeLoadError(
                        f"entrypoint {endpoint.entrypoint!r} is not callable"
                    )
                runners[endpoint.id] = (endpoint.task_type, runner)
            protobuf_pool, grpc_codecs = load_grpc_codecs(
                version_root,
                [
                    (endpoint.id, endpoint.grpc)
                    for endpoint in endpoints
                    if endpoint.grpc is not None
                ],
            )
        except BaseException:
            self._remove_modules(module_prefix)
            shutil.rmtree(version_root, ignore_errors=True)
            raise

        return LoadedVersion(
            service=service,
            revision=revision,
            module_prefix=module_prefix,
            root=version_root,
            runners=runners,
            grpc_codecs=grpc_codecs,
            descriptor_pool=protobuf_pool,
        )

    def _obtain_artifact(self, artifact_uri: str, expected_digest: str) -> Path:
        return self.artifact_cache.obtain(artifact_uri, expected_digest)

    async def execute(
        self,
        service: str,
        revision: str,
        endpoint_id: str,
        context: Mapping[str, Any],
        params: Mapping[str, Any],
        *,
        artifact_uri: str,
        artifact_digest: str,
        endpoints: list[EndpointDefinition],
    ) -> Any:
        loaded = await self.ensure_loaded(
            service,
            revision,
            artifact_uri,
            artifact_digest,
            endpoints,
        )
        async with loaded.lock:
            if loaded.state != "READY":
                raise VersionDrainingError(f"{service}@{revision} is draining")
            runner_info = loaded.runners.get(endpoint_id)
            if runner_info is None:
                raise RuntimeLoadError(f"unknown endpoint: {endpoint_id}")
            loaded.inflight += 1

        task_type, runner = runner_info
        try:
            return await self._invoke_runner(
                task_type,
                runner,
                dict(context),
                keyword_arguments=dict(params),
            )
        finally:
            await self._release(loaded)

    async def execute_grpc(
        self,
        service: str,
        revision: str,
        endpoint_id: str,
        context: Mapping[str, Any],
        payload: bytes,
        *,
        artifact_uri: str,
        artifact_digest: str,
        endpoints: list[EndpointDefinition],
    ) -> bytes:
        loaded = await self.ensure_loaded(
            service,
            revision,
            artifact_uri,
            artifact_digest,
            endpoints,
        )
        async with loaded.lock:
            if loaded.state != "READY":
                raise VersionDrainingError(f"{service}@{revision} is draining")
            runner_info = loaded.runners.get(endpoint_id)
            codec = loaded.grpc_codecs.get(endpoint_id)
            if runner_info is None or codec is None:
                raise RuntimeLoadError(f"unknown gRPC endpoint: {endpoint_id}")
            loaded.inflight += 1

        task_type, runner = runner_info
        try:
            request = codec.parse_request(payload)
            result = await self._invoke_runner(
                task_type,
                runner,
                dict(context),
                positional_arguments=(request,),
            )
            return codec.serialize_response(result)
        finally:
            await self._release(loaded)

    async def _invoke_runner(
        self,
        task_type: TaskType,
        runner: Callable[..., Any],
        context: dict[str, Any],
        *,
        positional_arguments: tuple[Any, ...] = (),
        keyword_arguments: dict[str, Any] | None = None,
    ) -> Any:
        kwargs = keyword_arguments or {}
        prefix = (context,) if self._declares_context(runner) else ()
        args = (*prefix, *positional_arguments)
        if inspect.iscoroutinefunction(runner):
            return await runner(*args, **kwargs)
        loop = asyncio.get_running_loop()
        executor = self._io_executor if task_type == "io" else self._compute_executor
        return await loop.run_in_executor(
            executor,
            lambda: runner(*args, **kwargs),
        )

    @staticmethod
    def _declares_context(runner: Callable[..., Any]) -> bool:
        try:
            parameters = tuple(inspect.signature(runner).parameters.values())
        except (TypeError, ValueError):
            return False
        return bool(
            parameters
            and parameters[0].name == "context"
            and parameters[0].kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
        )

    async def _release(self, loaded: LoadedVersion) -> None:
        should_unload = False
        async with loaded.lock:
            loaded.inflight -= 1
            should_unload = loaded.state == "DRAINING" and loaded.inflight == 0
        if should_unload:
            await self._unload(loaded.service, loaded.revision)

    async def drain(self, service: str, revision: str) -> bool:
        key = (service, revision)
        async with self._registry_lock:
            loaded = self._versions.get(key)
        if loaded is None:
            return True
        async with loaded.lock:
            loaded.state = "DRAINING"
            can_unload = loaded.inflight == 0
        if can_unload:
            await self._unload(service, revision)
        return can_unload

    async def _unload(self, service: str, revision: str) -> None:
        key = (service, revision)
        async with self._registry_lock:
            loaded = self._versions.pop(key, None)
        if loaded is None:
            return
        loaded.runners.clear()
        loaded.grpc_codecs.clear()
        loaded.descriptor_pool = None
        self._remove_modules(loaded.module_prefix)
        if not self.retain_extracted_on_unload:
            await asyncio.to_thread(shutil.rmtree, loaded.root, True)

    @staticmethod
    def _remove_modules(prefix: str) -> None:
        for module_name in list(sys.modules):
            if module_name == prefix or module_name.startswith(f"{prefix}."):
                sys.modules.pop(module_name, None)

    async def status(self) -> dict[str, Any]:
        async with self._registry_lock:
            versions = list(self._versions.values())
        return {
            "versions": [
                {
                    "service": item.service,
                    "revision": item.revision,
                    "state": item.state,
                    "inflight": item.inflight,
                }
                for item in versions
            ]
        }
