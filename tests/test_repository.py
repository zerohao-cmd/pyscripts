from __future__ import annotations

from dataclasses import replace

import pytest
from pydantic import SecretStr
from sqlalchemy import func, select

from pyscripts.config import Settings
from pyscripts.contracts.builder import PublishedContract
from pyscripts.db import (
    create_engine,
    create_schema,
    create_session_factory,
    session_scope,
)
from pyscripts.models import (
    Endpoint,
    ExecutionKind,
    Invocation,
    InvocationExecution,
    InvocationStatus,
    Revision,
    RevisionStatus,
    ServiceStatus,
)
from pyscripts.repository import (
    InvalidTransitionError,
    NotFoundError,
    PlatformRepository,
)
from pyscripts.schemas import CreateServiceRequest, ResolvedRevisionRequest


def revision_request(name: str) -> ResolvedRevisionRequest:
    return ResolvedRevisionRequest(
        revision=name,
        artifact_uri=f"file:///tmp/{name}.zip",
        artifact_digest="a" * 64,
        runtime_profile="py312-test@v1",
        requires_python=">=3.12,<3.13",
        dependencies=[],
        endpoints=[
            {
                "id": "add",
                "task_type": "compute",
                "entrypoint": "service:add",
            }
        ],
    )


def grpc_revision_request(name: str, method_path: str) -> ResolvedRevisionRequest:
    service, method = method_path.removeprefix("/").split("/", 1)
    return ResolvedRevisionRequest(
        revision=name,
        artifact_uri=f"file:///tmp/{name}.zip",
        artifact_digest="b" * 64,
        runtime_profile="py312-test@v1",
        requires_python=">=3.12,<3.13",
        dependencies=[],
        grpc_contract={"version": "1.0.0", "proto_root": "proto"},
        endpoints=[
            {
                "id": "echo",
                "task_type": "io",
                "entrypoint": "service:echo",
                "io_type": ["grpc"],
                "parameters": {"value": {"type": "String"}},
                "response_schema": {"type": "String"},
                "grpc": {
                    "service": service,
                    "method": method,
                    "descriptor_path": "descriptor.pb",
                },
            }
        ],
    )


def published_contract(method_path: str) -> PublishedContract:
    return PublishedContract(
        schema_digest="sha256:" + "c" * 64,
        source_digest="sha256:" + "d" * 64,
        contract_version="1.0.0",
        descriptor_uri="file:///tmp/descriptor.pb",
        proto_bundle_uri="file:///tmp/proto.zip",
        proto_bundle_digest="sha256:" + "e" * 64,
        methods=[method_path],
    )


async def test_activate_revision_drains_previous_revision() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        auto_create_schema=True,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        service = await repository.create_service(
            CreateServiceRequest(
                name="math-service",
                git_url="https://example.invalid/math.git",
            )
        )
        first = await repository.create_revision(service.id, revision_request("rev-1"))
        second = await repository.create_revision(service.id, revision_request("rev-2"))
        await repository.activate_revision(service.id, first.id)
        await repository.activate_revision(service.id, second.id)
        service_id = service.id
        first_id = first.id
        second_id = second.id

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        target = await repository.resolve_endpoint("math-service", "add")
        target = replace(target, environment_digest="environment-v1")
        invocation = await repository.begin_invocation(target, transport="REST")
        execution = await session.get(InvocationExecution, invocation.id)
        assert invocation.transport == "REST"
        assert await repository.active_runtime_execution_count("environment-v1") == 1
        await repository.finish_invocation(invocation.id, InvocationStatus.SUCCEEDED)
        assert await repository.active_runtime_execution_count("environment-v1") == 0
        revisions = {
            item.id: item.status
            for item in await session.scalars(
                select(Revision).where(Revision.service_id == service_id)
            )
        }

    assert target.revision == "rev-2"
    assert target.task_type == "compute"
    assert target.grpc is None
    assert execution is not None
    assert execution.execution_kind == ExecutionKind.COMPUTE_TASK
    assert execution.artifact_digest == "a" * 64
    assert revisions[first_id] == RevisionStatus.DRAINING
    assert revisions[second_id] == RevisionStatus.ACTIVE

    await engine.dispose()


async def test_legacy_interface_cannot_be_activated_started_or_routed() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        auto_create_schema=True,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        service = await repository.create_service(
            CreateServiceRequest(
                name="legacy-service",
                git_url="https://example.invalid/legacy.git",
            )
        )
        revision = await repository.create_revision(
            service.id, revision_request("legacy-rev")
        )
        revision.manifest = {**revision.manifest, "spec_version": 1}
        await session.flush()

        with pytest.raises(InvalidTransitionError, match="unsupported interface"):
            await repository.activate_revision(service.id, revision.id)

        # Simulate a legacy deployment that was active before spec v2 existed.
        revision.status = RevisionStatus.ACTIVE
        service.active_revision_id = revision.id
        service.status = ServiceStatus.ACTIVE
        await session.flush()

        with pytest.raises(NotFoundError, match="active service endpoint"):
            await repository.resolve_endpoint("legacy-service", "add")
        assert await repository.list_active_endpoints() == []

        assert await repository.stop_services_with_unsupported_interfaces() == [
            "legacy-service"
        ]
        assert service.status == ServiceStatus.STOPPED

        with pytest.raises(InvalidTransitionError, match="unsupported interface"):
            await repository.start_service(service.id)

    await engine.dispose()


async def test_delete_service_requires_stop_and_removes_owned_records() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        auto_create_schema=True,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        service = await repository.create_service(
            CreateServiceRequest(
                name="delete-service",
                git_url="https://example.invalid/delete.git",
            )
        )
        revision = await repository.create_revision(
            service.id, revision_request("delete-rev")
        )
        await repository.activate_revision(service.id, revision.id)

        with pytest.raises(InvalidTransitionError, match="stop it first"):
            await repository.delete_service(service.id)

        target = await repository.resolve_endpoint("delete-service", "add")
        invocation = await repository.begin_invocation(target, transport="REST")
        await repository.stop_service(service.id)
        with pytest.raises(InvalidTransitionError, match="active invocations"):
            await repository.delete_service(service.id)
        await repository.finish_invocation(
            invocation.id, InvocationStatus.SUCCEEDED
        )

        assert await repository.delete_service(service.id) == "delete-service"
        with pytest.raises(NotFoundError, match="service does not exist"):
            await repository.get_service(service.id)
        assert await session.scalar(select(func.count()).select_from(Revision)) == 0
        assert await session.scalar(select(func.count()).select_from(Endpoint)) == 0
        assert await session.scalar(select(func.count()).select_from(Invocation)) == 0

    await engine.dispose()


async def test_activate_rejects_grpc_route_owned_by_another_service() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        auto_create_schema=True,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)
    method_path = "/examples.shared.v1.SharedService/Echo"

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        first_service = await repository.create_service(
            CreateServiceRequest(
                name="first-service",
                git_url="https://example.invalid/first.git",
            )
        )
        second_service = await repository.create_service(
            CreateServiceRequest(
                name="second-service",
                git_url="https://example.invalid/second.git",
            )
        )
        first_contract = await repository.register_contract(
            first_service.id, published_contract(method_path)
        )
        second_method_path = "/examples.shared.v1.SharedService/Other"
        second_contract = await repository.register_contract(
            second_service.id, published_contract(second_method_path)
        )
        first_revision = await repository.create_revision(
            first_service.id,
            grpc_revision_request("first-rev", method_path),
            first_contract.id,
        )
        second_revision = await repository.create_revision(
            second_service.id,
            grpc_revision_request("second-rev", second_method_path),
            second_contract.id,
        )
        await repository.activate_revision(first_service.id, first_revision.id)

        with pytest.raises(
            InvalidTransitionError,
            match=(
                "gRPC service examples.shared.v1.SharedService is already "
                "published by service 'first-service'"
            ),
        ):
            await repository.activate_revision(
                second_service.id, second_revision.id
            )

        assert second_service.active_revision_id is None
        assert second_revision.status == RevisionStatus.READY

    await engine.dispose()


async def test_start_revalidates_grpc_route_namespace() -> None:
    settings = Settings(
        database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
        auto_create_schema=True,
    )
    engine = create_engine(settings)
    await create_schema(engine)
    factory = create_session_factory(engine)
    method_path = "/examples.shared.v1.SharedService/Echo"

    async with session_scope(factory) as session:
        repository = PlatformRepository(session)
        stopped_service = await repository.create_service(
            CreateServiceRequest(
                name="stopped-service",
                git_url="https://example.invalid/stopped.git",
            )
        )
        active_service = await repository.create_service(
            CreateServiceRequest(
                name="active-service",
                git_url="https://example.invalid/active.git",
            )
        )
        stopped_contract = await repository.register_contract(
            stopped_service.id, published_contract(method_path)
        )
        active_contract = await repository.register_contract(
            active_service.id, published_contract(method_path)
        )
        stopped_revision = await repository.create_revision(
            stopped_service.id,
            grpc_revision_request("stopped-rev", method_path),
            stopped_contract.id,
        )
        active_revision = await repository.create_revision(
            active_service.id,
            grpc_revision_request("active-rev", method_path),
            active_contract.id,
        )
        await repository.activate_revision(
            stopped_service.id, stopped_revision.id
        )
        await repository.stop_service(stopped_service.id)
        await repository.activate_revision(active_service.id, active_revision.id)

        with pytest.raises(
            InvalidTransitionError,
            match=(
                "gRPC service examples.shared.v1.SharedService is already "
                "published by service 'active-service'"
            ),
        ):
            await repository.start_service(stopped_service.id)

    await engine.dispose()
