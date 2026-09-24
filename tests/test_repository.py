from __future__ import annotations

from dataclasses import replace

from pydantic import SecretStr
from sqlalchemy import select

from pyscripts.config import Settings
from pyscripts.db import (
    create_engine,
    create_schema,
    create_session_factory,
    session_scope,
)
from pyscripts.models import (
    ExecutionKind,
    InvocationExecution,
    InvocationStatus,
    Revision,
    RevisionStatus,
)
from pyscripts.repository import PlatformRepository
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
