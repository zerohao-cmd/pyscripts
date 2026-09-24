from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import secrets
import urllib.parse
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import replace
from datetime import UTC
from pathlib import Path, PurePosixPath
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from pyscripts.config import Settings, get_settings
from pyscripts.contracts import ProtoContractPublisher
from pyscripts.contracts.builder import ContractBuildError
from pyscripts.db import (
    create_engine,
    create_schema,
    create_session_factory,
    session_scope,
)
from pyscripts.grpc_gateway.registry import GrpcRouteRegistry
from pyscripts.grpc_gateway.server import (
    GrpcGateway,
    GrpcInvocationDispatcher,
    refresh_routes_forever,
)
from pyscripts.interface_metadata import (
    InterfaceMetadataError,
    parse_artifact_interface,
)
from pyscripts.git_source import GitArtifactBuilder, GitSourceError
from pyscripts.models import InvocationStatus, RuntimeProfileStatus, ServiceStatus
from pyscripts.repository import (
    InvalidTransitionError,
    NotFoundError,
    PlatformRepository,
    RuntimeProfileRecord,
)
from pyscripts.runtime.directory import PoolOverloadedError, ProfilePoolScheduler
from pyscripts.runtime.output import ExecutionOutcome
from pyscripts.runtime.profiles import (
    RayRuntimeEnvironmentValidator,
    RuntimeEnvironmentValidator,
    RuntimeProfileCompatibilityError,
    RuntimeProfileValidationError,
    validate_project_compatibility,
)
from pyscripts.schemas import (
    ActorPoolStatusResponse,
    ContractResponse,
    CreateRevisionRequest,
    CreateRuntimeLabelRequest,
    CreateRuntimeProfileVersionRequest,
    CreateServiceRequest,
    EndpointSpec,
    GrpcContractSpec,
    InvocationListItemResponse,
    InvocationLogResponse,
    InvocationResponse,
    RevisionDetailResponse,
    RevisionInterfaceSpec,
    RevisionResponse,
    ResolvedRevisionRequest,
    RuntimeLabelResponse,
    RuntimeProfileVersionResponse,
    ServiceDetailResponse,
    ServiceResponse,
    UpdateServiceRequest,
    WorkerPoolResponse,
    WebhookConfigResponse,
)
from pyscripts.runtime.worker_pools import (
    UnknownWorkerPoolError,
    WorkerPoolCatalog,
    WorkerPoolDiscoveryError,
    create_worker_pool_catalog,
)
from pyscripts.storage import (
    ArtifactStore,
    ArtifactStoreError,
    create_artifact_store,
)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.session_factory() as session:
        yield session


SessionDependency = Annotated[AsyncSession, Depends(get_session)]
logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    runtime_profile_validator: RuntimeEnvironmentValidator | None = None,
    worker_pool_catalog: WorkerPoolCatalog | None = None,
    artifact_store: ArtifactStore | None = None,
) -> FastAPI:
    app_settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(app_settings)
        if app_settings.auto_create_schema:
            await create_schema(engine)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.artifact_store = artifact_store or create_artifact_store(
            app_settings
        )
        app.state.git_artifact_builder = GitArtifactBuilder()
        profile_scheduler = ProfilePoolScheduler(
            app_settings,
            artifact_store=app.state.artifact_store,
        )
        profile_scheduler.start()
        app.state.profile_scheduler = profile_scheduler
        app.state.runtime_profile_validator = (
            runtime_profile_validator or RayRuntimeEnvironmentValidator(app_settings)
        )
        app.state.worker_pool_catalog = (
            worker_pool_catalog or create_worker_pool_catalog(app_settings)
        )
        app.state.background_tasks = set()
        app.state.service_sync_locks = {}
        app.state.contract_publisher = ProtoContractPublisher(
            app_settings.contract_artifact_root
        )
        app.state.grpc_routes = GrpcRouteRegistry()
        await app.state.grpc_routes.refresh(app.state.session_factory)
        app.state.grpc_gateway = None
        app.state.grpc_route_refresh_task = None

        if app_settings.grpc_enabled:
            dispatcher = GrpcInvocationDispatcher(
                app.state.session_factory,
                app.state.profile_scheduler,
                app_settings.invocation_timeout_seconds,
            )
            app.state.grpc_gateway = GrpcGateway(
                app_settings,
                app.state.grpc_routes,
                dispatcher,
            )
            await app.state.grpc_gateway.start()
            app.state.grpc_route_refresh_task = asyncio.create_task(
                refresh_routes_forever(
                    app.state.grpc_routes,
                    app.state.session_factory,
                    app_settings.grpc_route_refresh_seconds,
                )
            )

        try:
            yield
        finally:
            background_tasks = list(app.state.background_tasks)
            for task in background_tasks:
                task.cancel()
            if background_tasks:
                await asyncio.gather(*background_tasks, return_exceptions=True)
            refresh_task = app.state.grpc_route_refresh_task
            if refresh_task is not None:
                refresh_task.cancel()
                with suppress(asyncio.CancelledError):
                    await refresh_task
            if app.state.grpc_gateway is not None:
                await app.state.grpc_gateway.stop()
            await profile_scheduler.close()
            await engine.dispose()

    app = FastAPI(title="pyscripts", version="0.1.0", lifespan=lifespan)
    app.state.settings = app_settings

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready(session: SessionDependency) -> dict[str, str]:
        await session.execute(text("SELECT 1"))
        return {"status": "ready"}

    def runtime_version_response(
        record: RuntimeProfileRecord,
    ) -> RuntimeProfileVersionResponse:
        version = record.version
        return RuntimeProfileVersionResponse(
            id=version.id,
            label_id=record.label.id,
            label_name=record.label.name,
            version=version.version,
            profile_ref=version.profile_ref,
            python_version=version.python_version,
            worker_pool=version.worker_pool,
            pip_source=version.pip_source,
            requested_dependencies=version.requested_dependencies,
            resolved_dependencies=version.resolved_dependencies,
            import_checks=version.import_checks,
            environment_digest=version.environment_digest,
            status=version.status.value,
            error=version.error,
            validation_result=version.validation_result,
            reference_count=record.reference_count,
            created_at=version.created_at,
            validated_at=version.validated_at,
            retired_at=version.retired_at,
        )

    async def validate_runtime_record(
        record: RuntimeProfileRecord,
        session: AsyncSession,
    ) -> RuntimeProfileRecord:
        version = record.version
        try:
            result = await app.state.runtime_profile_validator.validate(
                profile_ref=version.profile_ref,
                python_version=version.python_version,
                worker_pool=version.worker_pool,
                dependencies=version.requested_dependencies,
                import_checks=version.import_checks,
                pip_source=version.pip_source,
            )
        except RuntimeProfileValidationError as error:
            async with session.begin():
                await PlatformRepository(session).fail_runtime_profile_validation(
                    version.id, str(error)
                )
            return await PlatformRepository(session).get_runtime_profile(version.id)

        async with session.begin():
            validated = await PlatformRepository(
                session
            ).complete_runtime_profile_validation(
                version.id,
                resolved_dependencies=result.resolved_dependencies,
                validation_result=result.validation_result,
                runtime_env=result.runtime_env,
                environment_digest=result.environment_digest,
            )
            if validated.label.active_version_id is None:
                validated, _, _ = await PlatformRepository(
                    session
                ).activate_runtime_profile(validated.version.id)
        return validated

    async def create_and_validate_runtime(
        session: AsyncSession,
        worker_pool: str,
        creator: Any,
    ) -> RuntimeProfileVersionResponse:
        try:
            await app.state.worker_pool_catalog.require(worker_pool)
            async with session.begin():
                record = await creator(PlatformRepository(session))
            record = await validate_runtime_record(record, session)
        except UnknownWorkerPoolError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except WorkerPoolDiscoveryError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="runtime label or version already exists"
            ) from error
        return runtime_version_response(record)

    @app.get("/admin/worker-pools", response_model=list[WorkerPoolResponse])
    async def list_worker_pools() -> list[WorkerPoolResponse]:
        try:
            pools = await app.state.worker_pool_catalog.list()
        except WorkerPoolDiscoveryError as error:
            raise HTTPException(status_code=503, detail=str(error)) from error
        return [
            WorkerPoolResponse(
                name=pool.name,
                label_key=pool.label_key,
                node_count=pool.node_count,
                source=pool.source,
            )
            for pool in pools
        ]

    @app.get(
        "/admin/actor-pools",
        response_model=list[ActorPoolStatusResponse],
    )
    async def list_actor_pools(
        session: SessionDependency,
    ) -> list[ActorPoolStatusResponse]:
        # The scheduler learns profiles lazily from invocations. Registering
        # usable database versions here also makes never-invoked Cold pools
        # visible without creating an Actor for them.
        records = await PlatformRepository(session).list_runtime_profiles()
        for record in records:
            version = record.version
            if version.environment_digest is None or version.status not in {
                RuntimeProfileStatus.READY,
                RuntimeProfileStatus.ACTIVE,
                RuntimeProfileStatus.RETIRING,
            }:
                continue
            app.state.profile_scheduler.register_runtime_profile(
                environment_digest=version.environment_digest,
                profile_ref=version.profile_ref,
                worker_pool=version.worker_pool,
                runtime_env=dict(version.runtime_env or {}),
                pip_source=version.pip_source,
            )
        snapshots = await app.state.profile_scheduler.actor_pool_statuses()
        return [
            ActorPoolStatusResponse.model_validate(snapshot)
            for snapshot in snapshots
        ]

    @app.get("/admin/runtime-labels", response_model=list[RuntimeLabelResponse])
    async def list_runtime_labels(
        session: SessionDependency,
    ) -> list[RuntimeLabelResponse]:
        records = await PlatformRepository(session).list_runtime_profiles()
        grouped: dict[uuid.UUID, RuntimeLabelResponse] = {}
        for record in records:
            grouped.setdefault(
                record.label.id,
                RuntimeLabelResponse(
                    id=record.label.id,
                    name=record.label.name,
                    active_version_id=record.label.active_version_id,
                    created_at=record.label.created_at,
                    versions=[],
                ),
            ).versions.append(runtime_version_response(record))
        return list(grouped.values())

    @app.post(
        "/admin/runtime-labels",
        response_model=RuntimeProfileVersionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_runtime_label(
        body: CreateRuntimeLabelRequest,
        session: SessionDependency,
    ) -> RuntimeProfileVersionResponse:
        return await create_and_validate_runtime(
            session,
            body.worker_pool,
            lambda repository: repository.create_runtime_label(body),
        )

    @app.post(
        "/admin/runtime-labels/{label_id}/versions",
        response_model=RuntimeProfileVersionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_runtime_version(
        label_id: uuid.UUID,
        body: CreateRuntimeProfileVersionRequest,
        session: SessionDependency,
    ) -> RuntimeProfileVersionResponse:
        try:
            return await create_and_validate_runtime(
                session,
                body.worker_pool,
                lambda repository: repository.create_runtime_version(label_id, body),
            )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    async def drain_and_retire_profile(
        version_id: uuid.UUID,
        environment_digest: str,
    ) -> None:
        timeout = app_settings.runtime_profile_retirement_timeout_seconds
        drained = await app.state.profile_scheduler.retire_profile(
            environment_digest,
            timeout_seconds=timeout,
        )
        if not drained:
            return
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            async with app.state.session_factory() as execution_session:
                active = await PlatformRepository(
                    execution_session
                ).active_runtime_execution_count(environment_digest)
            if active == 0:
                break
            if asyncio.get_running_loop().time() >= deadline:
                return
            await asyncio.sleep(min(0.5, app_settings.actor_scheduler_poll_seconds * 10))
        async with session_scope(app.state.session_factory) as retire_session:
            try:
                await PlatformRepository(retire_session).retire_runtime_profile(
                    version_id, finalize=True
                )
            except (NotFoundError, InvalidTransitionError):
                return

    def schedule_profile_retirement(
        version_id: uuid.UUID,
        environment_digest: str,
    ) -> None:
        task = asyncio.create_task(
            drain_and_retire_profile(version_id, environment_digest)
        )
        app.state.background_tasks.add(task)
        task.add_done_callback(app.state.background_tasks.discard)

    @app.post(
        "/admin/runtime-profile-versions/{version_id}/activate",
        response_model=RuntimeProfileVersionResponse,
    )
    async def activate_runtime_version(
        version_id: uuid.UUID,
        session: SessionDependency,
    ) -> RuntimeProfileVersionResponse:
        try:
            async with session.begin():
                activated, old, old_references = await PlatformRepository(
                    session
                ).activate_runtime_profile(version_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        if (
            old is not None
            and old.id != activated.version.id
            and old_references == 0
            and old.environment_digest
        ):
            schedule_profile_retirement(old.id, old.environment_digest)
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return runtime_version_response(activated)

    @app.post(
        "/admin/runtime-profile-versions/{version_id}/retire",
        response_model=RuntimeProfileVersionResponse,
    )
    async def retire_runtime_version(
        version_id: uuid.UUID,
        session: SessionDependency,
    ) -> RuntimeProfileVersionResponse:
        try:
            async with session.begin():
                record = await PlatformRepository(session).retire_runtime_profile(
                    version_id
                )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        if record.version.environment_digest:
            schedule_profile_retirement(
                record.version.id, record.version.environment_digest
            )
        return runtime_version_response(record)

    @app.post(
        "/admin/services",
        response_model=ServiceResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_service(
        body: CreateServiceRequest,
        session: SessionDependency,
    ) -> ServiceResponse:
        try:
            async with session.begin():
                service = await PlatformRepository(session).create_service(body)
        except IntegrityError as error:
            raise HTTPException(
                status_code=409, detail="service name already exists"
            ) from error
        return ServiceResponse.model_validate(service)

    @app.get("/admin/services", response_model=list[ServiceResponse])
    async def list_services(
        session: SessionDependency,
    ) -> list[ServiceResponse]:
        services = await PlatformRepository(session).list_services()
        return [ServiceResponse.model_validate(service) for service in services]

    def revision_detail_response(revision: Any) -> RevisionDetailResponse:
        return RevisionDetailResponse(
            id=revision.id,
            service_id=revision.service_id,
            revision=revision.revision,
            artifact_uri=revision.artifact_uri,
            artifact_digest=revision.artifact_digest,
            runtime_profile=revision.runtime_profile,
            status=revision.status,
            endpoints=revision.manifest["endpoints"],
            created_at=revision.created_at,
            activated_at=revision.activated_at,
        )

    @app.get(
        "/admin/services/{service_id}",
        response_model=ServiceDetailResponse,
    )
    async def get_service_detail(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> ServiceDetailResponse:
        try:
            repository = PlatformRepository(session)
            service = await repository.get_service(service_id)
            revisions = await repository.list_revisions(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        active_revision = next(
            (
                revision
                for revision in revisions
                if revision.id == service.active_revision_id
            ),
            None,
        )
        return ServiceDetailResponse(
            **ServiceResponse.model_validate(service).model_dump(),
            revision_count=len(revisions),
            active_revision=(
                revision_detail_response(active_revision)
                if active_revision is not None
                else None
            ),
            endpoints=(
                list(active_revision.manifest["endpoints"])
                if active_revision is not None
                else []
            ),
        )

    @app.patch(
        "/admin/services/{service_id}",
        response_model=ServiceDetailResponse,
    )
    async def update_service(
        service_id: uuid.UUID,
        body: UpdateServiceRequest,
        session: SessionDependency,
    ) -> ServiceDetailResponse:
        try:
            async with session.begin():
                await PlatformRepository(session).update_service(service_id, body)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        return await get_service_detail(service_id, session)

    @app.get(
        "/admin/services/{service_id}/webhook",
        response_model=WebhookConfigResponse,
    )
    async def get_service_webhook(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> WebhookConfigResponse:
        try:
            service = await PlatformRepository(session).get_service(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return WebhookConfigResponse(
            enabled=service.webhook_enabled,
            configured_at=service.webhook_configured_at,
        )

    @app.post(
        "/admin/services/{service_id}/webhook/rotate",
        response_model=WebhookConfigResponse,
    )
    async def rotate_service_webhook(
        request: Request,
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> WebhookConfigResponse:
        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        try:
            async with session.begin():
                service = await PlatformRepository(
                    session
                ).configure_service_webhook(service_id, token_hash)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        base_url = (app_settings.public_base_url or str(request.base_url)).rstrip("/")
        return WebhookConfigResponse(
            enabled=True,
            url=f"{base_url}/hooks/services/{service.id}/{token}",
            configured_at=service.webhook_configured_at,
        )

    @app.delete(
        "/admin/services/{service_id}/webhook",
        response_model=WebhookConfigResponse,
    )
    async def disable_service_webhook(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> WebhookConfigResponse:
        try:
            async with session.begin():
                await PlatformRepository(session).configure_service_webhook(
                    service_id, None
                )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return WebhookConfigResponse(enabled=False)

    @app.get(
        "/admin/services/{service_id}/revisions",
        response_model=list[RevisionDetailResponse],
    )
    async def list_revisions(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> list[RevisionDetailResponse]:
        try:
            revisions = await PlatformRepository(session).list_revisions(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return [revision_detail_response(revision) for revision in revisions]

    def invocation_response(record: Any) -> InvocationListItemResponse:
        invocation = record.invocation
        return InvocationListItemResponse(
            id=invocation.id,
            service_id=invocation.service_id,
            service=record.service_name,
            revision_id=invocation.revision_id,
            revision=record.revision,
            endpoint_id=invocation.endpoint_id,
            transport=invocation.transport,
            status=invocation.status,
            error=invocation.error,
            created_at=invocation.created_at,
            started_at=invocation.started_at,
            finished_at=invocation.finished_at,
            execution_kind=(
                record.execution.execution_kind.value if record.execution else None
            ),
            runtime_profile=(
                record.execution.runtime_profile_ref if record.execution else None
            ),
            environment_digest=(
                record.execution.environment_digest if record.execution else None
            ),
            has_logs=bool(record.execution and record.execution.log_bytes),
            log_bytes=(record.execution.log_bytes if record.execution else 0),
            logs_truncated=(
                record.execution.logs_truncated if record.execution else False
            ),
        )

    @app.get(
        "/admin/invocations",
        response_model=list[InvocationListItemResponse],
    )
    async def list_invocations(
        session: SessionDependency,
        limit: int = 100,
    ) -> list[InvocationListItemResponse]:
        records = await PlatformRepository(session).list_invocations(
            limit=max(1, min(limit, 500))
        )
        return [invocation_response(record) for record in records]

    @app.get(
        "/admin/services/{service_id}/invocations",
        response_model=list[InvocationListItemResponse],
    )
    async def list_service_invocations(
        service_id: uuid.UUID,
        session: SessionDependency,
        limit: int = 100,
    ) -> list[InvocationListItemResponse]:
        try:
            records = await PlatformRepository(session).list_invocations(
                service_id,
                max(1, min(limit, 500)),
            )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return [invocation_response(record) for record in records]

    @app.get(
        "/admin/invocations/{invocation_id}/logs",
        response_model=list[InvocationLogResponse],
    )
    async def list_invocation_logs(
        invocation_id: uuid.UUID,
        session: SessionDependency,
        after_sequence: int = -1,
        limit: int = 500,
    ) -> list[InvocationLogResponse]:
        try:
            logs = await PlatformRepository(session).list_invocation_logs(
                invocation_id,
                after_sequence=after_sequence,
                limit=max(1, min(limit, 2000)),
            )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return [
            InvocationLogResponse(
                sequence=log.sequence,
                stream=log.stream,
                content=log.content,
                emitted_at=(
                    log.emitted_at
                    if log.emitted_at.tzinfo is not None
                    else log.emitted_at.replace(tzinfo=UTC)
                ),
                created_at=log.created_at,
            )
            for log in logs
        ]

    async def resolve_revision_profile(
        repository: PlatformRepository,
        interface: RevisionInterfaceSpec,
    ) -> RuntimeProfileRecord | None:
        try:
            profile = await repository.get_runtime_profile(
                interface.runtime_profile,
                require_usable=True,
            )
        except NotFoundError as error:
            if app_settings.require_registered_runtime_profiles:
                raise RuntimeProfileCompatibilityError(
                    f"runtime environment {interface.runtime_profile} does not exist"
                ) from error
            return None
        if profile.version.status != RuntimeProfileStatus.ACTIVE:
            raise RuntimeProfileCompatibilityError(
                f"runtime environment {interface.runtime_profile} is not ACTIVE"
            )
        validate_project_compatibility(
            profile,
            requires_python=interface.requires_python,
            dependencies=interface.dependencies,
        )
        return profile

    async def persist_resolved_revision(
        *,
        service_id: uuid.UUID,
        service_name: str,
        source_body: ResolvedRevisionRequest,
        runtime_profile: RuntimeProfileRecord | None,
        session: AsyncSession,
    ) -> Revision:
        async with session.begin():
            previous_contract = await PlatformRepository(session).get_latest_contract(
                service_id
            )
        previous_descriptor = None
        previous_version = None
        if previous_contract is not None:
            previous_uri = app.state.artifact_store.distribution_uri(
                previous_contract.descriptor_uri
            )
            previous_descriptor = await asyncio.to_thread(
                app.state.contract_publisher.read_bytes, previous_uri
            )
            previous_version = previous_contract.contract_version

        prepared = await asyncio.to_thread(
            app.state.contract_publisher.prepare_generated_artifact,
            service_name,
            source_body,
            previous_descriptor=previous_descriptor,
            previous_version=previous_version,
        )
        if prepared is not None:
            source_body = source_body.model_copy(
                update={
                    "artifact_uri": prepared.artifact_uri,
                    "artifact_digest": prepared.artifact_digest,
                    "endpoints": prepared.endpoints,
                    "grpc_contract": prepared.contract,
                }
            )
        published = await asyncio.to_thread(
            app.state.contract_publisher.publish,
            service_name,
            source_body,
        )
        if published is not None:
            descriptor_uri = await asyncio.to_thread(
                app.state.artifact_store.publish_blob,
                published.descriptor_uri,
                published.schema_digest,
                category="contracts/descriptors",
                suffix=".pb",
                content_type="application/octet-stream",
            )
            proto_bundle_uri = await asyncio.to_thread(
                app.state.artifact_store.publish_blob,
                published.proto_bundle_uri,
                published.proto_bundle_digest,
                category="contracts/proto",
                suffix=".zip",
                content_type="application/zip",
            )
            published = replace(
                published,
                descriptor_uri=descriptor_uri,
                proto_bundle_uri=proto_bundle_uri,
            )
        stored_uri = await asyncio.to_thread(
            app.state.artifact_store.publish,
            source_body.artifact_uri,
            source_body.artifact_digest,
        )
        stored_body = source_body.model_copy(update={"artifact_uri": stored_uri})
        async with session.begin():
            repository = PlatformRepository(session)
            contract_id = None
            if published is not None:
                contract = await repository.register_contract(service_id, published)
                contract_id = contract.id
            revision = await repository.create_revision(
                service_id,
                stored_body,
                contract_id,
                runtime_profile,
            )
            service = await repository.get_service(service_id)
            if service.status != ServiceStatus.STOPPED:
                revision = await repository.activate_revision(
                    service_id,
                    revision.id,
                )
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return revision

    async def sync_git_revision_unlocked(
        service_id: uuid.UUID,
        session: AsyncSession,
    ) -> RevisionResponse:
        artifact = None
        try:
            async with session.begin():
                service = await PlatformRepository(session).get_service(service_id)
                service_name = service.name
                git_url = service.git_url
                git_branch = service.git_branch
            artifact = await asyncio.to_thread(
                app.state.git_artifact_builder.build,
                git_url,
                git_branch,
            )
            interface = await asyncio.to_thread(
                parse_artifact_interface,
                artifact.uri,
                artifact.digest,
            )
            async with session.begin():
                repository = PlatformRepository(session)
                existing = await repository.find_revision(
                    service_id,
                    artifact.revision,
                )
                if existing is not None:
                    return RevisionResponse.model_validate(existing)
                runtime_profile = await resolve_revision_profile(
                    repository,
                    interface,
                )
            source_body = ResolvedRevisionRequest(
                revision=artifact.revision,
                artifact_uri=artifact.uri,
                artifact_digest=artifact.digest,
                endpoints=interface.endpoints,
                grpc_contract=interface.grpc_contract,
                runtime_profile=interface.runtime_profile,
                requires_python=interface.requires_python,
                dependencies=interface.dependencies,
            )
            revision = await persist_resolved_revision(
                service_id=service_id,
                service_name=service_name,
                source_body=source_body,
                runtime_profile=runtime_profile,
                session=session,
            )
        except (GitSourceError, InterfaceMetadataError) as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except RuntimeProfileCompatibilityError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ContractBuildError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ArtifactStoreError as error:
            raise HTTPException(status_code=502, detail=str(error)) from error
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except IntegrityError as error:
            raise HTTPException(
                status_code=409,
                detail="revision or contract version already exists",
            ) from error
        finally:
            if artifact is not None:
                artifact.cleanup()
        return RevisionResponse.model_validate(revision)

    async def synchronize_git_revision(
        service_id: uuid.UUID,
        session: AsyncSession,
    ) -> RevisionResponse:
        lock = app.state.service_sync_locks.setdefault(service_id, asyncio.Lock())
        async with lock:
            return await sync_git_revision_unlocked(service_id, session)

    @app.post(
        "/admin/services/{service_id}/revisions",
        response_model=RevisionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def sync_git_revision(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> RevisionResponse:
        return await synchronize_git_revision(service_id, session)

    async def run_webhook_sync(service_id: uuid.UUID) -> None:
        async with app.state.session_factory() as sync_session:
            try:
                await synchronize_git_revision(service_id, sync_session)
            except Exception:
                logger.exception(
                    "webhook-triggered service synchronization failed",
                    extra={"service_id": str(service_id)},
                )

    @app.post(
        "/hooks/services/{service_id}/{token}",
        status_code=status.HTTP_202_ACCEPTED,
        include_in_schema=False,
    )
    async def receive_git_webhook(
        request: Request,
        service_id: uuid.UUID,
        token: str,
        session: SessionDependency,
    ) -> dict[str, str]:
        try:
            service = await PlatformRepository(session).get_service(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail="webhook does not exist") from error
        supplied_hash = hashlib.sha256(token.encode()).hexdigest()
        if (
            service.tracking_mode != "webhook"
            or service.webhook_token_hash is None
            or not hmac.compare_digest(service.webhook_token_hash, supplied_hash)
        ):
            raise HTTPException(status_code=404, detail="webhook does not exist")

        declared_size = request.headers.get("content-length")
        if declared_size:
            try:
                content_length = int(declared_size)
            except ValueError as error:
                raise HTTPException(
                    status_code=400, detail="invalid Content-Length header"
                ) from error
            if content_length < 0:
                raise HTTPException(
                    status_code=400, detail="invalid Content-Length header"
                )
            if content_length > app_settings.webhook_max_body_bytes:
                raise HTTPException(
                    status_code=413, detail="webhook payload is too large"
                )
        received = 0
        payload_bytes = bytearray()
        async for chunk in request.stream():
            received += len(chunk)
            if received > app_settings.webhook_max_body_bytes:
                raise HTTPException(status_code=413, detail="webhook payload is too large")
            payload_bytes.extend(chunk)

        gitea_event = request.headers.get("x-gitea-event")
        gitlab_event = request.headers.get("x-gitlab-event")
        if gitea_event is not None:
            provider = "gitea"
            is_push = gitea_event.lower() == "push"
        elif gitlab_event is not None:
            provider = "gitlab"
            is_push = gitlab_event.lower() == "push hook"
        else:
            raise HTTPException(
                status_code=400,
                detail="expected X-Gitea-Event or X-Gitlab-Event header",
            )
        if not is_push:
            return {"status": "ignored", "provider": provider}

        if service.git_branch is not None:
            try:
                payload = json.loads(payload_bytes)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise HTTPException(
                    status_code=400,
                    detail="webhook payload must be valid JSON",
                ) from error
            pushed_ref = payload.get("ref") if isinstance(payload, dict) else None
            if pushed_ref != f"refs/heads/{service.git_branch}":
                return {"status": "ignored", "provider": provider}

        task = asyncio.create_task(run_webhook_sync(service_id))
        app.state.background_tasks.add(task)
        task.add_done_callback(app.state.background_tasks.discard)
        return {"status": "accepted", "provider": provider}

    @app.post(
        "/admin/services/{service_id}/revisions/import",
        response_model=RevisionResponse,
        status_code=status.HTTP_201_CREATED,
        include_in_schema=False,
    )
    async def import_revision_artifact(
        service_id: uuid.UUID,
        body: CreateRevisionRequest,
        session: SessionDependency,
    ) -> RevisionResponse:
        try:
            source_uri = app.state.artifact_store.distribution_uri(body.artifact_uri)
            async with session.begin():
                service = await PlatformRepository(session).get_service(service_id)
                service_name = service.name
            interface = await asyncio.to_thread(
                parse_artifact_interface,
                source_uri,
                body.artifact_digest,
            )
            async with session.begin():
                runtime_profile = await resolve_revision_profile(
                    PlatformRepository(session),
                    interface,
                )
            source_body = ResolvedRevisionRequest(
                **body.model_dump(exclude={"artifact_uri"}),
                artifact_uri=source_uri,
                endpoints=interface.endpoints,
                grpc_contract=interface.grpc_contract,
                runtime_profile=interface.runtime_profile,
                requires_python=interface.requires_python,
                dependencies=interface.dependencies,
            )
            revision = await persist_resolved_revision(
                service_id=service_id,
                service_name=service_name,
                source_body=source_body,
                runtime_profile=runtime_profile,
                session=session,
            )
        except (InterfaceMetadataError, RuntimeProfileCompatibilityError) as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ContractBuildError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except ArtifactStoreError as error:
            raise HTTPException(status_code=502, detail=str(error)) from error
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except IntegrityError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return RevisionResponse.model_validate(revision)

    @app.post(
        "/admin/services/{service_id}/revisions/{revision_id}/activate",
        response_model=RevisionResponse,
    )
    async def activate_revision(
        service_id: uuid.UUID,
        revision_id: uuid.UUID,
        session: SessionDependency,
    ) -> RevisionResponse:
        try:
            async with session.begin():
                revision = await PlatformRepository(session).activate_revision(
                    service_id, revision_id
                )
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return RevisionResponse.model_validate(revision)

    @app.post("/admin/services/{service_id}/stop", response_model=ServiceResponse)
    async def stop_service(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> ServiceResponse:
        try:
            async with session.begin():
                service = await PlatformRepository(session).stop_service(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return ServiceResponse.model_validate(service)

    @app.post("/admin/services/{service_id}/start", response_model=ServiceResponse)
    async def start_service(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> ServiceResponse:
        try:
            async with session.begin():
                service = await PlatformRepository(session).start_service(service_id)
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except InvalidTransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return ServiceResponse.model_validate(service)

    async def ensure_contract_artifacts(
        bundle: Any,
        session: AsyncSession,
    ) -> None:
        contract = bundle.contract
        descriptor_scheme = urllib.parse.urlparse(contract.descriptor_uri).scheme
        proto_scheme = urllib.parse.urlparse(contract.proto_bundle_uri).scheme
        if (
            descriptor_scheme == "s3"
            and proto_scheme == "s3"
            and contract.proto_bundle_digest
        ):
            return
        if (
            contract.proto_bundle_digest
            and descriptor_scheme in {"", "file"}
            and proto_scheme in {"", "file"}
        ):
            descriptor_path = Path(
                urllib.parse.unquote(
                    urllib.parse.urlparse(contract.descriptor_uri).path
                    or contract.descriptor_uri
                )
            )
            proto_path = Path(
                urllib.parse.unquote(
                    urllib.parse.urlparse(contract.proto_bundle_uri).path
                    or contract.proto_bundle_uri
                )
            )
            if descriptor_path.is_file() and proto_path.is_file():
                return

        published = None
        try:
            descriptor_path = app.state.contract_publisher.resolve_local_uri(
                contract.descriptor_uri
            )
            proto_path = app.state.contract_publisher.resolve_local_uri(
                contract.proto_bundle_uri
            )
            descriptor_uri = descriptor_path.as_uri()
            proto_bundle_uri = proto_path.as_uri()
            proto_bundle_digest = "sha256:" + hashlib.sha256(
                proto_path.read_bytes()
            ).hexdigest()
        except ContractBuildError:
            revision = bundle.revision
            if revision is None:
                raise ContractBuildError(
                    "contract artifacts are unavailable and no source revision exists"
                )
            source_uri = app.state.artifact_store.distribution_uri(
                revision.artifact_uri
            )
            interface = await asyncio.to_thread(
                parse_artifact_interface,
                source_uri,
                revision.artifact_digest,
            )
            endpoints = [
                EndpointSpec.from_manifest(dict(item))
                for item in revision.manifest["endpoints"]
            ]
            contract_metadata = revision.manifest.get("grpc_contract")
            if contract_metadata is not None:
                grpc_contract = GrpcContractSpec.model_validate(contract_metadata)
            elif interface.grpc_contract is not None:
                grpc_contract = interface.grpc_contract
            else:
                grpc_endpoints = [
                    endpoint for endpoint in endpoints if endpoint.grpc is not None
                ]
                if not grpc_endpoints or not all(
                    endpoint.grpc.generated for endpoint in grpc_endpoints
                ):
                    raise ContractBuildError(
                        "contract metadata is unavailable for artifact recovery"
                    )
                descriptor_paths = {
                    endpoint.grpc.descriptor_path for endpoint in grpc_endpoints
                }
                if len(descriptor_paths) != 1:
                    raise ContractBuildError(
                        "generated endpoints do not share one descriptor"
                    )
                descriptor_path = PurePosixPath(descriptor_paths.pop())
                grpc_contract = GrpcContractSpec(
                    version=contract.contract_version,
                    proto_root=str(descriptor_path.parent / "proto"),
                )
            grpc_contract = grpc_contract.model_copy(
                update={"version": contract.contract_version}
            )
            request = ResolvedRevisionRequest(
                revision=revision.revision,
                artifact_uri=source_uri,
                artifact_digest=revision.artifact_digest,
                endpoints=endpoints,
                grpc_contract=grpc_contract,
                runtime_profile=revision.runtime_profile,
                requires_python=interface.requires_python,
                dependencies=interface.dependencies,
            )
            published = await asyncio.to_thread(
                app.state.contract_publisher.publish,
                bundle.service.name,
                request,
            )
            if published is None:
                raise ContractBuildError("source revision did not produce a contract")
            expected = (
                contract.schema_digest,
                contract.source_digest,
                contract.contract_version,
                sorted(contract.methods),
            )
            actual = (
                published.schema_digest,
                published.source_digest,
                published.contract_version,
                sorted(published.methods),
            )
            if actual != expected:
                raise ContractBuildError(
                    "regenerated contract does not match persisted metadata"
                )
            descriptor_uri = published.descriptor_uri
            proto_bundle_uri = published.proto_bundle_uri
            proto_bundle_digest = published.proto_bundle_digest

        stored_descriptor_uri = await asyncio.to_thread(
            app.state.artifact_store.publish_blob,
            descriptor_uri,
            contract.schema_digest,
            category="contracts/descriptors",
            suffix=".pb",
            content_type="application/octet-stream",
        )
        stored_proto_uri = await asyncio.to_thread(
            app.state.artifact_store.publish_blob,
            proto_bundle_uri,
            proto_bundle_digest,
            category="contracts/proto",
            suffix=".zip",
            content_type="application/zip",
        )
        contract.descriptor_uri = stored_descriptor_uri
        contract.proto_bundle_uri = stored_proto_uri
        contract.proto_bundle_digest = proto_bundle_digest
        await session.commit()
        logger.info(
            "persisted contract artifacts in object storage",
            extra={"contract_id": str(contract.id), "regenerated": published is not None},
        )

    def contract_response(
        request: Request,
        bundle: Any,
    ) -> ContractResponse:
        contract = bundle.contract
        assert contract.proto_bundle_digest is not None
        return ContractResponse(
            id=contract.id,
            service=bundle.service.name,
            revision=bundle.revision.revision if bundle.revision else None,
            contract_version=contract.contract_version,
            schema_digest=contract.schema_digest,
            source_digest=contract.source_digest,
            proto_bundle_digest=contract.proto_bundle_digest,
            methods=contract.methods,
            proto_bundle_url=str(
                request.url_for("download_contract_proto", contract_id=contract.id)
            ),
        )

    @app.get(
        "/v1/services/{service_name}/grpc-contract",
        response_model=ContractResponse,
    )
    async def get_active_grpc_contract(
        request: Request,
        service_name: str,
        session: SessionDependency,
    ) -> ContractResponse:
        try:
            bundle = await PlatformRepository(session).get_active_contract(service_name)
            await ensure_contract_artifacts(bundle, session)
        except (
            NotFoundError,
            ContractBuildError,
            InterfaceMetadataError,
            ArtifactStoreError,
        ) as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return contract_response(request, bundle)

    def contract_download_response(
        stored_uri: str,
        *,
        media_type: str,
        filename: str,
    ) -> Any:
        uri = app.state.artifact_store.distribution_uri(stored_uri)
        parsed = urllib.parse.urlparse(uri)
        if parsed.scheme in {"http", "https"}:
            return RedirectResponse(uri, status_code=307)
        if parsed.scheme in {"", "file"}:
            path = Path(urllib.parse.unquote(parsed.path or uri)).resolve()
            if not path.is_file():
                raise HTTPException(status_code=404, detail="contract file is missing")
            return FileResponse(path, media_type=media_type, filename=filename)
        raise HTTPException(status_code=502, detail="contract storage URI is invalid")

    @app.get(
        "/v1/contracts/{contract_id}/proto.zip",
        name="download_contract_proto",
    )
    async def download_contract_proto(
        contract_id: uuid.UUID,
        session: SessionDependency,
    ) -> Any:
        try:
            bundle = await PlatformRepository(session).get_contract(contract_id)
            await ensure_contract_artifacts(bundle, session)
        except (
            NotFoundError,
            ContractBuildError,
            InterfaceMetadataError,
            ArtifactStoreError,
        ) as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return contract_download_response(
            bundle.contract.proto_bundle_uri,
            media_type="application/zip",
            filename="proto.zip",
        )

    @app.post(
        "/v1/services/{service_name}/{endpoint_id}",
        response_model=InvocationResponse,
    )
    async def invoke(
        request: Request,
        service_name: str,
        endpoint_id: str,
        params: dict[str, Any],
    ) -> InvocationResponse:
        factory = request.app.state.session_factory
        try:
            async with session_scope(factory) as session:
                repository = PlatformRepository(session)
                target = await repository.resolve_endpoint(service_name, endpoint_id)
                if "rest" not in target.io_type:
                    raise NotFoundError("active service REST endpoint does not exist")
                invocation = await repository.begin_invocation(
                    target,
                    transport="REST",
                )
                request_id = invocation.id
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

        try:
            raw_result = await asyncio.wait_for(
                request.app.state.profile_scheduler.execute(
                    target, request_id, params
                ),
                timeout=app_settings.invocation_timeout_seconds,
            )
        except PoolOverloadedError as error:
            async with session_scope(factory) as session:
                await PlatformRepository(session).finish_invocation(
                    request_id, InvocationStatus.FAILED, str(error)[:4000]
                )
            raise HTTPException(
                status_code=503, detail="runtime profile has no available capacity"
            ) from error
        except TimeoutError as error:
            async with session_scope(factory) as session:
                await PlatformRepository(session).finish_invocation(
                    request_id, InvocationStatus.TIMED_OUT, "execution timed out"
                )
            raise HTTPException(
                status_code=504, detail="execution timed out"
            ) from error
        except Exception as error:
            async with session_scope(factory) as session:
                await PlatformRepository(session).finish_invocation(
                    request_id, InvocationStatus.FAILED, str(error)[:4000]
                )
            raise HTTPException(
                status_code=502, detail="script execution failed"
            ) from error

        outcome = (
            raw_result
            if isinstance(raw_result, ExecutionOutcome)
            else ExecutionOutcome(succeeded=True, value=raw_result)
        )
        if not outcome.succeeded:
            error_text = ": ".join(
                part
                for part in (outcome.error_type, outcome.error_message)
                if part
            )[:4000]
            async with session_scope(factory) as session:
                await PlatformRepository(session).finish_invocation(
                    request_id,
                    InvocationStatus.FAILED,
                    error_text or "script execution failed",
                    logs=outcome.logs,
                    log_bytes=outcome.log_bytes,
                    logs_truncated=outcome.logs_truncated,
                )
            raise HTTPException(status_code=502, detail="script execution failed")

        async with session_scope(factory) as session:
            await PlatformRepository(session).finish_invocation(
                request_id,
                InvocationStatus.SUCCEEDED,
                logs=outcome.logs,
                log_bytes=outcome.log_bytes,
                logs_truncated=outcome.logs_truncated,
            )
        return InvocationResponse(
            request_id=request_id,
            service=target.service_name,
            revision=target.revision,
            result=outcome.value,
        )

    ui_dist = app_settings.ui_dist_path.resolve()
    if app_settings.serve_ui and ui_dist.is_dir():
        app.mount("/", StaticFiles(directory=ui_dist, html=True), name="ui")

    return app


app = create_app()
