from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from pyscripts.config import Settings, get_settings
from pyscripts.contracts import PythonSdkPublisher
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
from pyscripts.runtime.profiles import (
    RayRuntimeEnvironmentValidator,
    RuntimeEnvironmentValidator,
    RuntimeProfileCompatibilityError,
    RuntimeProfileValidationError,
    validate_project_compatibility,
)
from pyscripts.schemas import (
    ContractResponse,
    CreateRevisionRequest,
    CreateRuntimeLabelRequest,
    CreateRuntimeProfileVersionRequest,
    CreateServiceRequest,
    InvocationListItemResponse,
    InvocationResponse,
    RevisionDetailResponse,
    RevisionInterfaceSpec,
    RevisionResponse,
    ResolvedRevisionRequest,
    RuntimeLabelResponse,
    RuntimeProfileVersionResponse,
    SdkArtifactResponse,
    ServiceDetailResponse,
    ServiceResponse,
    UpdateServiceRequest,
    WorkerPoolResponse,
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
        app.state.profile_scheduler = ProfilePoolScheduler(
            app_settings,
            artifact_store=app.state.artifact_store,
        )
        app.state.runtime_profile_validator = (
            runtime_profile_validator or RayRuntimeEnvironmentValidator(app_settings)
        )
        app.state.worker_pool_catalog = (
            worker_pool_catalog or create_worker_pool_catalog(app_settings)
        )
        app.state.background_tasks = set()
        app.state.sdk_publisher = PythonSdkPublisher(
            app_settings.contract_artifact_root,
            app_settings.sdk_distribution_prefix,
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
        published = await asyncio.to_thread(
            app.state.sdk_publisher.publish,
            service_name,
            source_body,
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
                contract, _ = await repository.register_contract(
                    service_id, published
                )
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

    @app.post(
        "/admin/services/{service_id}/revisions",
        response_model=RevisionResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def sync_git_revision(
        service_id: uuid.UUID,
        session: SessionDependency,
    ) -> RevisionResponse:
        artifact = None
        try:
            async with session.begin():
                service = await PlatformRepository(session).get_service(service_id)
                service_name = service.name
                git_url = service.git_url
            artifact = await asyncio.to_thread(
                app.state.git_artifact_builder.build,
                git_url,
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
        await app.state.grpc_routes.refresh(app.state.session_factory)
        return ServiceResponse.model_validate(service)

    def contract_response(
        request: Request,
        bundle: Any,
    ) -> ContractResponse:
        contract = bundle.contract
        sdk = bundle.sdk
        return ContractResponse(
            id=contract.id,
            service=bundle.service.name,
            revision=bundle.revision.revision if bundle.revision else None,
            contract_version=contract.contract_version,
            schema_digest=contract.schema_digest,
            source_digest=contract.source_digest,
            methods=contract.methods,
            descriptor_url=str(
                request.url_for("download_contract_descriptor", contract_id=contract.id)
            ),
            proto_bundle_url=str(
                request.url_for("download_contract_proto", contract_id=contract.id)
            ),
            python_sdk=SdkArtifactResponse(
                language=sdk.language,
                generator_version=sdk.generator_version,
                package_name=sdk.package_name,
                package_version=sdk.package_version,
                artifact_digest=sdk.artifact_digest,
                download_url=str(
                    request.url_for("download_python_sdk", contract_id=contract.id)
                ),
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
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return contract_response(request, bundle)

    @app.get(
        "/v1/contracts/{contract_id}/descriptor.pb",
        name="download_contract_descriptor",
    )
    async def download_contract_descriptor(
        contract_id: uuid.UUID,
        session: SessionDependency,
    ) -> FileResponse:
        try:
            bundle = await PlatformRepository(session).get_contract(contract_id)
            path = app.state.sdk_publisher.resolve_local_uri(
                bundle.contract.descriptor_uri
            )
        except (NotFoundError, ContractBuildError) as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return FileResponse(
            path,
            media_type="application/octet-stream",
            filename="descriptor.pb",
        )

    @app.get(
        "/v1/contracts/{contract_id}/proto.zip",
        name="download_contract_proto",
    )
    async def download_contract_proto(
        contract_id: uuid.UUID,
        session: SessionDependency,
    ) -> FileResponse:
        try:
            bundle = await PlatformRepository(session).get_contract(contract_id)
            path = app.state.sdk_publisher.resolve_local_uri(
                bundle.contract.proto_bundle_uri
            )
        except (NotFoundError, ContractBuildError) as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return FileResponse(path, media_type="application/zip", filename="proto.zip")

    @app.get(
        "/v1/contracts/{contract_id}/python-sdk",
        name="download_python_sdk",
    )
    async def download_python_sdk(
        contract_id: uuid.UUID,
        session: SessionDependency,
    ) -> FileResponse:
        try:
            bundle = await PlatformRepository(session).get_contract(contract_id)
            path = app.state.sdk_publisher.resolve_local_uri(bundle.sdk.artifact_uri)
        except (NotFoundError, ContractBuildError) as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        return FileResponse(
            path,
            media_type="application/zip",
            filename=path.name,
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
                invocation = await repository.begin_invocation(target)
                request_id = invocation.id
        except NotFoundError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

        try:
            result = await asyncio.wait_for(
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

        async with session_scope(factory) as session:
            await PlatformRepository(session).finish_invocation(
                request_id, InvocationStatus.SUCCEEDED
            )
        return InvocationResponse(
            request_id=request_id,
            service=target.service_name,
            revision=target.revision,
            result=result,
        )

    ui_dist = app_settings.ui_dist_path.resolve()
    if app_settings.serve_ui and ui_dist.is_dir():
        app.mount("/", StaticFiles(directory=ui_dist, html=True), name="ui")

    return app


app = create_app()
