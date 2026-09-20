from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable

import grpc
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pyscripts.config import Settings
from pyscripts.db import session_scope
from pyscripts.grpc_gateway.registry import GrpcRoute, GrpcRouteRegistry
from pyscripts.models import InvocationStatus
from pyscripts.repository import PlatformRepository
from pyscripts.runtime.directory import PoolOverloadedError, ProfilePoolScheduler
from pyscripts.runtime.output import ExecutionOutcome

logger = logging.getLogger(__name__)

GrpcDispatch = Callable[[GrpcRoute, bytes, grpc.aio.ServicerContext], Awaitable[bytes]]


class DynamicGrpcHandler(grpc.GenericRpcHandler):
    """One permanent handler that resolves native business RPC method paths."""

    def __init__(self, registry: GrpcRouteRegistry, dispatch: GrpcDispatch):
        self._registry = registry
        self._dispatch = dispatch

    def service(
        self, handler_call_details: grpc.HandlerCallDetails
    ) -> grpc.RpcMethodHandler | None:
        route = self._registry.resolve(handler_call_details.method)
        if route is None:
            return None

        # Capturing the immutable route here pins the selected revision for the
        # full lifetime of this RPC, even if the registry swaps immediately.
        async def invoke(
            payload: bytes,
            context: grpc.aio.ServicerContext,
            pinned_route: GrpcRoute = route,
        ) -> bytes:
            return await self._dispatch(pinned_route, payload, context)

        return grpc.unary_unary_rpc_method_handler(
            invoke,
            request_deserializer=None,
            response_serializer=None,
        )


class GrpcInvocationDispatcher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        profile_scheduler: ProfilePoolScheduler,
        timeout_seconds: float,
    ):
        self._session_factory = session_factory
        self._profile_scheduler = profile_scheduler
        self._timeout_seconds = timeout_seconds

    async def __call__(
        self,
        route: GrpcRoute,
        payload: bytes,
        context: grpc.aio.ServicerContext,
    ) -> bytes:
        target = route.target
        async with session_scope(self._session_factory) as session:
            invocation = await PlatformRepository(session).begin_invocation(target)
            request_id = invocation.id

        try:
            raw_result = await asyncio.wait_for(
                self._profile_scheduler.execute_grpc(target, request_id, payload),
                timeout=self._timeout_seconds,
            )
        except asyncio.CancelledError as error:
            await self._finish(request_id, InvocationStatus.CANCELLED, error)
            raise
        except PoolOverloadedError as error:
            await self._finish(request_id, InvocationStatus.FAILED, error)
            await context.abort(
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                "runtime profile has no available capacity",
            )
        except TimeoutError as error:
            await self._finish(request_id, InvocationStatus.TIMED_OUT, error)
            await context.abort(
                grpc.StatusCode.DEADLINE_EXCEEDED, "execution timed out"
            )
        except Exception as error:
            await self._finish(request_id, InvocationStatus.FAILED, error)
            logger.exception(
                "gRPC script invocation failed",
                extra={"request_id": str(request_id), "method": route.method_path},
            )
            await context.abort(grpc.StatusCode.INTERNAL, "script execution failed")

        outcome = (
            raw_result
            if isinstance(raw_result, ExecutionOutcome)
            else ExecutionOutcome(succeeded=True, value=raw_result)
        )
        if not outcome.succeeded:
            await self._finish(
                request_id,
                InvocationStatus.FAILED,
                outcome=outcome,
            )
            await context.abort(grpc.StatusCode.INTERNAL, "script execution failed")

        if not isinstance(outcome.value, bytes):
            error = TypeError("gRPC script returned a non-bytes result")
            await self._finish(request_id, InvocationStatus.FAILED, error)
            await context.abort(grpc.StatusCode.INTERNAL, "script execution failed")

        await self._finish(
            request_id,
            InvocationStatus.SUCCEEDED,
            outcome=outcome,
        )
        context.set_trailing_metadata(
            (
                ("pyscripts-request-id", str(request_id)),
                ("pyscripts-revision", target.revision),
            )
        )
        return outcome.value

    async def _finish(
        self,
        request_id: uuid.UUID,
        status: InvocationStatus,
        error: BaseException | None = None,
        outcome: ExecutionOutcome | None = None,
    ) -> None:
        error_text = str(error)[:4000] if error is not None else None
        if outcome is not None and not outcome.succeeded:
            error_text = ": ".join(
                part
                for part in (outcome.error_type, outcome.error_message)
                if part
            )[:4000]
        async with session_scope(self._session_factory) as session:
            await PlatformRepository(session).finish_invocation(
                request_id,
                status,
                error_text,
                logs=outcome.logs if outcome is not None else (),
                log_bytes=outcome.log_bytes if outcome is not None else 0,
                logs_truncated=(
                    outcome.logs_truncated if outcome is not None else False
                ),
            )


class GrpcGateway:
    def __init__(
        self,
        settings: Settings,
        registry: GrpcRouteRegistry,
        dispatcher: GrpcInvocationDispatcher,
    ):
        self._settings = settings
        self._server = grpc.aio.server(
            options=(
                (
                    "grpc.max_receive_message_length",
                    settings.grpc_max_receive_message_bytes,
                ),
                ("grpc.max_send_message_length", settings.grpc_max_send_message_bytes),
            )
        )
        self._handler = DynamicGrpcHandler(registry, dispatcher)
        self._server.add_generic_rpc_handlers((self._handler,))
        self.bound_port: int | None = None

    async def start(self) -> int:
        address = f"{self._settings.grpc_host}:{self._settings.grpc_port}"
        bound_port = self._server.add_insecure_port(address)
        if bound_port == 0:
            raise RuntimeError(f"could not bind gRPC server to {address}")
        await self._server.start()
        self.bound_port = bound_port
        logger.info("gRPC gateway listening on %s", address)
        return bound_port

    async def stop(self) -> None:
        await self._server.stop(self._settings.grpc_shutdown_grace_seconds)


async def refresh_routes_forever(
    registry: GrpcRouteRegistry,
    session_factory: async_sessionmaker[AsyncSession],
    interval_seconds: float,
) -> None:
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            await registry.refresh(session_factory)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to refresh dynamic gRPC routes")
