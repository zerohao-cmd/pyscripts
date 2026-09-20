from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pyscripts.repository import PlatformRepository, ResolvedEndpoint


class DuplicateGrpcRouteError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class GrpcRoute:
    method_path: str
    target: ResolvedEndpoint


class GrpcRouteRegistry:
    """An atomically replaceable routing snapshot read by GenericRpcHandler."""

    def __init__(self) -> None:
        self._routes: Mapping[str, GrpcRoute] = MappingProxyType({})
        self._generation = 0

    @property
    def generation(self) -> int:
        return self._generation

    def resolve(self, method_path: str) -> GrpcRoute | None:
        return self._routes.get(method_path)

    def snapshot(self) -> Mapping[str, GrpcRoute]:
        return self._routes

    def replace(self, targets: list[ResolvedEndpoint]) -> int:
        routes: dict[str, GrpcRoute] = {}
        for target in targets:
            if target.grpc is None:
                continue
            method_path = f"/{target.grpc['service']}/{target.grpc['method']}"
            if method_path in routes:
                first = routes[method_path].target
                raise DuplicateGrpcRouteError(
                    f"gRPC method {method_path} is published by both "
                    f"{first.service_name!r} and {target.service_name!r}"
                )
            routes[method_path] = GrpcRoute(method_path, target)

        self._routes = MappingProxyType(routes)
        self._generation += 1
        return self._generation

    async def refresh(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> int:
        async with session_factory() as session:
            targets = await PlatformRepository(session).list_active_endpoints()
        return self.replace(targets)
