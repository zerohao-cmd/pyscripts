"""Dynamic native-gRPC gateway primitives."""

from pyscripts.grpc_gateway.registry import GrpcRoute, GrpcRouteRegistry
from pyscripts.grpc_gateway.server import GrpcGateway

__all__ = ["GrpcGateway", "GrpcRoute", "GrpcRouteRegistry"]
