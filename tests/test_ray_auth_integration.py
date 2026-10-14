from __future__ import annotations

import os
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import grpc
import pytest
from ray.core.generated import ray_client_pb2, ray_client_pb2_grpc


def test_native_ray_client_requires_matching_token(tmp_path: Path) -> None:
    """Test a real token-enabled Ray server without touching a remote cluster."""
    with socket.socket() as sock, socket.socket() as client_sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        client_sock.bind(("127.0.0.1", 0))
        client_port = client_sock.getsockname()[1]
    token = secrets.token_urlsafe(32)
    server_code = """
import signal, sys, time
from pathlib import Path
import ray
from ray.util.client.server.server import init_and_serve
handle, _ = init_and_serve('127.0.0.1', int(sys.argv[1]),
    address='local', num_cpus=1, include_dashboard=False,
    object_store_memory=100 * 1024 * 1024,
    _temp_dir=sys.argv[2], _node_ip_address='127.0.0.1')
# Start the normal per-job Ray Client proxy used by a ray start --head cluster.
node = ray._private.worker._global_node
node._ray_params.ray_client_server_port = int(sys.argv[4])
node.start_ray_client_server()
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
try:
    Path(sys.argv[3]).touch()
    while True:
        time.sleep(0.1)
finally:
    handle.grpc_server.stop(0).wait()
    ray.shutdown()
"""
    ready_file = tmp_path / "ready"
    environment = {**os.environ, "RAY_AUTH_MODE": "token", "RAY_AUTH_TOKEN": token}
    # Ray's Unix socket paths need a short parent directory (<107 bytes total).
    with tempfile.TemporaryDirectory(prefix="ra-") as ray_tmp, (tmp_path / "ray-server.log").open("w+") as log:
        process = subprocess.Popen(
            [sys.executable, "-c", server_code, str(port), ray_tmp, str(ready_file), str(client_port)],
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 40
            while not ready_file.exists():
                if process.poll() is not None:
                    log.seek(0)
                    pytest.fail(f"Authenticated local Ray exited:\n{log.read()}")
                if time.monotonic() >= deadline:
                    pytest.fail("Authenticated local Ray did not become ready")
                time.sleep(0.1)
            with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                stub = ray_client_pb2_grpc.RayletDriverStub(channel)
                request = ray_client_pb2.ClusterInfoRequest()
                for metadata in [(), (("authorization", "Bearer incorrect-token"),)]:
                    with pytest.raises(grpc.RpcError) as error:
                        stub.ClusterInfo(request, metadata=metadata, timeout=5)
                    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
                stub.ClusterInfo(request, metadata=(("authorization", f"Bearer {token}"),), timeout=5)

            # Ray Client uses this same channel factory to attach process
            # credentials; check it in a fresh process without manual metadata.
            client_code = """
import sys
from ray._private.grpc_utils import init_grpc_channel
from ray.core.generated import ray_client_pb2, ray_client_pb2_grpc
with init_grpc_channel(sys.argv[1]) as channel:
    stub = ray_client_pb2_grpc.RayletDriverStub(channel)
    response = stub.ClusterInfo(ray_client_pb2.ClusterInfoRequest(
        type=ray_client_pb2.ClusterInfoType.CLUSTER_RESOURCES), timeout=5)
    assert response.resource_table.table['CPU'] == 1
"""
            completed = subprocess.run(
                [sys.executable, "-c", client_code, f"127.0.0.1:{port}"],
                env=environment, capture_output=True, text=True, timeout=15,
            )
            assert completed.returncode == 0, completed.stderr
            application_code = """
import sys
import ray
from pydantic import SecretStr
from pyscripts.config import Settings
from pyscripts.ray_client import connect_ray
try:
    connect_ray(Settings(database_url=SecretStr('sqlite+aiosqlite:///:memory:'),
                         ray_address=sys.argv[1]))
    assert any(n['Alive'] for n in ray.nodes())
    assert ray.get(ray.remote(lambda: 42).remote(), timeout=20) == 42
finally:
    ray.shutdown()
"""
            completed = subprocess.run(
                [sys.executable, "-c", application_code, f"ray://127.0.0.1:{client_port}"],
                env=environment, capture_output=True, text=True, timeout=40,
            )
            assert completed.returncode == 0, completed.stderr
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
