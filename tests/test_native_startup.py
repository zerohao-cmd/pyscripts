from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import grpc
import httpx
import pytest


def unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_native_gateway_serves_http_ui_and_grpc(tmp_path: Path) -> None:
    """Exercise the real entrypoint, sockets, auth, probes and graceful exit."""
    root = Path(__file__).resolve().parents[1]
    ui = tmp_path / "ui"
    ui.mkdir()
    (ui / "index.html").write_text("<html>native-startup-console</html>")
    http_port, grpc_port = unused_port(), unused_port()
    while http_port == grpc_port:
        grpc_port = unused_port()
    environment = {
        **os.environ,
        "PYSCRIPTS_DATABASE_URL": f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        "PYSCRIPTS_AUTO_CREATE_SCHEMA": "true",
        "PYSCRIPTS_ADMIN_PASSWORD": "native-test-password",
        "PYSCRIPTS_ADMIN_SESSION_SECRET": "native-test-session-secret-at-least-32-characters",
        "PYSCRIPTS_ADMIN_COOKIE_SECURE": "false",
        "PYSCRIPTS_OBJECT_STORE_ENABLED": "false",
        "PYSCRIPTS_RAY_ADDRESS": "local",
        "PYSCRIPTS_RAY_USE_LABEL_SELECTOR": "false",
        "PYSCRIPTS_GRPC_ENABLED": "true",
        "PYSCRIPTS_GRPC_HOST": "127.0.0.1",
        "PYSCRIPTS_GRPC_PORT": str(grpc_port),
        "PYSCRIPTS_SERVE_UI": "true",
        "PYSCRIPTS_UI_DIST_PATH": str(ui),
        "PYSCRIPTS_CONTRACT_CACHE_ROOT": str(tmp_path / "contracts"),
        "RAY_AUTH_MODE": "disabled",
    }
    with (tmp_path / "gateway.log").open("w+") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "pyscripts.api:app", "--host",
             "127.0.0.1", "--port", str(http_port), "--workers", "1"],
            cwd=root,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{http_port}", timeout=2) as client:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        log.seek(0)
                        pytest.fail(f"Native gateway exited during startup:\n{log.read()}")
                    try:
                        if client.get("/health/ready").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    time.sleep(0.1)
                else:
                    pytest.fail("Native gateway did not become ready")
                assert client.get("/health/live").json() == {"status": "ok"}
                assert "native-startup-console" in client.get("/").text
                assert "native-startup-console" in client.get("/api-docs").text
                assert client.get("/admin/services").status_code == 401
                assert client.post("/auth/login", json={"password": "native-test-password"}).status_code == 200
                assert client.get("/admin/services").status_code == 200
                issued = client.post("/admin/api-tokens", json={"name": "native-smoke"})
                assert issued.status_code == 201
                assert issued.json()["token"].startswith("pst_")

            with grpc.insecure_channel(f"127.0.0.1:{grpc_port}") as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                with pytest.raises(grpc.RpcError) as error:
                    channel.unary_unary("/native.smoke.Unknown/Call")(b"", timeout=5)
                assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        assert process.returncode in (0, -15)
