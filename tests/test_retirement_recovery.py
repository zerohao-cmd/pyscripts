import asyncio
import time

from fastapi.testclient import TestClient
from pydantic import SecretStr

from pyscripts import api
from pyscripts.api import create_app
from pyscripts.config import Settings
from test_runtime_profiles import FakeRuntimeValidator, login_admin


class Retirer:
    instances = []
    interrupted = False
    recover = False

    def __init__(self, *args, **kwargs):
        self.attempts = []
        self.running = 0
        self.peak = 0
        self.instances.append(self)

    def start(self):
        pass

    async def close(self):
        pass

    async def retire_profile(self, digest, timeout_seconds):
        self.attempts.append(digest)
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            if not self.recover:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    self.interrupted = True
                    raise
            await asyncio.sleep(.04)
            if len(self.attempts) == 1:
                return False
            if len(self.attempts) == 2:
                raise RuntimeError("transient retirement failure")
            return True
        finally:
            self.running -= 1


def test_restart_recovers_and_retries_retirement_without_duplicates(tmp_path, monkeypatch):
    Retirer.instances = []
    Retirer.recover = False
    monkeypatch.setattr(api, "ProfilePoolScheduler", Retirer)
    settings = Settings(
        database_url=SecretStr(f"sqlite+aiosqlite:///{tmp_path / 'recovery.db'}"),
        grpc_enabled=False, ray_use_label_selector=False,
        runtime_profile_retirement_retry_seconds=.01,
    )
    spec = {"python_version": "3.12", "worker_pool": "default", "dependencies": []}
    with TestClient(create_app(settings, runtime_profile_validator=FakeRuntimeValidator())) as client:
        login_admin(client)
        first = client.post('/admin/runtime-labels', json={"name": "recovery", **spec}).json()
        second = client.post(f"/admin/runtime-labels/{first['label_id']}/versions", json=spec).json()
        assert client.post(f"/admin/runtime-profile-versions/{second['id']}/activate").status_code == 200
        deadline = time.monotonic() + 3
        while not Retirer.instances[0].attempts:
            assert time.monotonic() < deadline
            time.sleep(.01)
        assert len(Retirer.instances[0].attempts) == 1
    assert Retirer.instances[0].interrupted

    Retirer.recover = True
    with TestClient(create_app(settings, runtime_profile_validator=FakeRuntimeValidator())) as client:
        login_admin(client)
        deadline = time.monotonic() + 3
        while True:
            label = client.get('/admin/runtime-labels').json()[0]
            versions = {v['id']: v for v in label['versions']}
            if versions[first['id']]['status'] == 'RETIRED':
                break
            assert time.monotonic() < deadline
            time.sleep(.01)
        assert versions[second['id']]['status'] == 'ACTIVE'
        assert label['active_version_id'] == second['id']
        assert Retirer.instances[1].attempts == [first['environment_digest']] * 3
        assert Retirer.instances[1].peak == 1
