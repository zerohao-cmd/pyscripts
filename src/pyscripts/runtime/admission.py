from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Literal

TaskType = Literal["io"]
LeaseState = Literal["RESERVED", "RUNNING"]
ActorState = Literal[
    "IDLE",
    "SHARED_IO",
    "SATURATED_IO",
    "DRAINING",
]


class InvalidLeaseError(RuntimeError):
    pass


@dataclass(slots=True)
class Lease:
    id: str
    request_id: str
    task_type: TaskType
    service: str
    revision: str
    state: LeaseState
    expires_at: float | None


class AdmissionController:
    """Atomic shared-IO admission for an asynchronous actor."""

    def __init__(self, max_io: int, lease_ttl_seconds: float):
        if max_io < 1:
            raise ValueError("max_io must be positive")
        if lease_ttl_seconds <= 0:
            raise ValueError("lease_ttl_seconds must be positive")
        self.max_io = max_io
        self.lease_ttl_seconds = lease_ttl_seconds
        self.active_io = 0
        self.actor_draining = False
        self.leases: dict[str, Lease] = {}
        self.lock = asyncio.Lock()

    async def try_reserve(
        self,
        request_id: str,
        task_type: TaskType,
        service: str,
        revision: str,
    ) -> str | None:
        now = time.monotonic()
        async with self.lock:
            self._expire_unlocked(now)
            if self.actor_draining:
                return None

            if task_type != "io":
                raise ValueError("IO actors only accept io task leases")
            if self.active_io >= self.max_io:
                return None
            self.active_io += 1

            lease_id = uuid.uuid4().hex
            self.leases[lease_id] = Lease(
                id=lease_id,
                request_id=request_id,
                task_type=task_type,
                service=service,
                revision=revision,
                state="RESERVED",
                expires_at=now + self.lease_ttl_seconds,
            )
            return lease_id

    async def start(self, lease_id: str) -> Lease:
        now = time.monotonic()
        async with self.lock:
            self._expire_unlocked(now)
            lease = self.leases.get(lease_id)
            if lease is None:
                raise InvalidLeaseError("lease does not exist or has expired")
            if lease.state != "RESERVED":
                raise InvalidLeaseError("lease has already been consumed")
            lease.state = "RUNNING"
            lease.expires_at = None
            return lease

    async def finish(self, lease_id: str) -> bool:
        async with self.lock:
            lease = self.leases.pop(lease_id, None)
            if lease is None:
                return False
            self._release_capacity_unlocked(lease)
            return True

    async def cancel_reservation(self, lease_id: str) -> bool:
        async with self.lock:
            lease = self.leases.get(lease_id)
            if lease is None or lease.state != "RESERVED":
                return False
            self.leases.pop(lease_id)
            self._release_capacity_unlocked(lease)
            return True

    async def drain_actor(self) -> None:
        async with self.lock:
            self.actor_draining = True

    async def status(self) -> dict[str, object]:
        now = time.monotonic()
        async with self.lock:
            self._expire_unlocked(now)
            state = self._state_unlocked(now)
            reserved = sum(lease.state == "RESERVED" for lease in self.leases.values())
            running = len(self.leases) - reserved
            return {
                "state": state,
                "active_io": self.active_io,
                "max_io": self.max_io,
                "reserved_leases": reserved,
                "running_leases": running,
            }

    def _state_unlocked(self, now: float) -> ActorState:
        if self.actor_draining:
            return "DRAINING"
        if self.active_io >= self.max_io:
            return "SATURATED_IO"
        if self.active_io:
            return "SHARED_IO"
        return "IDLE"

    def _expire_unlocked(self, now: float) -> None:
        expired = [
            lease_id
            for lease_id, lease in self.leases.items()
            if lease.state == "RESERVED"
            and lease.expires_at is not None
            and lease.expires_at <= now
        ]
        for lease_id in expired:
            lease = self.leases.pop(lease_id)
            self._release_capacity_unlocked(lease)

    def _release_capacity_unlocked(self, lease: Lease) -> None:
        self.active_io -= 1
