from __future__ import annotations

import asyncio

from pyscripts.runtime.admission import AdmissionController, InvalidLeaseError


async def test_io_is_shared_and_compute_is_rejected() -> None:
    admission = AdmissionController(max_io=2, lease_ttl_seconds=1)

    first_io = await admission.try_reserve("io-1", "io", "service", "rev")
    second_io = await admission.try_reserve("io-2", "io", "service", "rev")
    assert first_io is not None
    assert second_io is not None
    assert await admission.try_reserve("io-3", "io", "service", "rev") is None
    try:
        await admission.try_reserve("compute-1", "compute", "service", "rev")
    except ValueError:
        pass
    else:
        raise AssertionError("compute work was accepted by an IO actor")

    await admission.start(first_io)
    assert await admission.finish(first_io) is True
    assert await admission.cancel_reservation(second_io) is True

    third_io = await admission.try_reserve("io-3", "io", "service", "rev")
    assert third_io is not None
    await admission.start(third_io)
    assert await admission.finish(third_io) is True
    assert (await admission.status())["state"] == "IDLE"


async def test_draining_actor_rejects_new_io() -> None:
    admission = AdmissionController(max_io=10, lease_ttl_seconds=1)
    io_lease = await admission.try_reserve("io", "io", "service", "rev")
    assert io_lease is not None
    await admission.start(io_lease)

    await admission.drain_actor()
    assert (await admission.status())["state"] == "DRAINING"
    assert await admission.try_reserve("new-io", "io", "service", "rev") is None
    await admission.finish(io_lease)
    assert (await admission.status())["state"] == "DRAINING"


async def test_unconsumed_lease_expires_and_releases_capacity() -> None:
    admission = AdmissionController(max_io=1, lease_ttl_seconds=0.01)
    expired = await admission.try_reserve("io", "io", "service", "rev")
    assert expired is not None
    await asyncio.sleep(0.02)

    replacement = await admission.try_reserve("io-2", "io", "service", "rev")
    assert replacement is not None

    try:
        await admission.start(expired)
    except InvalidLeaseError:
        pass
    else:
        raise AssertionError("expired lease was accepted")
