import asyncio
import inspect
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, TypedDict, Callable
import ray

# 允许任务类型
TASK_TYPE = Literal["io", "compute"]
ACTOR_STATUS = Literal["SHARED", "SATURATED", "IDLE", "EXCLUSIVE"]


class LeaseInfo(TypedDict):
    request_id: str  # 请求id
    task_type: TASK_TYPE  # 任务类型
    service: str  # 服务id
    version: str  # 该次请求使用的服务代码版本


class StatusInfo(TypedDict):
    state: TASK_TYPE
    active_io: int
    max_io: int
    profile: dict
    versions: dict[str, str]


@ray.remote
class Actor:
    """
    任务执行
    """

    def __init__(self, profile: dict, max_io: int = 100):
        self.profile = profile
        self.max_io = max_io

        self.active_io = 0
        self.exclusive = False

        self.lock = asyncio.Lock()
        self.leases: dict[str, LeaseInfo] = {}

        # 计算逻辑不直接阻塞Actor事件循环
        self.compute_executor = ThreadPoolExecutor(max_workers=1)

        # (service, version) -> callable
        self.runners: dict[(str, str), Callable] = {}

        # service -> active_version
        self.active_versions: dict[str, str] = {}

    async def try_reserve(
        self,
        request_id: str,
        task_type: TASK_TYPE,
        service: str,
        version: str | None = None,
    ) -> str:
        """
        尝试保留此次请求
        """
        async with self.lock:
            version = version or self.active_versions.get(service)

            if (service, version) not in self.runners:
                return None

            if task_type == "io":
                if self.exclusive or self.active_io >= self.max_io:
                    return None
                self.active_io += 1

            elif task_type == "compute":
                if self.exclusive or self.active_io != 0:
                    return None
                self.exclusive = True

            else:
                raise ValueError(f"Unknown task type: {task_type}")

            lease_id = uuid.uuid4().hex

            self.leases[lease_id] = {
                "request_id": request_id,
                "task_type": task_type,
                "service": service,
                "version": version,
            }

            return lease_id

    async def execute(self, lease_id: str, context: dict, params: dict):
        lease = self.leases.get(lease_id)
        if lease is None:
            raise RuntimeError("Invalid or expired lease")

        # 根据服务和版本信息获取执行逻辑
        runner = self.runners[(lease["service"], lease["version"])]

        try:
            if lease["task_type"] == "io":
                result = runner(context, **params)

                if not inspect.isawaitable(result):
                    raise TypeError("IO runner must be async")

                return await result

            loop = asyncio.get_running_loop()

            # exclusive状态下执行同步计算函数
            return await loop.run_in_executor(
                self.compute_executor,
                lambda: runner(context, **params),
            )

        finally:
            await self._release(lease_id)

    async def _release(self, lease_id: str):
        async with self.lock:
            lease = self.leases.pop(lease_id, None)
            if lease is None:
                return

            if lease["task_type"] == "io":
                self.active_io -= 1
            else:
                self.exclusive = False

    async def status(self):
        async with self.lock:
            if self.exclusive:
                state = "EXCLUSIVE"
            elif self.active_io == 0:
                state = "IDLE"
            elif self.active_io >= self.max_io:
                state = "SATURATED"
            else:
                state = "SHARED"

            return {
                "state": state,
                "active_io": self.active_io,
                "max_io": self.max_io,
                "profile": self.profile,
                "versions": dict(self.active_versions),
            }
