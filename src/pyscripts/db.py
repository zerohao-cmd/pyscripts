from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy import inspect
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from pyscripts.config import Settings
from pyscripts.models import Base


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.sqlalchemy_url,
        echo=settings.database_echo,
        pool_pre_ping=True,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


async def create_schema(engine: AsyncEngine) -> None:
    async with engine.begin() as connection:
        await connection.run_sync(_create_and_upgrade_schema)


def _create_and_upgrade_schema(connection: Connection) -> None:
    Base.metadata.create_all(connection)
    columns = {
        column["name"] for column in inspect(connection).get_columns("services")
    }
    if "runtime_profile" not in columns:
        connection.exec_driver_sql(
            "ALTER TABLE services ADD COLUMN runtime_profile VARCHAR(256)"
        )
    if "runtime_tracking_mode" not in columns:
        connection.exec_driver_sql(
            "ALTER TABLE services ADD COLUMN runtime_tracking_mode VARCHAR(32)"
        )


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    async with factory() as session, session.begin():
        yield session
