from __future__ import annotations

import argparse
import asyncio

from sqlalchemy import inspect, text

from pyscripts.config import get_settings
from pyscripts.db import create_engine, create_schema


async def database_status(initialize: bool) -> None:
    engine = create_engine(get_settings())
    try:
        async with engine.connect() as connection:
            database, user = (
                await connection.execute(
                    text("SELECT current_database(), current_user")
                )
            ).one()
            print(f"connected database={database} user={user}")

        if initialize:
            await create_schema(engine)

        async with engine.connect() as connection:
            tables = await connection.run_sync(
                lambda sync_connection: inspect(sync_connection).get_table_names()
            )
            print(f"tables={','.join(sorted(tables)) or '<none>'}")
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check or initialize the pyscripts database"
    )
    parser.add_argument(
        "--init",
        action="store_true",
        help="create the current development schema if it does not exist",
    )
    arguments = parser.parse_args()
    asyncio.run(database_status(arguments.init))


if __name__ == "__main__":
    main()
