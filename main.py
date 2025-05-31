from contextlib import asynccontextmanager
from typing import AsyncIterator

from mcp.server.fastmcp import FastMCP

from connectors.postgres import PostgresConnector


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[None]:
    """Manage application lifecycle with type-safe context"""
    postgres = PostgresConnector(
        mcp_server=server,
        name="local",
        database_url="postgresql://indiebi:Password1!@localhost:5432/postgres",
    )
    await postgres.initialize_pool()
    yield


def main():
    mcp = FastMCP("research", port=8001, lifespan=app_lifespan)
    mcp.run(transport="sse")


if __name__ == "__main__":
    main()
