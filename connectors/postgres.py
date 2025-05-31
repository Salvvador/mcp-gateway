import json

import asyncpg
from mcp.server.fastmcp import FastMCP


class PostgresConnector:
    def __init__(self, mcp_server: FastMCP, name: str, database_url: str):
        self.pool = None
        self.mcp = mcp_server
        self.name = name
        self.database_url = database_url
        self._register_tools_and_resources()

    async def initialize_pool(self):
        """Initialize the database connection pool"""
        self.pool = await asyncpg.create_pool(self.database_url)

    async def close_pool(self):
        """Close the database connection pool"""
        if self.pool:
            await self.pool.close()

    def _register_tools_and_resources(self):
        """Register MCP tools and resources with the server"""
        self.mcp.tool(name=f"{self.name}_query")(self.query)
        self.mcp.resource(
            "postgres://table/{table_name}/schema", name=f"{self.name}_get_table_schema"
        )(self.get_table_schema)
        self.mcp.resource("postgres://tables", name=f"{self.name}_list_tables")(
            self.list_tables
        )

    async def query(self, sql: str) -> str:
        """Run a read-only SQL query against the database"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        async with self.pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                try:
                    rows = await conn.fetch(sql)

                    result_data = [dict(row) for row in rows]

                    return json.dumps(result_data, indent=2, default=str)
                except Exception as e:
                    raise e

    async def get_table_schema(self, table_name: str) -> str:
        """Get the schema information for a specific database table"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
                table_name,
            )

            schema_data = [dict(row) for row in rows]

            return json.dumps(schema_data, indent=2)

    async def list_tables(self) -> str:
        """List all tables in the public schema"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )

            tables = [row["table_name"] for row in rows]

            return json.dumps({"tables": tables}, indent=2)
