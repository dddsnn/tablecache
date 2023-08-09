# Copyright 2023 Marc Lehmann

# This file is part of tablecache.
#
# tablecache is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# tablecache is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with tablecache. If not, see <https://www.gnu.org/licenses/>.

import abc

import asyncpg.pool


class DbTable(abc.ABC):
    async def all(self):
        raise NotImplementedError

    async def get(self, primary_keys):
        raise NotImplementedError


class PostgresTable(DbTable):
    def __init__(
            self, pool: asyncpg.pool.Pool, query_all_string: str,
            query_some_string: str):
        self._pool = pool
        self.query_all_string = query_all_string
        self.query_some_string = query_some_string

    async def all(self):
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_all_string):
                yield record

    async def get(self, primary_keys):
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_some_string,
                                            primary_keys):
                yield record
