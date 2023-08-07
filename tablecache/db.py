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

import functools

import asyncpg


class PostgresDb:
    def __init__(self, **connect_kwargs):
        self._pool_factory = functools.partial(
            asyncpg.create_pool, min_size=0, max_size=1, **connect_kwargs)

    async def __aenter__(self):
        self._pool = await self._pool_factory()
        return self

    async def __aexit__(self, *_):
        await self._pool.close()
        del self._pool
        return False

    @property
    def pool(self):
        try:
            return self._pool
        except AttributeError as e:
            raise AttributeError(
                'You have to connect the DB before using it.') from e


class PostgresTable:
    def __init__(self, postgres_db, query_string):
        self._db = postgres_db
        self.query_string = query_string

    async def all(self):
        async with self._db.pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_string):
                yield record
