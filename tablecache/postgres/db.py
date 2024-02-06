# Copyright 2024 Marc Lehmann

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

import typing as t

import asyncpg

import tablecache.db as db
import tablecache.types as tp


class PostgresAccess(db.DbAccess[db.QueryArgsDbRecordsSpec]):
    """
    Postgres access.

    Provides access to records stored in Postgres via records specs that
    contain a query and arguments.

    Creates an asyncpg.pool.Pool connection pool on construction which is
    opened/closed on __aenter__() and __aexit__().
    """

    def __init__(self, **pool_kwargs: t.Any) -> None:
        """
        :param pool_kwargs: Arguments that will be passed to
            asyncpg.create_pool() to create the connection pool. The pool is
            only created, not connected. Arguments min_size=0 and max_size=1
            are added unless otherwise specified.
        """
        pool_kwargs.setdefault('min_size', 0)
        pool_kwargs.setdefault('max_size', 1)
        self._pool = asyncpg.create_pool(**pool_kwargs)

    async def __aenter__(self):
        await self._pool.__aenter__()
        return self

    async def __aexit__(self, *_):
        await self._pool.__aexit__()
        return False

    @t.override
    async def get_records(
            self, records_spec: db.QueryArgsDbRecordsSpec) -> tp.AsyncRecords:
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(
                    records_spec.query, *records_spec.args):
                yield record
