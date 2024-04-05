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

import collections.abc as ca
import typing as t

import asyncpg

import tablecache.db as db

type DbRecord = asyncpg.Record
type RecordsSpec[Record] = db.QueryArgsDbRecordsSpec[DbRecord, Record]


class PostgresAccess[Record](db.DbAccess[DbRecord, RecordsSpec[Record]]):
    """
    Postgres access.

    Provides access to records stored in Postgres via records specs that
    contain a query and arguments.

    Creates an asyncpg.pool.Pool connection pool on construction which is
    opened/closed on :py:meth:`__aenter__()` and :py:meth:`__aexit__()`.
    """

    def __init__(
            self,
            record_parser: t.Optional[
                db.RecordParser[DbRecord, Record]] = None,
            **pool_kwargs: t.Any) -> None:
        """
        :param record_parser: An optional function that is applied to each
            record before it is returned. This overrides the parser in the
            records spec.

            .. deprecated:: 4.2
                Use the record parser in the records spec instead.
        :param pool_kwargs: Arguments that will be passed to
            :external:py:func:`asyncpg.create_pool <asyncpg.pool.create_pool>`
            to create the connection pool. The pool is only created, not
            connected. Arguments ``min_size=0`` and ``max_size=1`` are added
            unless otherwise specified.
        """
        self._record_parser = record_parser
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
            self, records_spec: RecordsSpec[Record]
    ) -> ca.AsyncIterator[Record]:
        record_parser = self._record_parser or records_spec.record_parser
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(
                    records_spec.query, *records_spec.args):
                yield record_parser(record)
