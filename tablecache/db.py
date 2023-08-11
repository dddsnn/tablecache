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
import collections.abc as ca
import typing as t

import asyncpg.pool


class DbTable(abc.ABC):
    @abc.abstractmethod
    async def all(self) -> ca.AsyncIterator[ca.Mapping[str, t.Any]]:
        raise NotImplementedError

    @abc.abstractmethod
    async def get(
        self, primary_keys: t.Sequence[t.Any]
    ) -> ca.AsyncIterator[ca.Mapping[str, t.Any]]:
        raise NotImplementedError


class PostgresTable(DbTable):
    """
    Postgres table abstraction.

    Represents a table, or any table-like result set in Postgres that can be
    queried for all rows, or just some. The table is specified as query
    strings, one to get the entire table, and one to get only certain rows.

    While the table may be a join of many tables or other construct, it must
    have a column functioning as primary key, i.e. one that uniquely identifies
    any row in the table.
    """
    def __init__(
            self, pool: asyncpg.pool.Pool, query_all_string: str,
            query_some_string: str) -> None:
        """
        :param pool: A connection pool that is ready to be used.
        :param query_all_string: A query string to fetch all rows.
        :param query_some_string: A query string that allows filtering to fetch
            only specific rows. This is done by setting argument $1 to a
            sequence of primary keys, so this string essentially has to include
            "= ANY($1)" somewhere, likely taking a shape similar to "WHERE
            my_primary_key = ANY ($1)". Can probably be created based on the
            query_all_string, by appending the condition.
        """
        self._pool = pool
        self.query_all_string = query_all_string
        self.query_some_string = query_some_string

    async def all(self) -> ca.AsyncIterator[ca.Mapping[str, t.Any]]:
        """Asynchronously iterate over all rows."""
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_all_string):
                yield record

    async def get(
        self, primary_keys: t.Sequence[t.Any]
    ) -> ca.AsyncIterator[ca.Mapping[str, t.Any]]:
        """
        Asynchronously iterate over rows matching primary keys.

        Yields all rows whose primary key matches one in the given sequence. If
        a key doesn't exist in the table, it is ignored and no error is raised.
        """
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_some_string,
                                            primary_keys):
                yield record
