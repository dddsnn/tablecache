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
import typing as t

import asyncpg.pool

import tablecache.subset as ss
import tablecache.types as tp


class DbTable[PrimaryKey](abc.ABC):
    @abc.abstractmethod
    async def get_record_subset(self, subset: ss.Subset) -> tp.Records:
        """
        Asynchronously iterate over a subset of records.

        Yields records from the given subset using its query parameters.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_records(
            self, primary_keys: t.Sequence[PrimaryKey]) -> tp.Records:
        """
        Asynchronously iterate over records matching primary keys.

        Yields all records whose primary key matches one in the given sequence.
        If a key doesn't exist in the table, it is ignored and no error is
        raised.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Get a single record by primary key.

        Raises a KeyError if no matching record exists.
        """
        raise NotImplementedError


class PostgresTable[PrimaryKey](DbTable[PrimaryKey]):
    """
    Postgres table abstraction.

    Represents a table, or any table-like result set in Postgres that can be
    queried for a subset of records, or just some specific ones. The table is
    specified as query strings, one to get a whole subset in the table, and one
    to get only certain records by their primary key.

    While the table may be a join of many tables or other construct, it must
    have a column functioning as primary key, i.e. one that uniquely identifies
    any row in the table.
    """
    def __init__(
            self, pool: asyncpg.pool.Pool, query_subset_string: str,
            query_pks_string: str) -> None:
        """
        :param pool: A connection pool that is ready to be used (i.e. already
            set up and connected).
        :param query_subset_string: A query string to fetch a subset of
            records. The string must contain query arguments ($1, $2 etc.) that
            match the type of Subset used when calling get_record_subset()
            (i.e. the Subset's db_args tuple). The simplest case, All has no
            parameters and will fetch everything in the given query.
        :param query_pks_string: A query string that allows filtering to fetch
            only specific records by primary key. This is done by setting
            argument $1 to a sequence of primary keys, so this string
            essentially has to include "= ANY($1)" somewhere, likely taking a
            shape similar to "WHERE my_primary_key = ANY($1)". It can probably
            be created from a similar base query as the query_subset_string.
        """
        self._pool = pool
        self.query_subset_string = query_subset_string
        self.query_pks_string = query_pks_string

    @t.override
    async def get_record_subset(self, subset: ss.Subset) -> tp.Records:
        async with self._pool.acquire() as conn, conn.transaction():
            cursor = conn.cursor(self.query_subset_string, *subset.db_args)
            async for record in cursor:
                yield record

    @t.override
    async def get_records(
            self, primary_keys: t.Sequence[PrimaryKey]) -> tp.Records:
        async with self._pool.acquire() as conn, conn.transaction():
            async for record in conn.cursor(self.query_pks_string,
                                            primary_keys):
                yield record

    @t.override
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        try:
            return await anext(self.get_records([primary_key]))
        except StopAsyncIteration as e:
            raise KeyError from e
