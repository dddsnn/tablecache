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

import collections.abc as ca
import typing as t

import tablecache.db as db
import tablecache.storage as storage
import tablecache.range as rng


class CachedTable:
    """
    A cached table.

    Maintains records from a relatively slow storage (db_table) in a relatively
    fast one (storage_table).
    """
    def __init__(
        self, db_table: db.DbTable, storage_table: storage.StorageTable,
        cache_range: rng.Range = rng.AllRange()
    ) -> None:
        """
        :param cache_range: A range of values that should be cached. Only
            records matching this range are queried from the DB and put into
            storage. The default AllRange will load everything.
        """
        self._db_table = db_table
        self._storage_table = storage_table
        self._cache_range = cache_range
        self._dirty_keys = set()

    async def load(self) -> None:
        """
        Load all relevant data from the DB into storage.

        Loads all records matching the configured cache range. Clears the
        storage first.
        """
        await self._storage_table.clear()
        async for record in self._db_table.get_record_range(self._cache_range):
            await self._storage_table.put_record(record)

    async def get_record(self, primary_key: t.Any) -> ca.Mapping[str, t.Any]:
        """
        Get a record from storage by primary key.

        In case the key has been marked as dirty, ensures the data is fresh
        first.

        In case the primary key doesn't exist in cache, also tries the DB in
        case the key is from outside the cached range. This implies that
        querying keys that may not exist is potentially costly. There is
        however a special case if the cached range is the AllRange, where the
        DB is not checked (since everything is cached).

        Raises a KeyError if the key doesn't exist.
        """
        if primary_key in self._dirty_keys:
            await self._refresh_dirty()
        try:
            return await self._storage_table.get_record(primary_key)
        except KeyError:
            if isinstance(self._cache_range, rng.AllRange):
                raise
            return await self._db_table.get_record(primary_key)

    async def get_record_range(
            self, range: rng.Range) -> t.AsyncIterator[ca.Mapping[str, t.Any]]:
        """
        Asynchronously iterate over records from a range.

        Iterates over cached records from the given range, but only if it is
        fully contained in the configured cache range (i.e. no records are
        missing). Otherwise, queries the DB for the entire range and yields
        those records. This implies that querying a range that isn't completely
        in cache (even if just by a little bit) is expensive.
        """
        if self._cache_range.covers(range):
            source = self._storage_table
        else:
            source = self._db_table
        async for record in source.get_record_range(range):
            yield record

    async def invalidate(self, primary_key: t.Any) -> None:
        """
        Mark a key in storage as dirty.

        Data belonging to a dirty key is guaranteed to be fetched from the DB
        again before being served to a client.
        """
        self._dirty_keys.add(primary_key)

    async def _refresh_dirty(self) -> None:
        async for record in self._db_table.get_records(self._dirty_keys):
            await self._storage_table.put_record(record)
        self._dirty_keys.clear()
