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

import typing as t

import tablecache.db as db
import tablecache.storage as storage


class CachedTable:
    """
    A cached table.

    Maintains records from a relatively slow storage (db_table) in a relatively
    fast one (storage_table).
    """
    def __init__(
            self, db_table: db.DbTable,
            storage_table: storage.StorageTable) -> None:
        self._db_table = db_table
        self._storage_table = storage_table
        self._dirty_keys = set()

    async def load(self) -> None:
        """
        Load all data from the DB into storage.

        Clears the storage first.
        """
        await self._storage_table.clear()
        async for record in self._db_table.all():
            await self._storage_table.put(record)

    async def get(self, key: t.Any) -> t.Any:
        """
        Get a key from storage.

        In case the key has been marked as dirty, ensures the data is fresh
        first.

        Raises a KeyError if the key doesn't exist.
        """
        if key in self._dirty_keys:
            await self._refresh_dirty()
        return await self._storage_table.get(key)

    async def invalidate(self, key: t.Any) -> None:
        """
        Mark a key in storage as dirty.

        Data belonging to a dirty key is guaranteed to be fetched from the DB
        again before being served to a client.
        """
        self._dirty_keys.add(key)

    async def _refresh_dirty(self) -> None:
        async for record in self._db_table.get(self._dirty_keys):
            await self._storage_table.put(record)
        self._dirty_keys.clear()
