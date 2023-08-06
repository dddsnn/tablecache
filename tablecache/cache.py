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


class Cache:
    def __init__(self, db, storage):
        self._db = db
        self._storage = storage

    async def cache_table(self, db_table, storage_table):
        cached_table = Table(db_table, storage_table)
        await cached_table.load()
        return cached_table


class Table:
    def __init__(self, db_table, storage_table):
        self._db_table = db_table
        self._storage_table = storage_table

    async def load(self):
        async for record in self._db_table.all():
            await self._storage_table.put(record)

    async def get(self, key):
        return await self._storage_table.get(key)
