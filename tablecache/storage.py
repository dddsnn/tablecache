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

import redis.asyncio as redis


class RedisStorage:
    def __init__(self, **connect_kwargs):
        self._conn_factory = functools.partial(redis.Redis, **connect_kwargs)

    async def __aenter__(self):
        self._conn = self._conn_factory()
        return self

    async def __aexit__(self, *_):
        await self._conn.close()
        del self._conn
        return False

    @property
    def conn(self):
        try:
            return self._conn
        except AttributeError as e:
            raise AttributeError(
                'You have to connect the storage before using it.') from e


class RedisTable:
    def __init__(self, redis_storage, table_name, primary_key):
        self._storage = redis_storage
        self.table_name = table_name
        self.primary_key = primary_key

    async def put(self, record):
        record_key = record[self.primary_key]
        await self._storage.conn.hset(
            f'{self.table_name}:{record_key}', mapping=record)

    async def get(self, key):
        return await self._storage.conn.hgetall(f'{self.table_name}:{key}')
