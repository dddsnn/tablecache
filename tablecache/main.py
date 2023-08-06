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

import asyncio
import contextlib
import os
import signal

import cache
import db
import storage


def shutdown(shutdown_event):
    shutdown_event.set()


async def main():
    query_string = '''
    SELECT uc.*, u.name AS user_name, c.name AS city_name
    FROM
        users u
        JOIN users_cities uc USING (user_id)
        JOIN cities c USING (city_id)'''
    shutdown_event = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(
        signal.SIGTERM, shutdown, shutdown_event)
    postgres_db = db.PostgresDb(
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_storage = storage.RedisStorage()
    table_cache = cache.Cache(postgres_db, redis_storage)
    db_table = db.PostgresTable(postgres_db, query_string)
    storage_table = storage.RedisTable(redis_storage, 't', 'user_id')
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(redis_storage)
        await stack.enter_async_context(postgres_db)
        table = await table_cache.cache_table(db_table, storage_table)


if __name__ == '__main__':
    asyncio.run(main())
