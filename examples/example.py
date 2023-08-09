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
import pathlib
import sys

sys.path.append(str(pathlib.Path(__file__).parent.parent))

import asyncpg

import tablecache as tc


async def main():
    base_query_string = '''
        SELECT uc.*, u.name AS user_name, c.name AS city_name
        FROM
            users u
            JOIN users_cities uc USING (user_id)
            JOIN cities c USING (city_id)'''
    query_some_string = f'{base_query_string} WHERE uc.user_id = ANY ($1)'
    postgres_pool = asyncpg.create_pool(
        min_size=0, max_size=1,
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_storage = tc.RedisStorage()
    db_table = tc.PostgresTable(
        postgres_pool, base_query_string, query_some_string)
    storage_table = tc.RedisTable(
        redis_storage,
        primary_key_name='user_id',
        encoders={
            'user_id': tc.encode_int_as_str,
            'user_name': tc.encode_str,
            'city_name': tc.encode_str,},
        decoders={
            'user_id': tc.decode_int_as_str,
            'user_name': tc.decode_str,
            'city_name': tc.decode_str,},
    )
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(redis_storage)
        await stack.enter_async_context(postgres_pool)
        table = tc.CachedTable(db_table, storage_table)
        await table.load()
        print(await table.get(1))


if __name__ == '__main__':
    asyncio.run(main())
