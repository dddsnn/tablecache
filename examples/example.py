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

import tablecache as tc


def encode_str(s):
    return s.encode()


def decode_str(bs):
    return bs.decode()


def encode_int_str(i):
    return str(i).encode()


def decode_int_str(bs):
    return int(bs.decode())


async def main():
    query_string = '''
    SELECT uc.*, u.name AS user_name, c.name AS city_name
    FROM
        users u
        JOIN users_cities uc USING (user_id)
        JOIN cities c USING (city_id)'''
    postgres_db = tc.PostgresDb(
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_storage = tc.RedisStorage()
    db_table = tc.PostgresTable(postgres_db, query_string)
    storage_table = tc.RedisTable(
        redis_storage,
        table_name='t',
        primary_key_name='user_id',
        encoders={
            'user_id': encode_int_str,
            'user_name': encode_str,
            'city_name': encode_str,},
        decoders={
            'user_id': decode_int_str,
            'user_name': decode_str,
            'city_name': decode_str,},
    )
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(redis_storage)
        await stack.enter_async_context(postgres_db)
        table = tc.CachedTable(db_table, storage_table)
        await table.load()
        print(await table.get(1))


if __name__ == '__main__':
    asyncio.run(main())
