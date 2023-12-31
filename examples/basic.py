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
import redis.asyncio as redis

import tablecache as tc


# In this basic example, we have 2 Postgres tables that are joined together,
# where the join has a unique key. We want to store the result of the join in a
# fast storage (Redis).
async def main():
    # We define 2 query strings: one that queries for a subset of the join (in
    # our case the entire result set), and one that filters out rows with
    # specific primary keys. These query strings are passed into a
    # PostgresTable to represent our access to the data.
    query_subset_string = '''
        SELECT
            uc.*, u.name AS user_name, u.nickname AS user_nickname,
            c.name AS city_name
        FROM
            users u
            JOIN users_cities uc USING (user_id)
            JOIN cities c USING (city_id)'''
    query_pks_string = f'{query_subset_string} WHERE uc.user_id = ANY ($1)'
    postgres_pool = asyncpg.create_pool(
        min_size=0, max_size=1,
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_conn = redis.Redis()
    db_table = tc.PostgresTable(
        postgres_pool, query_subset_string, query_pks_string)
    # We specify a CachedTable which will create a RedisTable that can accept
    # records of the shape in our Postgres table. We need to give it a
    # CachedSubset subclass that it uses to query both Postgres and Redis. The
    # choice of All here loads everything, but doesn't allow meaningful queries
    # for ranges of values (only queries for individual records by primary
    # key). See the advanced examples for more options. (N.B. The
    # with_primary_key classmethod actually returns a subclass with the
    # primary_key_name class member set).
    #
    # Then, in addition to the DB table, we need to specify parameters to
    # create the RedisTable. These are a name for the table in (used to create
    # a namespace), the name of the primary key column, as well as a dictionary
    # of codecs for the attributes. Each key in it must be a column of the
    # table, and each value must be a codec (basic ones available in
    # tablecache.codec) which is capable of encoding the corresponding value to
    # bytes. The special Nullable codec can wrap another codec and enable null
    # values (by themselves, normal codecs can't store nulls). Note that only
    # attributes for which a codec has been specified are cached.
    table = tc.CachedTable(
        tc.All.with_primary_key('user_id'),
        db_table,
        primary_key_name='user_id',
        attribute_codecs={
            'user_id': tc.IntAsStringCodec(),
            'user_name': tc.StringCodec(),
            'user_nickname': tc.Nullable(tc.StringCodec()),
            'city_name': tc.StringCodec(),},
        redis_conn=redis_conn,
        redis_table_name='users_cities',
    )
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(postgres_pool)
        await setup_example_dbs(redis_conn, postgres_pool)
        await table.load()
        # After loading the CachedTable, we can query it, hitting the cache
        # rather than the DB.
        print(f'User 1 in Redis: {await table.get_record(1)}')
        print(f'User 2 in Redis: {await table.get_record(2)}')
        # When data in the DB changes, the cache will still return the old
        # record. Once we invalidate it (by primary key), the cache will
        # refresh that record on the next request.
        await postgres_pool.execute(
            'UPDATE users SET name=$1 WHERE user_id=$2', 'New Name', 1)
        print(f'User 1 still with old name: {await table.get_record(1)}')
        await table.invalidate_record(1)
        print(f'User 1 with new name: {await table.get_record(1)}')
        # We can also invalidate primary keys that didn't exist in cache at all
        # yet. This will cause them to be loaded.
        await postgres_pool.execute(
            '''
            INSERT INTO users(user_id, name) VALUES(3, 'User 3');
            INSERT INTO users_cities(user_id, city_id)
            SELECT 3, city_id FROM cities LIMIT 1''')
        await table.invalidate_record(3)
        print(f'Newly inserted user 3: {await table.get_record(3)}')
    await redis_conn.close()


async def setup_example_dbs(redis_conn, postgres_pool):
    await redis_conn.flushall()
    await postgres_pool.execute(
        '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE users (
            user_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            name text NOT NULL,
            nickname text
        );
        CREATE TABLE cities (
            city_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            name text NOT NULL
        );
        CREATE TABLE users_cities (
            user_id integer PRIMARY KEY REFERENCES users(user_id),
            city_id integer NOT NULL REFERENCES cities(city_id)
        );

        INSERT INTO users(user_id, name, nickname) VALUES(1, 'User 1', 'u1');
        WITH c AS (INSERT INTO cities(name) VALUES('City 1') RETURNING city_id)
        INSERT INTO users_cities(user_id, city_id)
        SELECT 1, c.city_id FROM c;

        INSERT INTO users(user_id, name) VALUES(2, 'User 2');
        WITH c AS (INSERT INTO cities(name) VALUES('City 2') RETURNING city_id)
        INSERT INTO users_cities(user_id, city_id)
        SELECT 2, c.city_id FROM c;''')


if __name__ == '__main__':
    asyncio.run(main())
