# Copyright 2024 Marc Lehmann

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
import logging
import operator as op

import asyncpg
import redis.asyncio as redis

import tablecache as tc
import tablecache.postgres as tcpg
import tablecache.redis as tcr

POSTGRES_DSN = 'postgres://postgres:@localhost:5432/postgres'

# This is the basic example again, but with a RedisTable. This requires some
# extra setup, since records in Redis need to be encoded into bytes objects.


async def main():
    query_all_string = '''
        SELECT
            uc.*, u.name AS user_name, u.nickname AS user_nickname,
            c.name AS city_name
        FROM
            users u
            JOIN users_cities uc USING (user_id)
            JOIN cities c USING (city_id)'''
    query_some_string = f'{query_all_string} WHERE uc.user_id = ANY ($1)'
    indexes = tc.PrimaryKeyIndexes(
        op.itemgetter('user_id'), query_all_string, query_some_string)
    db_access = tcpg.PostgresAccess(dsn=POSTGRES_DSN)
    redis_conn = redis.Redis()
    # To create a RedisTable, we need a redis.asyncio.Redis instance (the
    # connection), a table name (which will be used as a namespace in Redis), a
    # record scorer (i.e. our Indexes), a Codec instance to en-/decode the
    # primary key, and Codecs for each of the attributes that should be stored.
    # A selection of Codecs can be found in the tablecache.redis submodule.
    # Note that, since user_nickname is nullable, we have to wrap the
    # StringCodec into a Nullable, as codecs by themselves don't support
    # storing nulls.
    storage_table = tcr.RedisTable(
        redis_conn,
        table_name='users',
        record_scorer=indexes,
        primary_key_codec=tcr.SignedInt32Codec(),
        attribute_codecs={
            'user_id': tcr.SignedInt32Codec(),
            'user_name': tcr.StringCodec(),
            'user_nickname': tcr.Nullable(tcr.StringCodec()),
            'city_name': tcr.StringCodec(),
        })
    table = tc.CachedTable(indexes, db_access, storage_table)
    async with db_access, asyncpg.create_pool(POSTGRES_DSN) as postgres_pool:
        await setup_example_db(redis_conn, postgres_pool)
        await table.load('primary_key', all_primary_keys=True)
        await table.loaded()
        print('Users with primary key 1 and 2:')
        async for user in table.get_records('primary_key', 1, 2):
            print(user)
        try:
            print('Trying to find user with primary key 3:')
            await table.get_first_record('primary_key', 3)
        except KeyError:
            print('No user with primary key 3 exists.')
        await postgres_pool.execute(
            'UPDATE users SET name = $1 WHERE user_id = $2', 'New Name', 1)
        print('User 1 has changed in the DB, but not yet in cache:')
        print(await table.get_first_record('primary_key', 1))
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 1)],
            [indexes.IndexSpec('primary_key', 1)])
        print('After invalidating, a refresh is triggered to update user 1:')
        print(await table.get_first_record('primary_key', 1))
        await postgres_pool.execute('''
            INSERT INTO users(user_id, name) VALUES(3, 'User 3');
            INSERT INTO users_cities(user_id, city_id)
            SELECT 3, city_id FROM cities LIMIT 1''')
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 3)],
            [indexes.IndexSpec('primary_key', 3)])
        print('Newly inserted user 3:')
        print(await table.get_first_record('primary_key', 3))
    await redis_conn.close()


async def setup_example_db(redis_conn, postgres_pool):
    await redis_conn.flushall()
    await postgres_pool.execute('''
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
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())