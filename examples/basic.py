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

import tablecache as tc
import tablecache.postgres as tcpg
import tablecache.local as tcl

POSTGRES_DSN = 'postgres://postgres:@localhost:5432/postgres'

# In this basic example, we have 2 Postgres tables that are joined together
# (see setup_example_db() below). We want to store the result of the join in a
# fast storage.


async def main():
    # We define 2 query strings: one that returns the entire join, and one that
    # filters for rows with specific primary keys. We give these query strings
    # to a PrimaryKeyIndexes instance, which abstracts access to DB and
    # storage. PrimaryKeyIndexes is a simple implementation of the abstract
    # Indexes which is likely not useful in practice but good for
    # demonstration. It allows us to load and query either everything, or
    # specific primary keys. See the other examples for more useful indexes
    # implementations.
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
    # We create a PostgresAccess (an implementation of the abstract DbAccess),
    # a LocalStorageTable (an implementation of the abstract StorageTable which
    # stores its data locally in Python data structures). Then we create a
    # CachedTable with DB access, storage, and the indexes to tie them
    # together. The table_name argument to LocalStorageTable is optional.
    db_access = tcpg.PostgresAccess(dsn=POSTGRES_DSN)
    storage_table = tcl.LocalStorageTable(indexes, table_name='users')
    table = tc.CachedTable(indexes, db_access, storage_table)
    async with db_access, asyncpg.create_pool(POSTGRES_DSN) as postgres_pool:
        await setup_example_db(postgres_pool)
        # We load data into the cache using the name of an index and arguments
        # specific to it. The PrimaryKeyIndexes have only the primary_key
        # index, and here we want to load everything. The arguments we pass
        # here are actually directly converted into a
        # PrimaryKeyIndexes.IndexSpec object, which we could also pass
        # directly. The same holds for some other methods in the CachedTable.
        await table.load('primary_key', all_primary_keys=True)
        # Since we've awaited load(), the table is now ready to use and we can
        # query it, hitting the cache rather than the DB. If we had started the
        # load in a background task, we could have used CachedTable.loaded() to
        # wait until it's ready (e.g. in a readiness check). Since we're
        # already loaded, this will return immediately.
        await table.loaded()
        print('Users with primary key 1 and 2:')
        async for user in table.get_records('primary_key', 1, 2):
            # We're querying the cache for records with primary keys 1 and 2
            # (the arguments are again turned into an IndexSpec which we could
            # have passed directly). Note that these are asyncpg.Record
            # objects. That's because the DB access returns those, and the
            # local storage just adds them to its data structures as-is. If we
            # wanted regular dicts, we could use the record_parser argument
            # when creating DbRecordsSpecs in a custom Indexes implementation.
            print(user)
        try:
            # In addition to the get_records() asynchronous generator, there is
            # also a get_first_record() convenience method that takes the same
            # arguments as get_records() would, and simply returns the first
            # record. If there isn't one, as is the case here, it raises a
            # KeyError. Since we loaded all records in the beginning, our
            # PrimaryKeyIndexes know that if this key isn't found in storage,
            # it's also not in the DB, so this doesn't cause the DB to be hit.
            print('Trying to find user with primary key 3:')
            await table.get_first_record('primary_key', 3)
        except KeyError:
            print('No user with primary key 3 exists.')
        # When data in the DB changes, the cache will still return the old
        # record. We need to invalidate it using invalidate_records(), which
        # can get a bit complicated in the general case. But here we can simply
        # pass an IndexSpec that gets the record we've updated. We need to pass
        # this twice (each in a list with one element). After we've invalidated
        # the record, the cache makes sure that the next time we query that
        # primary key, it is fetched from the DB first.
        await postgres_pool.execute(
            'UPDATE users SET name = $1 WHERE user_id = $2', 'New Name', 1)
        print('User 1 has changed in the DB, but not yet in cache:')
        print(await table.get_first_record('primary_key', 1))
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 1)],
            [indexes.IndexSpec('primary_key', 1)])
        print('After invalidating, a refresh is triggered to update user 1:')
        print(await table.get_first_record('primary_key', 1))
        # We can also invalidate records that were added to the DB after the
        # cache was loaded, which will cause them to be added to storage. This
        # works in our case, but there is another way of loading new records
        # (using adjust()) that may be preferable since it allows us to observe
        # every new record that was added. See the other examples.
        await postgres_pool.execute('''
            INSERT INTO users(user_id, name) VALUES(3, 'User 3');
            INSERT INTO users_cities(user_id, city_id)
            SELECT 3, city_id FROM cities LIMIT 1''')
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 3)],
            [indexes.IndexSpec('primary_key', 3)])
        # Normally, a refresh is triggered only when a read request for invalid
        # data comes in. But we can also trigger one manually to serve requests
        # from storage immediately.
        print('Manually triggering a refresh.')
        await table.refresh_invalid()
        print('Newly inserted user 3:')
        print(await table.get_first_record('primary_key', 3))


async def setup_example_db(postgres_pool):
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
