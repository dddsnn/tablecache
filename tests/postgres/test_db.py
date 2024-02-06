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

import asyncpg
from hamcrest import *
import pytest

import tablecache as tc
import tablecache.postgres as tcp


@pytest.fixture(scope='session')
def postgres_host():
    import socket
    try:
        return socket.gethostbyname('postgres')
    except socket.gaierror:
        return 'localhost'


@pytest.fixture(scope='session')
def postgres_dsn(postgres_host):
    return f'postgres://postgres:@{postgres_host}:5432/postgres'


@pytest.fixture(scope='session')
async def wait_for_postgres(postgres_dsn):
    start_time = asyncio.get_running_loop().time()
    while True:
        try:
            conn = await asyncpg.connect(dsn=postgres_dsn)
            await conn.close()
            return
        except Exception as e:
            if asyncio.get_running_loop().time() > start_time + 60:
                raise Exception('Testing Postgres isn\'t coming up.') from e
            await asyncio.sleep(0.1)


@pytest.fixture(scope='session')
async def pool(wait_for_postgres, postgres_dsn):
    async with asyncpg.create_pool(dsn=postgres_dsn) as pool:
        yield pool


@pytest.fixture(autouse=True)
async def setup_db(pool):
    await pool.execute(
        '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE users (
            user_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            name text NOT NULL,
            age integer
        );
        CREATE TABLE cities (
            city_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            name text NOT NULL
        );
        CREATE TABLE users_cities (
            user_id integer PRIMARY KEY REFERENCES users(user_id),
            city_id integer NOT NULL REFERENCES cities(city_id)
        );''')


@pytest.fixture
def insert_user(pool):
    async def inserter(user_id, user_name, user_age, city_id, city_name):
        await pool.execute(
            '''INSERT INTO users (user_id, name, age)
            VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING''', user_id,
            user_name, user_age)
        await pool.execute(
            '''INSERT INTO cities (city_id, name)
            VALUES ($1, $2) ON CONFLICT (city_id) DO NOTHING''', city_id,
            city_name)
        await pool.execute(
            '''INSERT INTO users_cities (user_id, city_id)
            VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING''', user_id,
            city_id)

    return inserter


async def collect_async_iter(i):
    ls = []
    async for item in i:
        ls.append(item)
    return ls


class TestPostgresAccess:
    def query_spec(self, *user_ids):
        query_string = '''
            SELECT
                uc.*, u.name AS user_name, u.age AS user_age,
                c.name AS city_name
            FROM
                users u
                JOIN users_cities uc USING (user_id)
                JOIN cities c USING (city_id)'''
        if user_ids:
            query_string += ' WHERE uc.user_id IN ({})'.format(
                ','.join(f'${i}' for i in range(1, len(user_ids) + 1)))
        return tc.QueryArgsDbRecordsSpec(query_string, tuple(user_ids))

    @pytest.fixture
    async def access(self, wait_for_postgres, postgres_dsn):
        async with tcp.PostgresAccess(dsn=postgres_dsn) as access:
            yield access

    async def test_get_records_on_empty(self, access):
        assert_that(
            await collect_async_iter(access.get_records(self.query_spec())),
            empty())

    async def test_get_records_on_one(self, access, insert_user):
        await insert_user(1, 'u1', 1, 11, 'c1')
        assert_that(
            await collect_async_iter(access.get_records(self.query_spec())),
            contains_inanyorder(
                has_entries(
                    user_id=1, user_name='u1', user_age=1, city_id=11,
                    city_name='c1')))

    async def test_get_records_on_many(self, access, insert_user):
        await insert_user(1, 'u1', 1, 11, 'c1')
        await insert_user(2, 'u2', None, 11, 'c1')
        await insert_user(3, 'u3', 3, 12, 'c2')
        assert_that(
            await collect_async_iter(access.get_records(self.query_spec())),
            contains_inanyorder(
                has_entries(
                    user_id=1, user_name='u1', user_age=1, city_id=11,
                    city_name='c1'),
                has_entries(
                    user_id=2, user_name='u2', user_age=None, city_id=11,
                    city_name='c1'),
                has_entries(
                    user_id=3, user_name='u3', user_age=3, city_id=12,
                    city_name='c2')))

    async def test_get_records_with_args(self, access, insert_user):
        await insert_user(1, 'u1', 1, 11, 'c1')
        await insert_user(2, 'u2', None, 11, 'c1')
        await insert_user(3, 'u3', 3, 12, 'c2')
        assert_that(
            await collect_async_iter(
                access.get_records(self.query_spec(2, 3))),
            contains_inanyorder(
                has_entries(user_id=2), has_entries(user_id=3)))
