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

from hamcrest import *
import pytest
import redis.asyncio as redis

import tablecache as tc


@pytest.fixture(scope='session')
def redis_host():
    import socket
    try:
        return socket.gethostbyname('redis')
    except socket.gaierror:
        return 'localhost'


@pytest.fixture(scope='session')
async def wait_for_redis(redis_host):
    start_time = asyncio.get_running_loop().time()
    while True:
        try:
            conn = redis.Redis(host=redis_host)
            await conn.ping()
            await conn.close(close_connection_pool=True)
            return
        except Exception as e:
            if asyncio.get_running_loop().time() > start_time + 60:
                raise Exception('Testing Redis isn\'t coming up.') from e
            await asyncio.sleep(0.1)


@pytest.fixture(scope='session')
async def redis_storage(wait_for_redis, redis_host):
    async with tc.RedisStorage(host=redis_host) as redis_storage:
        yield redis_storage


class TestRedisTable:
    @pytest.fixture(autouse=True)
    async def flush_db(self, redis_storage):
        await redis_storage.conn.flushdb()

    @pytest.fixture
    def make_table(self, redis_storage):
        def factory(
                primary_key_name='pk', encoders=None, decoders=None,
                primary_key_encoder=str):
            encoders = encoders or {
                'pk': tc.encode_int_as_str, 's': tc.encode_str}
            decoders = decoders or {
                'pk': tc.decode_int_as_str, 's': tc.decode_str}
            return tc.RedisTable(
                redis_storage, primary_key_name=primary_key_name,
                encoders=encoders, decoders=decoders,
                primary_key_encoder=primary_key_encoder)

        return factory

    @pytest.fixture
    def table(self, make_table):
        return make_table()

    async def test_construction_raises_if_codecs_dont_match(self, make_table):
        with pytest.raises(ValueError):
            make_table(
                encoders={'pk': tc.encode_int_as_str, 's1': tc.encode_str},
                decoders={'pk': tc.decode_int_as_str, 's2': tc.decode_str})

    async def test_construction_raises_on_non_str_attribute_name(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                encoders={'pk': tc.encode_int_as_str, 1: tc.encode_str},
                decoders={'pk': tc.decode_int_as_str, 1: tc.decode_str})

    async def test_construction_raises_if_primary_key_missing_from_codec(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                primary_key_name='pk', encoders={'s': tc.encode_str},
                decoders={'s': tc.decode_str})

    async def test_put_and_get(self, table):
        await table.put({'pk': 1, 's': 's1'})
        assert_that(await table.get(1), has_entries(pk=1, s='s1'))

    async def test_put_ignores_extra_attributes(self, table):
        await table.put({'pk': 1, 's': 's1', 'x': 'x1'})
        assert_that(await table.get(1), is_not(has_entry('x', 'x1')))

    async def test_put_raises_on_missing_primary_key(self, table):
        with pytest.raises(ValueError):
            await table.put({'s': 's1'})

    async def test_put_raises_on_missing_attributes(self, table):
        with pytest.raises(ValueError):
            await table.put({'pk': 1})

    async def test_put_raises_on_primary_key_encoding_error(self, make_table):
        def fail(*_):
            raise Exception

        table = make_table(primary_key_encoder=fail)
        with pytest.raises(tc.CodingError):
            await table.put({'pk': 1, 's': 's1'})

    async def test_put_raises_if_primary_key_doesnt_encode_to_str(
            self, make_table):
        def return_int(*_):
            return 1

        table = make_table(primary_key_encoder=return_int)
        with pytest.raises(tc.CodingError):
            await table.put({'pk': 1, 's': 's1'})

    async def test_put_raises_on_attribute_encoding_error(self, make_table):
        def fail(*_):
            raise Exception

        table = make_table(encoders={'pk': tc.encode_int_as_str, 's': fail})
        with pytest.raises(tc.CodingError):
            await table.put({'pk': 1, 's': 's1'})

    async def test_put_raises_if_attribute_doesnt_encode_to_bytes(
            self, make_table):
        def return_str(*_):
            return 'some string'

        table = make_table(
            encoders={'pk': tc.encode_int_as_str, 's': return_str})
        with pytest.raises(tc.CodingError):
            await table.put({'pk': 1, 's': 's1'})

    async def test_get_raises_on_nonexistent(self, table):
        with pytest.raises(KeyError):
            await table.get(1)

    async def test_get_raises_on_primary_key_encoding_error(self, make_table):
        def fail(*_):
            raise Exception

        table = make_table(primary_key_encoder=fail)
        with pytest.raises(tc.CodingError):
            await table.get(1)

    async def test_get_raises_if_primary_key_doesnt_encode_to_str(
            self, make_table):
        def return_int(*_):
            return 1

        table = make_table(primary_key_encoder=return_int)
        with pytest.raises(tc.CodingError):
            await table.get(1)

    async def test_get_raises_on_nonexistent(self, table):
        with pytest.raises(KeyError):
            await table.get(1)

    async def test_get_raises_on_missing_attributes(self, make_table):
        await make_table(
            encoders={'pk': tc.encode_int_as_str},
            decoders={'pk': tc.decode_int_as_str}).put({'pk': 1})
        table = make_table(
            encoders={'pk': tc.encode_int_as_str, 's': tc.encode_str})
        with pytest.raises(ValueError):
            await table.get(1)

    async def test_get_raises_on_attribute_decoding_error(self, make_table):
        def fail(*_):
            raise Exception

        table = make_table(decoders={'pk': tc.decode_int_as_str, 's': fail})
        await table.put({'pk': 1, 's': 's1'})
        with pytest.raises(tc.CodingError):
            await table.get(1)

    async def test_uses_custom_primary_key_encoder(
            self, make_table, redis_storage):
        def custom_pk(i):
            return f'the number {i}'

        table = make_table(primary_key_encoder=custom_pk)
        await table.put({'pk': 1, 's': 's1'})
        assert_that(await table.get(1), has_entries(pk=1, s='s1'))
        assert_that(
            await redis_storage.conn.hgetall('the number 1'),
            has_entries({b'pk': b'1', b's': b's1'}))

    async def test_uses_custom_codec(self, make_table, redis_storage):
        def encode_tuple(t):
            return repr(t).encode()

        def decode_tuple_but_add(bs):
            i, s = eval(bs.decode())
            return (i + 1, f'{s} with an addition')

        table = make_table(
            encoders={'pk': tc.encode_int_as_str, 't': encode_tuple},
            decoders={'pk': tc.decode_int_as_str, 't': decode_tuple_but_add})
        await table.put({'pk': 1, 't': (5, 'x')})
        assert_that(
            await table.get(1), has_entries(t=(6, 'x with an addition')))

    async def test_clear(self, table):
        await table.put({'pk': 1, 's': 's1'})
        await table.get(1)
        await table.clear()
        with pytest.raises(KeyError):
            await table.get(1)
