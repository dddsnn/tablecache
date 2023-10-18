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
import operator as op
import unittest.mock as um

from hamcrest import *
import pytest
import redis.asyncio as redis

import tablecache.storage as storage
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
async def conn(wait_for_redis, redis_host):
    conn = redis.Redis(host=redis_host)
    yield conn
    await conn.close()


async def collect_async_iter(i):
    l = []
    async for item in i:
        l.append(item)
    return l


class FailCodec(tc.Codec):
    def encode(self, _):
        raise Exception

    def decode(self, _):
        raise Exception


class MultiNumberRangeSubset(tc.Subset):
    def __init__(self, *interval_bounds):
        self._score_intervals = [
            tc.Interval(ge, lt) for ge, lt in interval_bounds]

    @property
    def score_intervals(self):
        return self._score_intervals

    @property
    def db_args(self):
        raise NotImplementedError


class TestRedisTable:
    @pytest.fixture(autouse=True)
    async def flush_db(self, conn):
        await conn.flushdb()

    @pytest.fixture
    def make_table(self, conn):
        def factory(
            table_name='table', primary_key_name='pk', attribute_codecs=None,
            score_function=op.itemgetter('pk')):
            attribute_codecs = attribute_codecs or {
                'pk': tc.IntAsStringCodec(), 's': tc.StringCodec()}
            return tc.RedisTable(
                conn, table_name=table_name, primary_key_name=primary_key_name,
                attribute_codecs=attribute_codecs,
                score_function=score_function)

        return factory

    @pytest.fixture
    def table(self, make_table):
        return make_table()

    async def test_construction_raises_on_non_str_attribute_name(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                attribute_codecs={
                    'pk': tc.IntAsStringCodec(), 1: tc.StringCodec()})

    async def test_construction_raises_if_primary_key_missing_from_codec(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                primary_key_name='pk',
                attribute_codecs={'s': tc.StringCodec()})

    async def test_put_and_get(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))

    async def test_put_and_get_multiple(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_put_ignores_extra_attributes(self, table):
        await table.put_record({'pk': 1, 's': 's1', 'x': 'x1'})
        assert_that(await table.get_record(1), is_not(has_entry('x', 'x1')))

    async def test_put_raises_on_missing_primary_key(self, table):
        with pytest.raises(ValueError):
            await table.put_record({'s': 's1'})

    async def test_put_raises_on_missing_attributes(self, table):
        with pytest.raises(ValueError):
            await table.put_record({'pk': 1})

    async def test_put_raises_on_primary_key_encoding_error(self, make_table):
        table = make_table(
            attribute_codecs={'pk': FailCodec(), 's': tc.StringCodec()})
        with pytest.raises(tc.CodingError):
            await table.put_record({'pk': 1, 's': 's1'})

    async def test_put_raises_on_attribute_encoding_error(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec(), 's': FailCodec()})
        with pytest.raises(tc.CodingError):
            await table.put_record({'pk': 1, 's': 's1'})

    async def test_put_raises_if_attribute_doesnt_encode_to_bytes(
            self, make_table):
        class BrokenStringReturningCodec(tc.Codec):
            def encode(self, _):
                return 'a string (supposed to be bytes)'

            def decode(self, _):
                raise Exception

        table = make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(),
                's': BrokenStringReturningCodec(),})
        with pytest.raises(tc.CodingError):
            await table.put_record({'pk': 1, 's': 's1'})

    async def test_put_removes_old_value(self, table, conn):
        assert await conn.zcard('table:rows') == 0
        await table.put_record({'pk': 1, 's': 'a'})
        await table.put_record({'pk': 1, 's': 'b'})
        await table.put_record({'pk': 1, 's': 'aaaaaaaaaaaaaaaaaaaaaaaa'})
        await table.put_record({'pk': 1, 's': 'bbbbbbbbbbbbbbbbbbbbbbbb'})
        assert await conn.zcard('table:rows') == 1
        await table.put_record({'pk': 1, 's': 'new'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='new'))
        assert await conn.zcard('table:rows') == 1

    async def test_put_doesnt_overwrite_other_records_with_same_score(
            self, make_table, conn):
        table = make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(), 'i': tc.IntAsStringCodec()},
            score_function=op.itemgetter('i'))
        await table.put_record({'pk': 1, 'i': 10})
        await table.put_record({'pk': 2, 'i': 10})
        await table.put_record({'pk': 1, 'i': 15})
        assert_that(await table.get_record(1), has_entries(pk=1, i=15))
        assert_that(await table.get_record(2), has_entries(pk=2, i=10))
        assert await conn.zcard('table:rows') == 2

    async def test_get_raises_on_nonexistent(self, table):
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_get_raises_on_primary_key_encoding_error(self, make_table):
        await make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(), 's': tc.StringCodec()}
        ).put_record({'pk': 1, 's': 's1'})
        table = make_table(
            attribute_codecs={'pk': FailCodec(), 's': tc.StringCodec()})
        with pytest.raises(tc.CodingError):
            await table.get_record(1)

    async def test_get_raises_on_missing_attributes(self, make_table):
        await make_table(attribute_codecs={'pk': tc.IntAsStringCodec()}
                         ).put_record({'pk': 1})
        table = make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(), 's': tc.StringCodec()})
        with pytest.raises(tc.CodingError):
            await table.get_record(1)

    async def test_get_raises_on_duplicate_attribute_ids(
            self, make_table, conn):
        table = make_table(score_function=lambda _: 0)
        await table.put_record({'pk': 1, 's': 's1'})
        row = (await conn.zrange('table:rows', 0, -1))[0]
        s_id_index = row.index(b's1') - 3
        assert int.from_bytes(row[s_id_index + 1:s_id_index + 3]) == 2
        s_id = row[s_id_index:s_id_index + 1]
        false_row = row + s_id + (6).to_bytes(length=2) + b'foobar'
        await conn.delete('table:rows')
        await conn.zadd('table:rows', {false_row: 0})
        with pytest.raises(tc.CodingError):
            await table.get_record(1)

    async def test_get_raises_on_attribute_decoding_error(self, make_table):
        await make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(), 's': tc.StringCodec()}
        ).put_record({'pk': 1, 's': 's1'})
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec(), 's': FailCodec()})
        with pytest.raises(tc.CodingError):
            await table.get_record(1)

    async def test_uses_custom_codec(self, make_table, conn):
        class WeirdTupleCodec(tc.Codec):
            def encode(self, t):
                return repr(t).encode()

            def decode(self, bs):
                i, s = eval(bs.decode())
                return (i + 1, f'{s} with an addition')

        table = make_table(
            attribute_codecs={
                'pk': tc.IntAsStringCodec(), 't': WeirdTupleCodec()})
        await table.put_record({'pk': 1, 't': (5, 'x')})
        assert_that(
            await table.get_record(1),
            has_entries(t=(6, 'x with an addition')))

    async def test_clear(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.get_record(1)
        await table.clear()
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_multiple_tables(self, make_table):
        table1 = make_table(table_name='t1')
        table2 = make_table(table_name='t2')
        await table1.put_record({'pk': 1, 's': 's1'})
        await table1.put_record({'pk': 2, 's': 's2'})
        await table2.put_record({'pk': 1, 's': 's3'})
        assert_that(await table1.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table1.get_record(2), has_entries(pk=2, s='s2'))
        assert_that(await table2.get_record(1), has_entries(pk=1, s='s3'))
        with pytest.raises(KeyError):
            await table2.get_record(2)

    async def test_clear_only_deletes_own_keys(self, make_table):
        table1 = make_table(table_name='t1')
        table2 = make_table(table_name='t2')
        await table1.put_record({'pk': 1, 's': 's1'})
        await table2.put_record({'pk': 1, 's': 's2'})
        await table1.clear()
        with pytest.raises(KeyError):
            await table1.get_record(1)
        assert_that(await table2.get_record(1), has_entries(pk=1, s='s2'))

    async def test_get_record_subset_on_empty(self, table):
        assert_that(
            await collect_async_iter(
                table.get_record_subset(tc.All.with_primary_key('pk')())),
            empty())

    async def test_get_record_subset_on_one_record(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-50, 51))),
            contains_inanyorder(has_entries(pk=0)))

    async def test_get_record_subset_on_all_contained(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': -50})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 50})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-50, 51))),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=0), has_entries(pk=50)))

    async def test_get_record_subset_on_some_not_contained(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': -50})
        await table.put_record({'pk': -10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 49})
        await table.put_record({'pk': 50})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 50))),
            contains_inanyorder(
                has_entries(pk=-10), has_entries(pk=0), has_entries(pk=49)))

    async def test_get_record_subset_on_none_contained(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 50})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(100, 200))),
            empty())

    async def test_get_record_subset_with_inf_bounds(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 50})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(
                        float('-inf'), float('inf')))),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=50)))

    async def test_get_record_subset_with_multiple_intervals(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': -50})
        await table.put_record({'pk': -10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 49})
        await table.put_record({'pk': 50})
        await table.put_record({'pk': 60})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    MultiNumberRangeSubset((-10, 5), (40, 51)))),
            contains_inanyorder(
                has_entries(pk=-10), has_entries(pk=0), has_entries(pk=49),
                has_entries(pk=50)))

    async def test_get_record_subset_uses_custom_score_function(
            self, make_table):
        def pk_minus_10(record):
            return record['pk'] - 10

        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=pk_minus_10)
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 59})
        await table.put_record({'pk': 60})
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(0, 50))),
            contains_inanyorder(has_entries(pk=10), has_entries(pk=59)))

    async def test_delete_record_raises_on_nonexistent(self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        with pytest.raises(KeyError):
            await table.delete_record(1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 10))),
            contains_inanyorder(has_entries(pk=0)))

    async def test_delete_record_deletes(self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.delete_record(1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 10))),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=2)))

    async def test_delete_record_subset_on_empty(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        assert await table.delete_record_subset([tc.Interval(0, 50)]) == 0
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 51))),
            empty())

    async def test_delete_record_subset_deletes_nothing(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        assert await table.delete_record_subset([]) == 0
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 51))),
            contains_inanyorder(has_entries(pk=0)))

    async def test_delete_record_subset_deletes_all(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 49})
        assert await table.delete_record_subset([tc.Interval(0, 50)]) == 3
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 51))),
            empty())

    async def test_delete_record_subset_deletes_some(self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': -1})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 50})
        await table.put_record({'pk': 51})
        assert await table.delete_record_subset([tc.Interval(0, 51)]) == 2
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-100, 100))),
            contains_inanyorder(has_entries(pk=-1), has_entries(pk=51)))

    async def test_delete_record_subset_deletes_multiple_intervals(
            self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': -50})
        await table.put_record({'pk': -20})
        await table.put_record({'pk': -10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 49})
        num_deleted = await table.delete_record_subset([
            tc.Interval(-40, -9), tc.Interval(10, 11)])
        assert num_deleted == 3
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-100, 100))),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=0), has_entries(pk=49)))

    async def test_delete_record_subset_deletes_overlapping_intervals(
            self, make_table):
        table = make_table(
            attribute_codecs={'pk': tc.IntAsStringCodec()},
            score_function=op.itemgetter('pk'))
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 20})
        await table.put_record({'pk': 30})
        await table.put_record({'pk': 40})
        num_deleted = await table.delete_record_subset([
            tc.Interval(5, 25), tc.Interval(15, 35)])
        assert num_deleted == 3
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-100, 100))),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=40)))

    async def test_delete_record_raises_if_deleted_in_subset_previously(
            self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.delete_record_subset([tc.Interval(1, 10)])
        with pytest.raises(KeyError):
            await table.delete_record(1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(-10, 10))),
            contains_inanyorder(has_entries(pk=0)))


class TestAttributeIdMap:
    def test_on_empty(self):
        d = storage.AttributeIdMap({})
        assert_that(list(d), empty())
        assert_that(list(d.attribute_names), empty())

    def test_single_item(self):
        d = storage.AttributeIdMap({'a': 1})
        assert_that(
            d,
            contains_inanyorder(contains_exactly('a', instance_of(bytes), 1)))

    def test_maps_entire_original(self):
        original = {'a': um.Mock(), 'b': um.Mock(), '': um.Mock()}
        d = storage.AttributeIdMap(original)
        ids = set()
        for n, i, v in d:
            assert isinstance(i, bytes)
            ids.add(i)
            assert original[n] is v
            assert d[i] == (n, v)
        assert len(ids) == len(original)

    def test_uses_small_ids(self):
        d = storage.AttributeIdMap({
            'x': um.Mock(), 10 * 'x': um.Mock(), 100 * 'x': um.Mock()})
        for _, i, _ in d:
            assert len(i) == 1

    def test_uses_fixed_length_ids(self):
        d = storage.AttributeIdMap({n * 'x': n for n in range(300)})
        assert all(len(i) == 2 for _, i, _ in d)

    def test_getitem_raises_on_nonexistent(self):
        d = storage.AttributeIdMap({'a': 1})
        attribute_id = next(i for _, i, _ in d)
        with pytest.raises(KeyError):
            d[attribute_id + b'\x00']

    def test_provides_attribute_names(self):
        original = {'a': 1, 'b': 1, '': 1}
        d = storage.AttributeIdMap(original)
        assert frozenset(original) == d.attribute_names


class TestRowCodec:
    @pytest.fixture
    def make_codec(self):
        def factory(attribute_codecs=None):
            attribute_codecs = attribute_codecs or {
                'i': tc.IntAsStringCodec(), 's': tc.StringCodec()}
            return storage.RowCodec(attribute_codecs)

        return factory

    @pytest.fixture
    def codec(self, make_codec):
        return make_codec()

    def test_encode_decode(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1'}))
        assert_that(codec.decode(encoded), has_entries(i=1, s='s1'))

    def test_encode_ignores_extra_attributes(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1', 'x': 'x1'}))
        assert_that(codec.decode(encoded), is_not(has_entry('x', 'x1')))

    def test_encode_raises_on_missing_attributes(self, codec):
        with pytest.raises(ValueError):
            codec.encode({'i': 1})

    def test_encode_raises_on_attribute_encoding_error(self, make_codec):
        codec = make_codec({'i': tc.IntAsStringCodec(), 's': FailCodec()})
        with pytest.raises(tc.CodingError):
            codec.encode({'i': 1, 's': 's1'})

    def test_encode_raises_if_attribute_doesnt_encode_to_bytes(
            self, make_codec):
        class BrokenStringReturningCodec(tc.Codec):
            def encode(self, _):
                return 'a string (supposed to be bytes)'

            def decode(self, _):
                raise Exception

        codec = make_codec({
            'i': tc.IntAsStringCodec(),
            's': BrokenStringReturningCodec(),})
        with pytest.raises(tc.CodingError):
            codec.encode({'i': 1, 's': 's1'})

    def test_encode_raises_if_encoded_attribute_is_too_long(self, make_codec):
        class LargeValueCodec(tc.Codec):
            def encode(self, length):
                return length * b'x'

            def decode(self, _):
                raise Exception

        codec = make_codec({'l': LargeValueCodec()})
        with pytest.raises(tc.CodingError):
            codec.encode({'l': codec.max_attribute_length + 1})

    def test_decode_raises_on_non_bytes_arg(self, codec):
        with pytest.raises(ValueError):
            codec.decode('not bytes')

    def test_decode_raises_on_missing_attributes(self, make_codec):
        encoded = bytes(
            make_codec({'i': tc.IntAsStringCodec()}).encode({'i': 1}))
        codec = make_codec({'i': tc.IntAsStringCodec(), 's': tc.StringCodec()})
        with pytest.raises(tc.CodingError):
            codec.decode(encoded)

    def test_decode_raises_on_duplicate_attribute_ids(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1'}))
        s_id_index = encoded.index(b's1') - 3
        assert int.from_bytes(encoded[s_id_index + 1:s_id_index + 3]) == 2
        s_id = encoded[s_id_index:s_id_index + 1]
        false_encoded = bytes(
            encoded + s_id + (6).to_bytes(length=2) + b'foobar')
        assert_that(codec.decode(false_encoded[:-9]), has_entries(i=1, s='s1'))
        with pytest.raises(tc.CodingError):
            codec.decode(false_encoded)

    def test_decode_raises_on_attribute_decoding_error(self, make_codec):
        encoded = bytes(
            make_codec({'s': tc.StringCodec()}).encode({'s': 's1'}))
        codec = make_codec({'s': FailCodec()})
        with pytest.raises(tc.CodingError):
            codec.decode(encoded)

    def test_decode_raises_on_unknown_attribute_ids(self, make_codec):
        codec = make_codec({'s': tc.StringCodec()})
        encoded = bytes(codec.encode({'s': 's1'}))
        assert len(encoded) == 5 and encoded.endswith(b's1')
        false_encoded = encoded + bytes([encoded[0] + 1]) + encoded[1:]
        assert_that(codec.decode(false_encoded[:5]), has_entries(s='s1'))
        with pytest.raises(tc.CodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_value_ends_unexpectedly(
            self, make_codec):
        codec = make_codec({'s': tc.StringCodec()})
        encoded = bytes(codec.encode({'s': 'foobar'}))
        assert len(encoded) == 9 and encoded.endswith(b'foobar')
        assert int.from_bytes(encoded[1:3]) == 6
        false_encoded = encoded[:-1]
        with pytest.raises(tc.CodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_header_ends_after_id(self, make_codec):
        codec = make_codec({'s1': tc.StringCodec(), 's2': tc.StringCodec()})
        encoded = bytes(codec.encode({'s1': 's1', 's2': 's2'}))
        assert len(encoded) == 10 and encoded[3:4] == encoded[8:9] == b's'
        false_encoded = encoded[:6]
        with pytest.raises(tc.CodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_header_ends_in_the_middle_of_length(
            self, make_codec):
        codec = make_codec({'s1': tc.StringCodec(), 's2': tc.StringCodec()})
        encoded = bytes(codec.encode({'s1': 's1', 's2': 's2'}))
        assert len(encoded) == 10 and encoded[3:4] == encoded[8:9] == b's'
        false_encoded = encoded[:7]
        with pytest.raises(tc.CodingError):
            codec.decode(false_encoded)


class TestBytesReader:
    def test_on_empty(self):
        reader = storage.BytesReader(b'')
        assert reader.bytes_remaining == 0
        assert reader.read(0) == b''
        with pytest.raises(storage.BytesReader.NotEnoughBytes):
            reader.read(1)

    def test_read(self):
        reader = storage.BytesReader(b'foobar')
        assert reader.bytes_remaining == 6
        assert reader.read(0) == b''
        assert reader.bytes_remaining == 6
        assert reader.read(2) == b'fo'
        assert reader.bytes_remaining == 4
        assert reader.read(4) == b'obar'
        with pytest.raises(storage.BytesReader.NotEnoughBytes):
            reader.read(1)
