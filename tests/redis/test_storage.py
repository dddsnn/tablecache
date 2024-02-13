# Copyright 2023, 2024 Marc Lehmann

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
import functools as ft
import unittest.mock as um

from hamcrest import *
import pytest
import redis.asyncio as redis

import tablecache as tc
import tablecache.redis as tcr
import tablecache.redis.storage as storage

_inf_to_inf = tc.Interval(float('-inf'), float('inf'))


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
            await asyncio.wait_for(conn.ping(), 1)
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
    ls = []
    async for item in i:
        ls.append(item)
    return ls


def filter_kwarg(kwarg_name):
    def filter(**kwargs):
        return kwargs[kwarg_name]
    return filter


class FailCodec(tcr.Codec):
    def encode(self, _):
        raise Exception

    def decode(self, _):
        raise Exception


class ManuallyUnlockedScratchSpace(storage.ScratchSpace):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.update_not_merging()

    def update_not_merging(self):
        self._manually_not_merging = super().is_not_merging()

    def is_not_merging(self):
        return self._manually_not_merging


class TestRedisTable:
    @pytest.fixture
    def tables(self):
        return []

    @pytest.fixture(autouse=True)
    async def flush_db(self, conn):
        await conn.flushdb()

    @pytest.fixture(autouse=True)
    async def let_background_merges_finish(self, scratch_space_is_not_merging):
        yield
        await scratch_space_is_not_merging()

    @pytest.fixture
    def make_table(self, conn, tables):
        def factory(
                table_name='table', primary_key_name='pk',
                attribute_codecs=None, score_functions=None):
            attribute_codecs = attribute_codecs or {
                'pk': tcr.IntAsStringCodec(), 's': tcr.StringCodec()}
            score_functions = score_functions or {
                'primary_key': filter_kwarg(primary_key_name)}
            table = tcr.RedisTable(
                conn, table_name=table_name, primary_key_name=primary_key_name,
                attribute_codecs=attribute_codecs,
                score_functions=score_functions)
            tables.append(table)
            return table

        return factory

    @pytest.fixture
    def table(self, make_table):
        return make_table()

    @pytest.fixture
    def scratch_space_is_in_condition(self, tables):
        async def checker(condition_predicate_name):
            for table in tables:
                predicate = getattr(
                    table._scratch_space, condition_predicate_name)
                async with table._scratch_condition:
                    await table._scratch_condition.wait_for(predicate)
        return checker

    @pytest.fixture
    def scratch_space_is_not_merging(self, scratch_space_is_in_condition):
        return ft.partial(scratch_space_is_in_condition, 'is_not_merging')

    @pytest.fixture
    def scratch_space_is_clear(self, scratch_space_is_in_condition):
        return ft.partial(scratch_space_is_in_condition, 'is_clear')

    async def test_construction_raises_on_non_str_attribute_name(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                attribute_codecs={
                    'pk': tcr.IntAsStringCodec(), 1: tcr.StringCodec()})

    async def test_construction_raises_if_primary_key_missing_from_codec(
            self, make_table):
        with pytest.raises(ValueError):
            make_table(
                primary_key_name='pk',
                attribute_codecs={'s': tcr.StringCodec()})

    async def test_put_record_get_record(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))

    async def test_put_record_get_record_multiple(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_put_record_ignores_extra_attributes(self, table):
        await table.put_record({'pk': 1, 's': 's1', 'x': 'x1'})
        assert_that(await table.get_record(1), is_not(has_entry('x', 'x1')))

    async def test_put_record_raises_on_missing_primary_key(self, table):
        with pytest.raises(ValueError):
            await table.put_record({'s': 's1'})

    async def test_put_record_raises_on_missing_attributes(self, table):
        with pytest.raises(ValueError):
            await table.put_record({'pk': 1})

    @pytest.mark.parametrize('fail_attribute', ['pk', 's'])
    async def test_put_record_raises_on_attribute_encoding_error(
            self, make_table, fail_attribute):
        attribute_codecs = {
            'pk': tcr.IntAsStringCodec(), 's': tcr.StringCodec()}
        attribute_codecs[fail_attribute] = FailCodec()
        table = make_table(attribute_codecs=attribute_codecs)
        with pytest.raises(tcr.RedisCodingError):
            await table.put_record({'pk': 1, 's': 's1'})

    async def test_put_record_raises_if_attribute_doesnt_encode_to_bytes(
            self, make_table):
        class BrokenStringReturningCodec(tcr.Codec):
            def encode(self, _):
                return 'a string (supposed to be bytes)'

            def decode(self, _):
                raise Exception

        table = make_table(
            attribute_codecs={
                'pk': tcr.IntAsStringCodec(),
                's': BrokenStringReturningCodec(), })
        with pytest.raises(tcr.RedisCodingError):
            await table.put_record({'pk': 1, 's': 's1'})

    async def test_put_record_overwrites_old_value_with_same_primary_key(
            self, table, conn):
        assert await conn.zcard('table:primary_key') == 0
        await table.put_record({'pk': 1, 's': 'a'})
        await table.put_record({'pk': 1, 's': 'b'})
        await table.put_record({'pk': 1, 's': 'aaaaaaaaaaaaaaaaaaaaaaaa'})
        await table.put_record({'pk': 1, 's': 'bbbbbbbbbbbbbbbbbbbbbbbb'})
        assert await conn.zcard('table:primary_key') == 1
        await table.put_record({'pk': 1, 's': 'new'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='new'))
        assert await conn.zcard('table:primary_key') == 1

    async def test_put_record_overwrites_old_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.put_record({'pk': 0, 's': 'dzzz'})
        assert await conn.zcard('table:first_char') == 1

    async def test_put_record_doesnt_overwrite_other_records_in_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 2
        await table.put_record({'pk': 0, 's': 'fzzz'})
        assert await conn.zcard('table:first_char') == 2
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='czzz')))

    async def test_put_record_doesnt_overwrite_others_with_same_pk_score(
            self, make_table, conn):
        table = make_table(
            score_functions={'primary_key': lambda **kwargs: kwargs['pk'] % 2})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 3, 's': 's3'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table.get_record(3), has_entries(pk=3, s='s3'))
        scores = {
            s for (_, s) in
            await conn.zrange(
                'table:primary_key', 0, 3, byscore=True, withscores=True)}
        assert len(scores) == 1

    async def test_put_record_doesnt_overwrite_others_with_same_scores(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        await table.put_record({'pk': 4, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.put_record({'pk': 2, 's': 'fzzz'})
        assert await conn.zcard('table:first_char') == 2
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))],
                lambda r: r['s'][0] == 'c'))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=0, s='czzz'), has_entries(pk=4, s='czzz')))

    async def test_put_record_handles_equal_scores_and_counts_in_other_index(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.put_record({'pk': 0, 's': 'fzzz'})
        assert await conn.zcard('table:first_char') == 2
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))],
                lambda r: r['s'][0] == 'c'))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=2, s='czzz')))

    async def test_get_record_raises_on_nonexistent(self, table):
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_get_record_raises_on_missing_attributes(self, make_table):
        table = make_table(attribute_codecs={'pk': tcr.IntAsStringCodec()})
        await table.put_record({'pk': 1})
        table = make_table(
            attribute_codecs={
                'pk': tcr.IntAsStringCodec(), 's': tcr.StringCodec()})
        with pytest.raises(tcr.RedisCodingError):
            await table.get_record(1)

    async def test_get_record_raises_on_duplicate_attribute_ids(
            self, make_table, conn):
        table = make_table(score_functions={'primary_key': lambda **_: 0})
        await table.put_record({'pk': 1, 's': 's1'})
        row = (await conn.zrange('table:primary_key', 0, -1))[0]
        s_id_index = row.index(b's1') - 3
        assert int.from_bytes(row[s_id_index + 1:s_id_index + 3]) == 2
        s_id = row[s_id_index:s_id_index + 1]
        false_row = row + s_id + (6).to_bytes(length=2) + b'foobar'
        await conn.delete('table:primary_key')
        await conn.zadd('table:primary_key', {false_row: 0})
        with pytest.raises(tcr.RedisCodingError):
            await table.get_record(1)

    async def test_get_record_raises_on_attribute_decoding_error(
            self, make_table):
        table = make_table(
            attribute_codecs={
                'pk': tcr.IntAsStringCodec(), 's': tcr.StringCodec()})
        await table.put_record({'pk': 1, 's': 's1'})
        table = make_table(
            attribute_codecs={'pk': tcr.IntAsStringCodec(), 's': FailCodec()})
        with pytest.raises(tcr.RedisCodingError):
            await table.get_record(1)

    async def test_get_record_handles_records_with_equal_scores(
            self, make_table):
        table = make_table(
            score_functions={'primary_key': lambda **r: r['pk'] % 2})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 2, 's': 's'})
        assert_that(await table.get_record(0), has_entries(pk=0))
        assert_that(await table.get_record(2), has_entries(pk=2))

    async def test_uses_custom_codec(self, make_table):
        class WeirdTupleCodec(tcr.Codec):
            def encode(self, t):
                return repr(t).encode()

            def decode(self, bs):
                i, s = eval(bs.decode())
                return (i + 1, f'{s} with an addition')

        table = make_table(
            attribute_codecs={
                'pk': tcr.IntAsStringCodec(), 't': WeirdTupleCodec()})
        await table.put_record({'pk': 1, 't': (5, 'x')})
        assert_that(
            await table.get_record(1),
            has_entries(t=(6, 'x with an addition')))

    async def test_clear(self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.get_record(1)
        assert await conn.zcard('table:primary_key') > 0
        assert await conn.zcard('table:first_char') > 0
        await table.clear()
        with pytest.raises(KeyError):
            await table.get_record(1)
        assert await conn.zcard('table:primary_key') == 0
        assert await conn.zcard('table:first_char') == 0

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

    async def test_get_records_on_no_intervals(self, table):
        assert_that(
            await collect_async_iter(
                table.get_records(tc.StorageRecordsSpec('primary_key', []))),
            empty())

    async def test_get_records_on_empty(self, table):
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(await collect_async_iter(records), empty())

    async def test_get_records_on_one_record(self, table):
        await table.put_record({'pk': 0, 's': 's1'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0, s='s1')))

    async def test_get_records_on_all_contained(self, table):
        await table.put_record({'pk': -50, 's': 's1'})
        await table.put_record({'pk': 0, 's': 's2'})
        await table.put_record({'pk': 50, 's': 's3'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-50, s='s1'), has_entries(pk=0, s='s2'),
                has_entries(pk=50, s='s3')))

    async def test_get_records_on_some_not_contained(self, table):
        await table.put_record({'pk': -50, 's': 's'})
        await table.put_record({'pk': -10, 's': 's'})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 49, 's': 's'})
        await table.put_record({'pk': 50, 's': 's'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(-10, 50)]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-10), has_entries(pk=0), has_entries(pk=49)))

    async def test_get_records_on_none_contained(self, table):
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 50, 's': 's'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(100, 150)]))
        assert_that(await collect_async_iter(records), empty())

    async def test_get_records_with_multiple_intervals(self, table):
        await table.put_record({'pk': -50, 's': 's'})
        await table.put_record({'pk': -10, 's': 's'})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 10, 's': 's'})
        await table.put_record({'pk': 49, 's': 's'})
        await table.put_record({'pk': 50, 's': 's'})
        await table.put_record({'pk': 60, 's': 's'})
        records = table.get_records(
            tc.StorageRecordsSpec(
                'primary_key', [tc.Interval(-10, 5), tc.Interval(40, 51)]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-10), has_entries(pk=0), has_entries(pk=49),
                has_entries(pk=50)))

    async def test_get_records_uses_custom_score_function(
            self, make_table):
        def pk_minus_10(**kwargs):
            return kwargs['pk'] - 10

        table = make_table(
            attribute_codecs={'pk': tcr.IntAsStringCodec()},
            score_functions={'primary_key': pk_minus_10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 59})
        await table.put_record({'pk': 60})
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(0, 50)]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=10), has_entries(pk=59)))

    async def test_get_records_uses_recheck_predicate(self, table):
        def x_in_s(record):
            return 'x' in record['s']
        await table.put_record({'pk': 0, 's': 'aaa'})
        await table.put_record({'pk': 1, 's': 'bxb'})
        await table.put_record({'pk': 2, 's': 'cxc'})
        await table.put_record({'pk': 3, 's': 'ddd'})
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf], x_in_s))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1), has_entries(pk=2)))

    async def test_get_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'haaa'})
        await table.put_record({'pk': 3, 's': 'iaaa'})
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('d'), ord('i'))]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1), has_entries(pk=2)))

    async def test_get_records_works_with_equal_primary_key_scores_in_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'cyyy'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=0, s='cyyy'), has_entries(pk=2, s='czzz')))

    async def test_delete_record_raises_on_nonexistent(self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        with pytest.raises(KeyError):
            await table.delete_record(1)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0)))

    async def test_delete_record_deletes(self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.delete_record(1)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=2)))

    async def test_delete_record_doesnt_delete_other_records_with_same_score(
            self, make_table):
        table = make_table(
            score_functions={'primary_key': lambda **r: r['pk'] % 2})
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.delete_record(0)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=2)))

    async def test_delete_record_deletes_last_element_from_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.delete_record(0)
        assert await conn.zcard('table:first_char') == 0

    async def test_delete_record_deletes_one_element_from_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 3
        await table.delete_record(0)
        assert_that(
            await conn.zrange(
                'table:first_char', ord('c'), ord('c'), byscore=True),
            has_length(1))
        assert_that(
            await conn.zrange(
                'table:first_char', ord('d'), ord('d'), byscore=True),
            has_length(1))
        assert_that(await table.get_record(2), has_entries(s='czzz'))

    async def test_delete_record_doesnt_overwrite_others_with_same_scores(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.delete_record(0)
        assert await conn.zcard('table:first_char') == 1
        assert_that(await table.get_record(2), has_entries(pk=2, s='czzz'))
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=2, s='czzz')))

    async def test_delete_records_on_empty(self, table):
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(0, 50)]))
        assert num_deleted == 0
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(await collect_async_iter(records), empty())

    async def test_delete_records_deletes_nothing(self, table):
        await table.put_record({'pk': 0, 's': 's'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', []))
        assert num_deleted == 0
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0)))

    async def test_delete_records_deletes_all(self, table):
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 10, 's': 's'})
        await table.put_record({'pk': 49, 's': 's'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(0, 50)]))
        assert num_deleted == 3
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(await collect_async_iter(records), empty())

    async def test_delete_records_deletes_some(self, table):
        await table.put_record({'pk': -1, 's': 's'})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 50, 's': 's'})
        await table.put_record({'pk': 51, 's': 's'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(0, 51)]))
        assert num_deleted == 2
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=-1), has_entries(pk=51)))

    async def test_delete_records_deletes_multiple_intervals(
            self, table):
        await table.put_record({'pk': -50, 's': 's'})
        await table.put_record({'pk': -20, 's': 's'})
        await table.put_record({'pk': -10, 's': 's'})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 10, 's': 's'})
        await table.put_record({'pk': 49, 's': 's'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec(
                'primary_key', [tc.Interval(-40, -9), tc.Interval(10, 11)]))
        assert num_deleted == 3
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=0), has_entries(pk=49)))

    async def test_delete_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('c') + 0.1)]))
        assert num_deleted == 2
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='dzzz')))

    async def test_delete_records_uses_recheck_predicate(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        await table.put_record({'pk': 3, 's': 'cxxx'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('c') + 0.1)],
                lambda r: 'y' not in r['s']))
        assert num_deleted == 2
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1, s='dzzz'), has_entries(pk=2, s='cyyy')))

    async def test_delete_records_deletes_all_from_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        assert await conn.zcard('table:first_char') == 2
        await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert await conn.zcard('table:first_char') == 0

    async def test_delete_records_deletes_some_from_other_indexes(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        await table.put_record({'pk': 3, 's': 'cxxx'})
        assert await conn.zcard('table:first_char') == 3
        await table.delete_records(
            tc.StorageRecordsSpec(
                'primary_key', [_inf_to_inf], lambda r: 'y' not in r['s']))
        assert await conn.zcard('table:first_char') == 1

    async def test_delete_records_doesnt_deletes_others_with_same_scores(
            self, make_table, conn):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.delete_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))],
                lambda r: r['pk'] == 0))
        assert await conn.zcard('table:first_char') == 1
        assert_that(await table.get_record(2), has_entries(pk=2, s='czzz'))

    async def test_delete_record_raises_if_deleted_in_subset_previously(
            self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 10)]))
        with pytest.raises(KeyError):
            await table.delete_record(1)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0)))

    async def test_added_scratch_space_records_are_not_returned_immediately(
            self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_added_scratch_space_records_are_returned_after_merge(
            self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        assert_that(await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_adding_to_scratch_space_handles_updates(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        assert_that(await table.get_record(1), has_entries(pk=1, s='s2'))

    async def test_scratch_discard_records_doesnt_removed_immediately(
            self, table):
        await table.put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        assert_that(
            await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_scratch_discard_records_removes_after_merge(self, table):
        await table.put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_scratch_records_can_be_deleted(self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_scratch_records_can_be_deleted_and_added_again(self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        assert_that(await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_scratch_discard_records_with_multiple_intervals(
            self, table):
        await table.put_record({'pk': -50, 's': 's'})
        await table.put_record({'pk': -10, 's': 's'})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 10, 's': 's'})
        await table.put_record({'pk': 49, 's': 's'})
        await table.put_record({'pk': 50, 's': 's'})
        await table.put_record({'pk': 60, 's': 's'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(-10, 5), tc.Interval(40, 51)]))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=10), has_entries(pk=60)))

    async def test_scratch_discard_records_uses_custom_score_function(
            self, make_table):
        def pk_minus_10(**kwargs):
            return kwargs['pk'] - 10

        table = make_table(
            attribute_codecs={'pk': tcr.IntAsStringCodec()},
            score_functions={'primary_key': pk_minus_10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 59})
        await table.put_record({'pk': 60})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(0, 50)]))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=60)))

    async def test_scratch_discard_records_uses_recheck_predicate(self, table):
        def x_in_s(record):
            return 'x' in record['s']
        await table.put_record({'pk': 0, 's': 'aaa'})
        await table.put_record({'pk': 1, 's': 'bxb'})
        await table.put_record({'pk': 2, 's': 'cxc'})
        await table.put_record({'pk': 3, 's': 'ddd'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf], x_in_s))
        table.scratch_merge()
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=3)))

    async def test_scratch_discard_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'haaa'})
        await table.put_record({'pk': 3, 's': 'iaaa'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('d'), ord('i'))]))
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=3)))

    async def test_scratch_discard_records_works_with_equal_primary_key_scores(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': lambda **r: r['pk'] % 2,
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'cyyy'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        table.scratch_merge()
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [_inf_to_inf]))
        assert_that(await collect_async_iter(records), empty())

    async def test_scratch_discard_returns_number_of_records(self, table):
        for i in range(4):
            await table.put_record({'pk': i, 's': 's'})
        num_records = await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 3)]))
        assert num_records == 2

    async def test_scratch_space_add_handles_indexes(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.scratch_put_record({'pk': 2, 's': 'daaa'})
        records = table.get_records(
            tc.StorageRecordsSpec('first_char', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='dzzz')))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('first_char', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1, s='dzzz'), has_entries(pk=2, s='daaa')))

    async def test_scratch_space_deletes_delete_from_storage(
            self, table, conn, scratch_space_is_clear):
        await table.put_record({'pk': 1, 's': 's1'})
        assert await conn.zcard('table:primary_key') == 1
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(1, 1.1)]))
        assert await conn.zcard('table:primary_key') == 1
        table.scratch_merge()
        await scratch_space_is_clear()
        assert await conn.zcard('table:primary_key') == 0

    async def test_scratch_space_updates_delete_overwritten_value(
            self, table, conn, scratch_space_is_clear):
        await table.put_record({'pk': 1, 's': 's1'})
        assert await conn.zcard('table:primary_key') == 1
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert await conn.zcard('table:primary_key') == 2
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        await scratch_space_is_clear()
        assert await conn.zcard('table:primary_key') == 1
        assert_that(await table.get_record(1), has_entries(pk=1, s='s2'))

    async def test_scratch_space_delete_handles_indexes(
            self, make_table, conn, scratch_space_is_clear):
        table = make_table(
            score_functions={
                'primary_key': filter_kwarg('pk'),
                'first_char': lambda **r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 'czzz'})
        assert await conn.zcard('table:first_char') == 1
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(1, 1.1)]))
        assert await conn.zcard('table:first_char') == 1
        table.scratch_merge()
        await scratch_space_is_clear()
        assert await conn.zcard('table:first_char') == 0

    async def test_scratch_activity_blocks_regular_puts(self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                table.put_record({'pk': 3, 's': 's3'}), 0.01)
        table.scratch_merge()
        await asyncio.wait_for(table.put_record({'pk': 3, 's': 's3'}), 0.01)

    async def test_scratch_activity_blocks_regular_single_deletes(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(table.delete_record(1), 0.01)
        table.scratch_merge()
        await asyncio.wait_for(table.delete_record(1), 0.01)

    async def test_scratch_activity_blocks_regular_multiple_deletes(
            self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                table.delete_records(
                    tc.StorageRecordsSpec('primary_key', [_inf_to_inf])), 0.01)
        table.scratch_merge()
        await asyncio.wait_for(
            table.delete_records(
                tc.StorageRecordsSpec('primary_key', [_inf_to_inf])), 0.01)

    async def test_scratch_merge_blocks_scratch_puts_until_merge_complete(
            self, table, monkeypatch):
        scratch_space = ManuallyUnlockedScratchSpace()
        monkeypatch.setattr(table, '_scratch_space', scratch_space)
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        scratch_space.update_not_merging()
        assert not scratch_space.is_not_merging()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                table.scratch_put_record({'pk': 3, 's': 's3'}), 0.01)
        assert not scratch_space.is_not_merging()
        scratch_space.update_not_merging()
        assert scratch_space.is_not_merging()
        await asyncio.wait_for(
            table.scratch_put_record({'pk': 3, 's': 's3'}), 0.01)

    async def test_scratch_merge_blocks_scratch_discards_until_merge_complete(
            self, table, monkeypatch):
        scratch_space = ManuallyUnlockedScratchSpace()
        monkeypatch.setattr(table, '_scratch_space', scratch_space)
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        scratch_space.update_not_merging()
        assert not scratch_space.is_not_merging()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(table.scratch_discard_records(
                tc.StorageRecordsSpec('primary_key', [_inf_to_inf])), 0.01)
        assert not scratch_space.is_not_merging()
        scratch_space.update_not_merging()
        assert scratch_space.is_not_merging()
        await asyncio.wait_for(table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf])), 0.01)

    async def test_multiple_scratch_operations(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.put_record({'pk': 3, 's': 's3'})
        await table.scratch_put_record({'pk': 3, 's': 's3.2'})
        await table.scratch_put_record({'pk': 4, 's': 's4'})
        await table.scratch_put_record({'pk': 5, 's': 's5'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(1, 1.1)]))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=2), has_entries(pk=3, s='s3.2'),
                has_entries(pk=4), has_entries(pk=5)))
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 3, 's': 's3'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(2, 2.1), tc.Interval(4, 4.1)]))
        await table.scratch_put_record({'pk': 6, 's': 's6'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1), has_entries(pk=2),
                has_entries(pk=3, s='s3'), has_entries(pk=4),
                has_entries(pk=5)))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [_inf_to_inf]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1), has_entries(pk=3, s='s3'),
                has_entries(pk=5), has_entries(pk=6)))


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
        def factory(**kwargs):
            kwargs.setdefault(
                'attribute_codecs',
                {'i': tcr.IntAsStringCodec(), 's': tcr.StringCodec()})
            return storage.RowCodec(**kwargs)

        return factory

    @pytest.fixture
    def codec(self, make_codec):
        return make_codec()

    def test_encode_decode(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1'}, 3))
        decoded, generation = codec.decode(encoded)
        assert_that(decoded, has_entries(i=1, s='s1'))
        assert generation == 3

    def test_encode_ignores_extra_attributes(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1', 'x': 'x1'}, 0))
        assert_that(codec.decode(encoded)[0], is_not(has_entry('x', 'x1')))

    def test_encode_raises_on_missing_attributes(self, codec):
        with pytest.raises(ValueError):
            codec.encode({'i': 1}, 0)

    def test_encode_raises_on_attribute_encoding_error(self, make_codec):
        codec = make_codec(
            attribute_codecs={'i': tcr.IntAsStringCodec(), 's': FailCodec()})
        with pytest.raises(tcr.RedisCodingError):
            codec.encode({'i': 1, 's': 's1'}, 0)

    def test_encode_raises_if_attribute_doesnt_encode_to_bytes(
            self, make_codec):
        class BrokenStringReturningCodec(tcr.Codec):
            def encode(self, _):
                return 'a string (supposed to be bytes)'

            def decode(self, _):
                raise Exception

        codec = make_codec(
            attribute_codecs={
                'i': tcr.IntAsStringCodec(),
                's': BrokenStringReturningCodec()})
        with pytest.raises(tcr.RedisCodingError):
            codec.encode({'i': 1, 's': 's1'}, 0)

    def test_encode_raises_if_encoded_attribute_is_too_long(self, make_codec):
        class LargeValueCodec(tcr.Codec):
            def encode(self, length):
                return length * b'x'

            def decode(self, _):
                raise Exception

        codec = make_codec(attribute_codecs={'l': LargeValueCodec()})
        with pytest.raises(tcr.RedisCodingError):
            codec.encode({'l': codec.max_attribute_length + 1}, 0)

    def test_encode_raises_if_generation_is_too_large(self, make_codec):
        codec = make_codec(num_bytes_generation=1)
        with pytest.raises(ValueError):
            codec.encode({'i': 1, 's': 's1'}, codec.max_generation + 1)

    def test_decode_raises_on_non_bytes_arg(self, codec):
        with pytest.raises(ValueError):
            codec.decode('not bytes')

    def test_decode_raises_on_missing_generation(self, codec):
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(b'\x00' * (codec.num_bytes_generation - 1))

    def test_decode_raises_on_missing_attributes(self, make_codec):
        codec = make_codec(attribute_codecs={'i': tcr.IntAsStringCodec()})
        encoded = bytes(codec.encode({'i': 1}, 0))
        codec = make_codec(
            attribute_codecs={
                'i': tcr.IntAsStringCodec(), 's': tcr.StringCodec()})
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(encoded)

    def test_decode_raises_on_duplicate_attribute_ids(self, codec):
        encoded = bytes(codec.encode({'i': 1, 's': 's1'}, 0))
        s_id_index = encoded.index(b's1') - 3
        assert int.from_bytes(encoded[s_id_index + 1:s_id_index + 3]) == 2
        s_id = encoded[s_id_index:s_id_index + 1]
        false_encoded = bytes(
            encoded + s_id + (6).to_bytes(length=2) + b'foobar')
        assert_that(
            codec.decode(false_encoded[:-9])[0], has_entries(i=1, s='s1'))
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(false_encoded)

    def test_decode_raises_on_attribute_decoding_error(self, make_codec):
        encoded = bytes(make_codec(
            attribute_codecs={'s': tcr.StringCodec()}).encode({'s': 's1'}, 0))
        codec = make_codec(attribute_codecs={'s': FailCodec()})
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(encoded)

    def test_decode_raises_on_unknown_attribute_ids(self, make_codec):
        codec = make_codec(attribute_codecs={'s': tcr.StringCodec()})
        encoded = bytes(codec.encode({'s': 's1'}, 0))
        num_bytes_attribute_id = 1
        assert len(encoded) == (
            num_bytes_attribute_id + codec.num_bytes_attribute_length +
            codec.num_bytes_generation + len('s1'))
        assert encoded.endswith(b's1')
        false_encoded = (
            encoded +
            encoded[:codec.num_bytes_generation] +
            bytes([encoded[codec.num_bytes_generation] + 1]) +
            encoded[codec.num_bytes_generation + 1:])
        good_part = false_encoded[:len(encoded)]
        assert_that(codec.decode(good_part)[0], has_entries(s='s1'))
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_value_ends_unexpectedly(
            self, make_codec):
        codec = make_codec(attribute_codecs={'s': tcr.StringCodec()})
        encoded = bytes(codec.encode({'s': 'foobar'}, 0))
        false_encoded = encoded[:-1]
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_header_ends_after_id(self, make_codec):
        codec = make_codec(attribute_codecs={
                           's1': tcr.StringCodec(), 's2': tcr.StringCodec()})
        encoded = bytes(codec.encode({'s1': 's1', 's2': 's2'}, 0))
        num_bytes_attribute_id = 1
        after_s2_id = (
            num_bytes_attribute_id + codec.num_bytes_generation +
            codec.num_bytes_attribute_length + len('s1') +
            codec.num_bytes_attribute_length)
        false_encoded = encoded[:after_s2_id]
        with pytest.raises(tcr.RedisCodingError):
            codec.decode(false_encoded)

    def test_decode_raises_if_attribute_header_ends_in_the_middle_of_length(
            self, make_codec):
        codec = make_codec(attribute_codecs={
                           's1': tcr.StringCodec(), 's2': tcr.StringCodec()})
        encoded = bytes(codec.encode({'s1': 's1', 's2': 's2'}, 0))
        num_bytes_attribute_id = 1
        after_s2_id = (
            num_bytes_attribute_id + codec.num_bytes_generation +
            codec.num_bytes_attribute_length + len('s1') +
            codec.num_bytes_attribute_length)
        false_encoded = encoded[:after_s2_id + 1]
        with pytest.raises(tcr.RedisCodingError):
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


class TestScratchSpace:
    @pytest.fixture
    def scratch_space(self):
        return storage.ScratchSpace()

    def test_is_clear_on_construction(self, scratch_space):
        assert scratch_space.is_clear()

    def test_is_not_merging_on_construction(self, scratch_space):
        assert scratch_space.is_not_merging()

    def test_next_generation_is_greater(self, scratch_space):
        assert scratch_space.next_generation > scratch_space.current_generation

    def test_mark_existing_for_del_means_not_clear(self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'', {})
        assert not scratch_space.is_clear()
        assert scratch_space.is_not_merging()

    def test_mark_pk_for_del_means_not_clear(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        assert not scratch_space.is_clear()
        assert scratch_space.is_not_merging()

    def test_mark_add_means_not_clear(self, scratch_space):
        scratch_space.mark_record_for_adding(1, {})
        assert not scratch_space.is_clear()
        assert scratch_space.is_not_merging()

    def test_merge_means_is_merging(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        assert not scratch_space.is_not_merging()

    def test_merge_means_advances_generation(self, scratch_space):
        generation = scratch_space.current_generation
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        assert scratch_space.current_generation > generation

    def test_merge_done_means_not_merging_anymore(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        scratch_space.merge_done()
        assert scratch_space.is_not_merging()

    def test_unaffected_records_are_current_before_merge(self, scratch_space):
        scratch_space.mark_record_for_adding(1, b'x')
        assert scratch_space.record_is_current(
            2, b'', scratch_space.current_generation)

    def test_unaffected_records_are_current_during_merge(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        assert scratch_space.record_is_current(
            2, b'', scratch_space.current_generation)

    def test_future_records_are_not_current_before_merge(self, scratch_space):
        assert not scratch_space.record_is_current(
            2, b'', scratch_space.next_generation)

    def test_future_records_are_not_current_during_merge(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        assert not scratch_space.record_is_current(
            2, b'', scratch_space.next_generation)

    def test_existing_marked_for_del_are_current_before_merge(
            self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'x', {})
        assert scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_existing_marked_for_del_are_not_current_during_merge(
            self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'x', {})
        scratch_space.merge()
        assert not scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_existing_marked_for_del_are_current_after_merge(
            self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'x', {})
        scratch_space.merge()
        scratch_space.merge_done()
        assert scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_pk_marked_for_del_are_current_before_merge(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        assert scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_pk_marked_for_del_are_not_current_during_merge(
            self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        assert not scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_pk_marked_for_del_are_current_after_merge(self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        scratch_space.merge_done()
        assert scratch_space.record_is_current(
            1, b'x', scratch_space.current_generation)

    def test_existing_marked_for_del_are_returned_before_merge_done(
            self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'x', {'a': 1})
        assert_that(scratch_space.records_for_deletion, has_item({'a': 1}))
        assert_that(scratch_space.encoded_records_for_deletion, has_item(b'x'))
        scratch_space.merge()
        assert_that(scratch_space.records_for_deletion, has_item({'a': 1}))
        assert_that(scratch_space.encoded_records_for_deletion, has_item(b'x'))

    def test_existing_marked_for_del_are_not_returned_after_merge_done(
            self, scratch_space):
        scratch_space.mark_existing_record_for_deletion(b'x', {'a': 1})
        scratch_space.merge()
        scratch_space.merge_done()
        assert_that(list(scratch_space.records_for_deletion), empty())
        assert_that(scratch_space.encoded_records_for_deletion, empty())

    def test_pk_marked_for_del_is_returned_before_merge_done(
            self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        assert_that(scratch_space.primary_keys_for_deletion, has_item(1))
        scratch_space.merge()
        assert_that(scratch_space.primary_keys_for_deletion, has_item(1))

    def test_pk_marked_for_del_is_not_returned_after_merge_done(
            self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.merge()
        scratch_space.merge_done()
        assert_that(scratch_space.primary_keys_for_deletion, empty())

    def test_pk_marked_for_del_and_then_added_is_not_returned(
            self, scratch_space):
        scratch_space.mark_primary_key_for_deletion(1)
        scratch_space.mark_record_for_adding(1, {})
        assert_that(scratch_space.primary_keys_for_deletion, empty())

    def test_can_merge_if_clear(self, scratch_space):
        generation = scratch_space.current_generation
        scratch_space.merge()
        assert scratch_space.current_generation > generation
        scratch_space.merge_done()
