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
import operator as op

from hamcrest import *
import pytest

import tablecache as tc
import tablecache.local as tcl
from tests.helpers import collect_async_iter


class IndexesFromScoreFunctionsDict:
    def __init__(self, primary_key_name, score_functions):
        self._primary_key_name = primary_key_name
        self._score_functions = score_functions

    @property
    def index_names(self):
        return frozenset(self._score_functions)

    def score(self, index_name, record):
        try:
            return self._score_functions[index_name](record)
        except KeyError:
            raise ValueError

    def primary_key_score(self, primary_key):
        return self.score('primary_key', {self._primary_key_name: primary_key})


class PausingCoroutine:
    def __init__(self, coro):
        self._coro = coro
        self._event = asyncio.Event()

    async def __call__(self, *args, **kwargs):
        await self._event.wait()
        await self._coro(*args, **kwargs)

    def _continue(self):
        assert not self._event.is_set()
        self._event.set()


class TestLocalTable:
    @pytest.fixture
    def tables(self):
        return []

    @pytest.fixture
    def make_table(self, tables):
        def factory(
                table_name='table', primary_key_name='pk',
                score_functions=None):
            score_functions = score_functions or {
                'primary_key': op.itemgetter(primary_key_name)}
            table = tcl.LocalStorageTable(
                table_name=table_name, primary_key_name=primary_key_name,
                indexes=IndexesFromScoreFunctionsDict(
                    primary_key_name, score_functions))
            tables.append(table)
            return table

        return factory

    @pytest.fixture
    def table(self, make_table):
        return make_table()

    @pytest.fixture
    def scratch_space_is_clear(self, tables):
        async def waiter():
            for table in tables:
                if table._scratch_merge_task:
                    await table._scratch_merge_task
        return waiter

    @pytest.fixture
    def pause_scratch_merge(self, tables, monkeypatch):
        def mock_scratch_merge():
            if len(tables) != 1:
                raise Exception('Need exactly one table.')
            mocked_coro = PausingCoroutine(tables[0]._scratch_merge)
            monkeypatch.setattr(tables[0], '_scratch_merge', mocked_coro)
            return mocked_coro
        return mock_scratch_merge

    @pytest.fixture(autouse=True)
    async def wait_for_ongoing_merges_to_finish(
            self, scratch_space_is_clear, tables):
        for table in tables:
            await scratch_space_is_clear(table)

    def assert_index_lengths(
            self, table, index_length, scratch_adds=0, scratch_deletes=0):
        assert_that(
            table._indexes.values(), only_contains(has_length(index_length)))
        assert_that(
            table._scratch_indexes.values(),
            only_contains(has_length(scratch_adds)))
        assert len(table._scratch_records_to_delete) == scratch_deletes

    async def test_put_record_get_record(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))

    async def test_put_record_get_record_multiple(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_put_record_raises_on_missing_primary_key(self, table):
        with pytest.raises(ValueError):
            await table.put_record({'s': 's1'})

    async def test_put_record_overwrites_old_value_with_same_primary_key(
            self, table):
        self.assert_index_lengths(table, 0)
        await table.put_record({'pk': 1, 's': 'a'})
        await table.put_record({'pk': 1, 's': 'b'})
        self.assert_index_lengths(table, 1)
        await table.put_record({'pk': 1, 's': 'new'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='new'))
        self.assert_index_lengths(table, 1)

    async def test_put_record_overwrites_old_other_indexes(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        self.assert_index_lengths(table, 1)
        await table.put_record({'pk': 0, 's': 'dzzz'})
        self.assert_index_lengths(table, 1)

    async def test_put_record_doesnt_overwrite_other_records_in_other_indexes(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'czzz'})
        self.assert_index_lengths(table, 2)
        await table.put_record({'pk': 0, 's': 'fzzz'})
        self.assert_index_lengths(table, 2)
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='czzz')))

    async def test_put_record_doesnt_overwrite_others_with_same_pk_score(
            self, make_table):
        table = make_table(
            score_functions={'primary_key': lambda r: r['pk'] % 2})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 3, 's': 's3'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        assert_that(await table.get_record(3), has_entries(pk=3, s='s3'))

    async def test_put_record_doesnt_overwrite_others_with_same_scores(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': lambda r: r['pk'] % 2,
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        await table.put_record({'pk': 4, 's': 'czzz'})
        self.assert_index_lengths(table, 3)
        await table.put_record({'pk': 2, 's': 'fzzz'})
        self.assert_index_lengths(table, 3)
        records = table.get_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))],
                lambda r: r['s'][0] == 'c'))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=0, s='czzz'), has_entries(pk=4, s='czzz')))

    async def test_put_record_handles_equal_scores_and_counts_in_other_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': lambda r: r['pk'] % 2,
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        await table.put_record({'pk': 0, 's': 'fzzz'})
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

    async def test_get_record_handles_records_with_equal_scores(
            self, make_table):
        table = make_table(
            score_functions={'primary_key': lambda r: r['pk'] % 2})
        await table.put_record({'pk': 0, 's': 's'})
        await table.put_record({'pk': 2, 's': 's'})
        assert_that(await table.get_record(0), has_entries(pk=0))
        assert_that(await table.get_record(2), has_entries(pk=2))

    async def test_clear(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.get_record(1)
        self.assert_index_lengths(table, 1)
        await table.clear()
        with pytest.raises(KeyError):
            await table.get_record(1)
        self.assert_index_lengths(table, 0)

    async def test_clear_clears_scratch_space(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(1, 2)]))
        self.assert_index_lengths(table, 1, scratch_adds=1, scratch_deletes=1)
        await table.clear()
        self.assert_index_lengths(table, 0, scratch_adds=0, scratch_deletes=0)

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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(await collect_async_iter(records), empty())

    async def test_get_records_on_one_record(self, table):
        await table.put_record({'pk': 0, 's': 's1'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0, s='s1')))

    async def test_get_records_on_all_contained(self, table):
        await table.put_record({'pk': -50, 's': 's1'})
        await table.put_record({'pk': 0, 's': 's2'})
        await table.put_record({'pk': 50, 's': 's3'})
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
        def pk_minus_10(record):
            return record['pk'] - 10

        table = make_table(score_functions={'primary_key': pk_minus_10})
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
            'primary_key', [tc.Interval.everything()], x_in_s))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1), has_entries(pk=2)))

    async def test_get_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
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
                'primary_key': lambda r: r['pk'] % 2,
                'first_char': lambda r: ord(r['s'][0])})
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0)))

    async def test_delete_record_deletes(self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.delete_record(1)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=2)))

    async def test_delete_record_doesnt_delete_other_records_with_same_score(
            self, make_table):
        table = make_table(
            score_functions={'primary_key': lambda r: r['pk'] % 2})
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.delete_record(0)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=2)))

    async def test_delete_record_deletes_from_all_indexes(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        self.assert_index_lengths(table, 3)
        await table.delete_record(0)
        self.assert_index_lengths(table, 2)

    async def test_delete_records_on_empty(self, table):
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(0, 50)]))
        assert num_deleted == 0
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(await collect_async_iter(records), empty())

    async def test_delete_records_deletes_nothing(self, table):
        await table.put_record({'pk': 0, 's': 's'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec('primary_key', []))
        assert num_deleted == 0
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=0), has_entries(pk=49)))

    async def test_delete_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        num_deleted = await table.delete_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('c') + 0.1)]))
        assert num_deleted == 2
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='dzzz')))

    async def test_delete_records_uses_recheck_predicate(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1, s='dzzz'), has_entries(pk=2, s='cyyy')))

    async def test_delete_records_deletes_from_all_indexes(self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 2, 's': 'cyyy'})
        await table.put_record({'pk': 3, 's': 'cxxx'})
        self.assert_index_lengths(table, 3)
        await table.delete_records(
            tc.StorageRecordsSpec(
                'primary_key', [tc.Interval.everything()],
                lambda r: 'y' not in r['s']))
        self.assert_index_lengths(table, 1)

    async def test_delete_record_raises_if_deleted_in_subset_previously(
            self, table):
        await table.put_record({'pk': 0, 's': 's0'})
        await table.put_record({'pk': 1, 's': 's1'})
        await table.delete_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 10)]))
        with pytest.raises(KeyError):
            await table.delete_record(1)
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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

    async def test_scratch_discard_records_doesnt_remove_immediately(
            self, table):
        await table.put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
        assert_that(
            await table.get_record(2), has_entries(pk=2, s='s2'))

    async def test_scratch_discard_records_removes_after_merge(self, table):
        await table.put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_scratch_records_can_be_deleted(self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_scratch_records_can_be_deleted_and_added_again(self, table):
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=-50), has_entries(pk=10), has_entries(pk=60)))

    async def test_scratch_discard_records_uses_custom_score_function(
            self, make_table):
        def pk_minus_10(record):
            return record['pk'] - 10

        table = make_table(score_functions={'primary_key': pk_minus_10})
        await table.put_record({'pk': 0})
        await table.put_record({'pk': 10})
        await table.put_record({'pk': 59})
        await table.put_record({'pk': 60})
        await table.scratch_discard_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval(0, 50)]))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
            'primary_key', [tc.Interval.everything()], x_in_s))
        table.scratch_merge()
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=3)))

    async def test_scratch_discard_records_with_non_primary_key_index(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'czzz'})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.put_record({'pk': 2, 's': 'haaa'})
        await table.put_record({'pk': 3, 's': 'iaaa'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('d'), ord('i'))]))
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=0), has_entries(pk=3)))

    async def test_scratch_discard_records_works_with_equal_primary_key_scores(
            self, make_table):
        table = make_table(
            score_functions={
                'primary_key': lambda r: r['pk'] % 2,
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 0, 's': 'cyyy'})
        await table.put_record({'pk': 2, 's': 'czzz'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec(
                'first_char', [tc.Interval(ord('c'), ord('d'))]))
        table.scratch_merge()
        records = table.get_records(tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()]))
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
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 'dzzz'})
        await table.scratch_put_record({'pk': 2, 's': 'daaa'})
        records = table.get_records(
            tc.StorageRecordsSpec('first_char', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(has_entries(pk=1, s='dzzz')))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('first_char', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1, s='dzzz'), has_entries(pk=2, s='daaa')))

    async def test_scratch_space_deletes_delete_from_storage(
            self, table, scratch_space_is_clear):
        await table.put_record({'pk': 1, 's': 's1'})
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=0)
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=1)
        table.scratch_merge()
        await scratch_space_is_clear()
        self.assert_index_lengths(table, 0, scratch_adds=0, scratch_deletes=0)

    async def test_scratch_space_updates_delete_overwritten_value(
            self, table, scratch_space_is_clear):
        await table.put_record({'pk': 1, 's': 's1'})
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=0)
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        self.assert_index_lengths(table, 1, scratch_adds=1, scratch_deletes=0)
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        assert_that(await table.get_record(1), has_entries(pk=1, s='s2'))
        await scratch_space_is_clear()
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=0)

    async def test_scratch_space_delete_handles_indexes(
            self, make_table, scratch_space_is_clear):
        table = make_table(
            score_functions={
                'primary_key': op.itemgetter('pk'),
                'first_char': lambda r: ord(r['s'][0])})
        await table.put_record({'pk': 1, 's': 'czzz'})
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=0)
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        self.assert_index_lengths(table, 1, scratch_adds=0, scratch_deletes=1)
        table.scratch_merge()
        await scratch_space_is_clear()
        self.assert_index_lengths(table, 0, scratch_adds=0, scratch_deletes=0)

    async def test_get_record_doesnt_return_deleted_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(1)
        paused_merge._continue()

    async def test_get_record_returns_overwritten_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        assert_that(await table.get_record(1), has_entries(pk=1, s='s2'))
        paused_merge._continue()

    async def test_get_record_doesnt_return_overwritten_then_deleted_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        with pytest.raises(KeyError):
            await table.get_record(1)
        paused_merge._continue()

    async def test_get_record_returns_deleted_then_overwritten_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert_that(await table.get_record(1), has_entries(pk=1, s='s1'))
        table.scratch_merge()
        assert_that(await table.get_record(1), has_entries(pk=1, s='s2'))
        paused_merge._continue()

    async def test_get_records_doesnt_return_deleted_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s1')))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            empty())
        paused_merge._continue()

    async def test_get_records_returns_overwritten_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s1')))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s2')))
        paused_merge._continue()

    async def test_get_records_doesnt_return_overwritten_then_deleted_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s1')))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            empty())
        paused_merge._continue()

    async def test_get_records_returns_deleted_then_overwritten_in_scratch(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.put_record({'pk': 1, 's': 's1'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        await table.scratch_put_record({'pk': 1, 's': 's2'})
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s1')))
        table.scratch_merge()
        assert_that(
            await collect_async_iter(table.get_records(
                tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))),
            contains_exactly(has_entries(pk=1, s='s2')))
        paused_merge._continue()

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
                    tc.StorageRecordsSpec(
                        'primary_key', [tc.Interval.everything()])), 0.01)
        table.scratch_merge()
        await asyncio.wait_for(
            table.delete_records(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval.everything()])), 0.01)

    async def test_scratch_merge_blocks_scratch_puts_until_merge_complete(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                table.scratch_put_record({'pk': 3, 's': 's3'}), 0.01)
        paused_merge._continue()
        await asyncio.wait_for(
            table.scratch_put_record({'pk': 3, 's': 's3'}), 0.01)

    async def test_scratch_merge_blocks_scratch_discards_until_merge_complete(
            self, table, pause_scratch_merge):
        paused_merge = pause_scratch_merge()
        await table.scratch_put_record({'pk': 2, 's': 's2'})
        table.scratch_merge()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(table.scratch_discard_records(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval.everything()])), 0.01)
        paused_merge._continue()
        await asyncio.wait_for(table.scratch_discard_records(
            tc.StorageRecordsSpec(
                'primary_key', [tc.Interval.everything()])), 0.01)

    async def test_multiple_scratch_operations(self, table):
        await table.put_record({'pk': 1, 's': 's1'})
        await table.put_record({'pk': 2, 's': 's2'})
        await table.put_record({'pk': 3, 's': 's3'})
        await table.scratch_put_record({'pk': 3, 's': 's3.2'})
        await table.scratch_put_record({'pk': 4, 's': 's4'})
        await table.scratch_put_record({'pk': 5, 's': 's5'})
        await table.scratch_discard_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval(1, 1.1)]))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
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
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1), has_entries(pk=2),
                has_entries(pk=3, s='s3'), has_entries(pk=4),
                has_entries(pk=5)))
        table.scratch_merge()
        records = table.get_records(
            tc.StorageRecordsSpec('primary_key', [tc.Interval.everything()]))
        assert_that(
            await collect_async_iter(records),
            contains_inanyorder(
                has_entries(pk=1), has_entries(pk=3, s='s3'),
                has_entries(pk=5), has_entries(pk=6)))
