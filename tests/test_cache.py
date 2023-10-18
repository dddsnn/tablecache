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

import unittest.mock as um

from hamcrest import *
import pytest

import tablecache as tc

_inf_to_inf = float('-inf'), float('inf')


async def collect_async_iter(i):
    l = []
    async for item in i:
        l.append(item)
    return l


class MockDbTable(tc.DbTable):
    def __init__(self):
        self.records = {}

    async def get_records(self, primary_keys):
        for key, record in self.records.items():
            if key in primary_keys:
                yield self._make_record(record)

    async def get_record(self, primary_key):
        return self._make_record(self.records[primary_key])

    async def get_record_subset(self, subset):
        for key, record in self.records.items():
            for interval in subset.score_intervals:
                if key in interval:
                    yield self._make_record(record)
                    break

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable(tc.StorageTable):
    def __init__(self, conn, **kwargs):
        self.score_function = kwargs['score_function']
        assert conn == 'conn_not_needed_for_mock'
        assert kwargs['attribute_codecs'] == (
            'attribute_codecs_not_needed_for_mock')
        assert kwargs['table_name'] == 'table_name_not_needed_for_mock'
        self.records = {}

    @property
    def table_name(self):
        return 'mock table'

    async def clear(self):
        self.records = {}

    async def put_record(self, record):
        primary_key = self.score_function(record)
        self.records[primary_key] = record

    async def get_record(self, primary_key):
        record = self._make_record(self.records[primary_key])
        return record

    async def get_record_subset(self, key_subset):
        for key, record in self.records.items():
            for interval in key_subset.score_intervals:
                if key in interval:
                    yield self._make_record(record)
                    break

    def _make_record(self, record):
        return record | {'source': 'storage'}

    async def delete_record(self, primary_key) -> None:
        del self.records[primary_key]

    async def delete_record_subset(self, score_intervals):
        num_deleted = 0
        for primary_key in list(self.records):
            if any(primary_key in i for i in score_intervals):
                del self.records[primary_key]
                num_deleted += 1
        return num_deleted


class AdjustableNumberRangeSubset(tc.NumberRangeSubset):
    def __init__(self, ge, lt):
        super().__init__(ge, lt)
        self.observe = um.Mock()

    def adjust(self, *, prune_ge, prune_lt, new_ge, new_lt):
        if prune_ge > prune_lt or new_ge > new_lt:
            raise ValueError
        if prune_lt > self._lt:
            raise ValueError
        self._ge = max(self._ge, prune_lt)
        self._lt = max(self._lt, new_lt)
        return tc.Adjustment([tc.Interval(prune_ge, prune_lt)],
                             type(self)(new_ge, new_lt))


class TestCachedTable:
    @pytest.fixture
    def db_table(self):
        return MockDbTable()

    @pytest.fixture
    def storage_table_instance(self):
        return {}

    @pytest.fixture
    def make_table(self, db_table, storage_table_instance, monkeypatch):
        def make_mock_storage_table(*args, **kwargs):
            if 'instance' in storage_table_instance:
                raise Exception('Can only have one mock storage table.')
            mock_storage_table = MockStorageTable(*args, **kwargs)
            storage_table_instance['instance'] = mock_storage_table
            return mock_storage_table

        import tablecache.storage
        monkeypatch.setattr(
            tablecache.storage, 'RedisTable', make_mock_storage_table)

        def factory(cached_subset_class=tc.All.with_primary_key('pk')):
            return tc.CachedTable(
                cached_subset_class, db_table, primary_key_name='pk',
                attribute_codecs='attribute_codecs_not_needed_for_mock',
                redis_conn='conn_not_needed_for_mock',
                redis_table_name='table_name_not_needed_for_mock')

        return factory

    @pytest.fixture
    def get_storage_table(self, storage_table_instance):
        def getter():
            return storage_table_instance['instance']

        return getter

    @pytest.fixture
    def table(self, make_table):
        return make_table()

    async def test_load_and_get_record(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        assert_that(
            await table.get_record(1),
            has_entries(pk=1, k='v1', source='storage'))

    async def test_get_record_raises_if_not_loaded(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        with pytest.raises(ValueError):
            await table.get_record(1)

    async def test_get_record_raises_on_nonexistent(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_get_record_subset_all(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load()
        assert_that(
            await collect_async_iter(table.get_record_subset()),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(6)]))

    async def test_get_record_subset_only_some(self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load(*_inf_to_inf)
        assert_that(
            await collect_async_iter(table.get_record_subset(2, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))

    async def test_loads_only_specified_subset(
            self, make_table, db_table, get_storage_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load(2, 4)
        assert_that(
            await collect_async_iter(
                get_storage_table().get_record_subset(
                    tc.NumberRangeSubset.with_primary_key('pk')(*_inf_to_inf))
            ), contains_inanyorder(*[has_entries(pk=i) for i in range(2, 4)]))

    async def test_load_observes_loaded_records(self, make_table, db_table):
        subset_class = AdjustableNumberRangeSubset.with_primary_key('pk')
        table = make_table(subset_class)
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load(2, 4)
        expected_observations = await collect_async_iter(
            db_table.get_record_subset(subset_class(2, 4)))
        assert_that(
            table.cached_subset.observe.call_args_list,
            contains_inanyorder(*[um.call(r) for r in expected_observations]))

    async def test_load_clears_storage_first(
            self, table, db_table, get_storage_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        get_storage_table().records = {2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        assert_that(await table.get_record(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_load_raises_if_already_loaded(self, table):
        await table.load()
        with pytest.raises(ValueError):
            await table.load()

    async def test_get_record_subset_returns_db_state_if_subset_not_cached(
            self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load(2, 4)
        assert_that(
            await collect_async_iter(table.get_record_subset(2, 5)),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2, 5)]))

    async def test_get_record_also_checks_db_in_case_not_in_cached_subset(
            self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load(2, 4)
        assert_that(await table.get_record(1), has_entries(pk=1, source='db'))

    async def test_get_record_doesnt_check_db_if_all_in_cache(
            self, table, db_table, get_storage_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load()
        del get_storage_table().records[1]
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='v1'))

    async def test_get_record_refreshes_invalid_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_record_subset_refreshes_invalid_keys(
            self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load(*_inf_to_inf)
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(table.get_record_subset(1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_only_refreshes_once(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        await table.get_record(1)
        db_table.records = {1: {'pk': 1, 'k': 'c1'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_record_subset_only_refreshes_once(
            self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load(*_inf_to_inf)
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        await collect_async_iter(table.get_record_subset(1, 2))
        db_table.records = {1: {'pk': 1, 'k': 'c1'}}
        assert_that(
            await collect_async_iter(table.get_record_subset(1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_deletes_invalid_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_get_record_subset_deletes_invalid_keys(
            self, make_table, db_table):
        table = make_table(tc.NumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i, 'k': f'a{i}'} for i in range(3)}
        await table.load(*_inf_to_inf)
        db_table.records = {0: {'pk': 0, 'k': 'a0'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 3)),
            contains_inanyorder(
                has_entries(pk=0, k='a0'), has_entries(pk=2, k='a2')))

    async def test_invalidate_record_ignores_nonexistent_keys(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load()
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)
        assert_that(await table.get_record(1), has_entries(pk=1, k='a1'))

    async def test_adjust_cached_subset_prunes_old_data(
            self, make_table, db_table):
        table = make_table(AdjustableNumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load(0, 4)
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))
        await table.adjust_cached_subset(
            prune_ge=0, prune_lt=2, new_ge=10, new_lt=11)
        assert_that(
            await collect_async_iter(table.get_record_subset(2, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(4)]))

    async def test_adjust_cached_subset_loads_new_subset(
            self, make_table, db_table):
        table = make_table(AdjustableNumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load(0, 2)
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 2)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset(
            prune_ge=-1, prune_lt=0, new_ge=2, new_lt=4)
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_doesnt_introduce_duplicates(
            self, make_table, db_table):
        table = make_table(AdjustableNumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load(0, 2)
        await table.adjust_cached_subset(
            prune_ge=-1, prune_lt=0, new_ge=0, new_lt=4)
        assert_that(
            await collect_async_iter(table.get_record_subset(0, 4)),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_observes_new_records(
            self, make_table, db_table):
        table = make_table(AdjustableNumberRangeSubset.with_primary_key('pk'))
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load(0, 2)
        await table.adjust_cached_subset(
            prune_ge=-1, prune_lt=0, new_ge=0, new_lt=4)
        expected_observations = await collect_async_iter(
            db_table.get_record_subset(
                AdjustableNumberRangeSubset.with_primary_key('pk')(2, 4)))
        assert_that(
            table._cached_subset.observe.call_args_list,
            contains_inanyorder(
                *[anything() for _ in range(4)],
                *[um.call(r) for r in expected_observations]))


class TestInvalidRecordRepository:
    @pytest.fixture
    def repo(self):
        import tablecache.cache
        return tablecache.cache.InvalidRecordRepository()

    def test_primary_key_invalid(self, repo):
        repo.flag_invalid(1, 101)
        assert 1 in repo.invalid_primary_keys
        assert 101 not in repo.invalid_primary_keys

    def test_score_invalid(self, repo):
        repo.flag_invalid(1, 101)
        assert 101 in repo.invalid_scores
        assert 1 not in repo.invalid_scores

    def test_primary_key_not_invalid(self, repo):
        repo.flag_invalid(1, 101)
        assert 2 not in repo.invalid_primary_keys

    def test_score_not_invalid(self, repo):
        repo.flag_invalid(1, 101)
        assert 102 not in repo.invalid_scores

    def test_primary_key_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, 101)
        repo.clear()
        assert 1 not in repo.invalid_primary_keys

    def test_score_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, 101)
        repo.clear()
        assert 101 not in repo.invalid_scores

    def test_len_is_number_of_primary_keys(self, repo):
        assert len(repo) == 0
        repo.flag_invalid(1, 101)
        repo.flag_invalid(2, 102)
        assert len(repo) == 2
        repo.flag_invalid(3, 102)
        assert len(repo) == 3
