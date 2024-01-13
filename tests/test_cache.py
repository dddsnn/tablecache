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


async def collect_async_iter(i):
    ls = []
    async for item in i:
        ls.append(item)
    return ls


class MockDbTable(tc.DbTable):
    def __init__(self):
        self.records = {}

    async def get_records(self, query, *args):
        if not args:
            assert query == 'query_all'
            def key_matches(_): return True
        else:
            assert query == 'query_some'
            assert_that(
                args,
                all_of(
                    instance_of(tuple),
                    contains_exactly(instance_of(tuple))))

            def key_matches(k): return k in args[0]
        for key, record in self.records.items():
            if key_matches(key):
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable(tc.StorageTable):
    def __init__(self, conn, **kwargs):
        self.score_functions = kwargs['score_functions']
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
        primary_key = self.score_functions['primary_key'](**record)
        self.records[primary_key] = record

    async def get_record(self, primary_key):
        record = self._make_record(self.records[primary_key])
        return record

    async def get_record_subset(
            self, index_name, score_intervals, recheck_predicate=None):
        if index_name != 'primary_key':
            raise NotImplementedError
        score_intervals = list(score_intervals)
        for key, record in self.records.items():
            if recheck_predicate and not recheck_predicate(record):
                continue
            for interval in score_intervals:
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


class TestCachedTable:
    class Indexes(tc.PrimaryKeyIndexes):
        def __init__(self):
            super().__init__('pk', 'query_all', 'query_some')
            self.observe_mock = um.Mock()

        def observe(self, *args, **kwargs):
            super().observe(*args, **kwargs)
            self.observe_mock(*args, **kwargs)

    @pytest.fixture
    def indexes(self):
        return self.Indexes()

    @pytest.fixture
    def db_table(self):
        return MockDbTable()

    @pytest.fixture
    def storage_table_instance(self):
        return {}

    @pytest.fixture
    def make_table(
            self, indexes, db_table, storage_table_instance, monkeypatch):
        def make_mock_storage_table(*args, **kwargs):
            if 'instance' in storage_table_instance:
                raise Exception('Can only have one mock storage table.')
            mock_storage_table = MockStorageTable(*args, **kwargs)
            storage_table_instance['instance'] = mock_storage_table
            return mock_storage_table

        import tablecache.storage
        monkeypatch.setattr(
            tablecache.storage, 'RedisTable', make_mock_storage_table)

        def factory(indexes=indexes):
            return tc.CachedTable(
                indexes, db_table, primary_key_name='pk',
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
        await table.load('primary_key')
        assert_that(
            await table.get_record(1),
            has_entries(pk=1, k='v1', source='storage'))


    async def test_get_record_raises_on_nonexistent(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load('primary_key')
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_get_record_subset_all(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(6))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(6)]))

    async def test_get_record_subset_only_some(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))

    async def test_loads_only_specified_subset(
            self, table, db_table, get_storage_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key', 2, 4)
        assert_that(
            await collect_async_iter(
                get_storage_table().get_record_subset(
                    'primary_key',
                    [tc.Interval(float('-inf'), float('inf'))])),
            contains_inanyorder(*[has_entries(pk=i) for i in [2, 4]]))

    async def test_load_observes_loaded_records(
            self, table, db_table, indexes):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key', 2, 4)
        expected_observations = await collect_async_iter(
            db_table.get_records('query_some', (2, 4)))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(*[um.call(r) for r in expected_observations]))

    async def test_load_clears_storage_first(
            self, table, db_table, get_storage_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        get_storage_table().records = {2: {'pk': 2, 'k': 'v2'}}
        assert_that(await table.get_record(2), has_entries(k='v2'))
        await table.load('primary_key')
        assert_that(await table.get_record(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get_record(2)


    async def test_get_record_subset_returns_db_state_if_subset_not_cached(
            self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key', *range(2, 4))
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(2, 5))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2, 5)]))

    async def test_get_record_also_checks_db_in_case_not_in_cached_subset(
            self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key', *range(2, 4))
        assert_that(await table.get_record(1), has_entries(pk=1, source='db'))

    async def test_get_record_doesnt_check_db_if_all_in_cache(
            self, table, db_table, get_storage_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load('primary_key', 1)
        del get_storage_table().records[1]
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='v1'))

    async def test_get_record_refreshes_existing_invalid_keys(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_record_subset_refreshes_existing_invalid_keys(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', 1)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_loads_new_invalid_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'a1'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.invalidate_record(2)
        assert_that(await table.get_record(2), has_entries(pk=2, k='a2'))

    async def test_get_record_subset_loads_new_invalid_keys(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'a1'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.invalidate_record(2)
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', 1, 2)),
            contains_inanyorder(
                has_entries(pk=1, k='a1'), has_entries(pk=2, k='a2')))

    async def test_get_record_only_refreshes_once(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        await table.get_record(1)
        db_table.records = {1: {'pk': 1, 'k': 'c1'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_record_subset_only_refreshes_once(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'b1'}}
        await table.invalidate_record(1)
        await collect_async_iter(table.get_record_subset('primary_key', 1, 2))
        db_table.records = {1: {'pk': 1, 'k': 'c1'}}
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', 1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_deletes_invalid_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.load('primary_key')
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_get_record_subset_deletes_invalid_keys(
            self, table, db_table):
        db_table.records = {i: {'pk': i, 'k': f'a{i}'} for i in range(3)}
        await table.load('primary_key')
        db_table.records = {0: {'pk': 0, 'k': 'a0'}, 2: {'pk': 2, 'k': 'a2'}}
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(3))),
            contains_inanyorder(
                has_entries(pk=0, k='a0'), has_entries(pk=2, k='a2')))

    async def test_invalidate_record_observes_newly_added(
            self, table, db_table, indexes):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        new_record = {'pk': 2, 'k': 'a2'}
        db_table.records = {1: {'pk': 1, 'k': 'a1'}, 2: new_record}
        await table.invalidate_record(2)
        indexes.observe_mock.assert_called_with(
            new_record | {'source': 'db'})

    async def test_invalidate_record_ignores_nonexistent_keys(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'a1'}}
        await table.load('primary_key')
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)
        assert_that(await table.get_record(1), has_entries(pk=1, k='a1'))

    async def test_adjust_cached_subset_prunes_old_data(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load('primary_key', 0, 1)
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key', *range(2, 4))
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(0, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(4)]))

    async def test_adjust_cached_subset_loads_new_data(
            self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load('primary_key', *range(2))
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_doesnt_introduce_duplicates(
            self, make_table, db_table):
        class Indexes(self.Indexes):
            def adjust(self, index_name, *primary_keys):
                assert index_name == 'primary_key'
                return tc.Adjustment([], 'primary_key', primary_keys, {})

        table = make_table(indexes=Indexes())
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load('primary_key', *range(2))
        await table.adjust_cached_subset('primary_key', *range(4))
        assert_that(
            await collect_async_iter(
                table.get_record_subset('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_observes_new_records(
            self, table, db_table, indexes):
        db_table.records = {i: {'pk': i} for i in range(4)}
        await table.load('primary_key', *range(2))
        await table.adjust_cached_subset('primary_key', *range(4))
        expected_observations = await collect_async_iter(
            db_table.get_records('query_some', (2, 3)))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(
                *[anything() for _ in range(4)],
                *[um.call(r) for r in expected_observations]))


class TestInvalidRecordRepository:
    @pytest.fixture
    def repo(self):
        import tablecache.cache
        return tablecache.cache.InvalidRecordRepository(
            MultiIndexes())

    @pytest.mark.parametrize(
        'scores', [{}, {'primary_key': 3}, {'primary_key': 3, 'x_range': 110}])
    def test_primary_key_invalid(self, repo, scores):
        assert not repo.primary_key_is_invalid(2)
        assert 2 not in repo.invalid_primary_keys
        repo.flag_invalid(2, scores)
        assert repo.primary_key_is_invalid(2)
        assert 2 in repo.invalid_primary_keys
        for x in scores.values():
            assert not repo.primary_key_is_invalid(x)
            assert x not in repo.invalid_primary_keys

    def test_score_is_invalid(self, repo):
        assert not repo.score_is_invalid('primary_key', 2)
        assert not repo.score_is_invalid('x_range', 110)
        repo.flag_invalid(1, {'primary_key': 2, 'x_range': 110})
        assert repo.score_is_invalid('primary_key', 2)
        assert repo.score_is_invalid('x_range', 110)

    def test_score_is_invalid_raises_with_unknown_index_name(self, repo):
        with pytest.raises(KeyError):
            repo.score_is_invalid('no_such_index', 2)

    def test_primary_key_score_is_invalid_implicitly(self, repo):
        assert not repo.score_is_invalid('primary_key', 2)
        repo.flag_invalid(1, {})
        assert repo.score_is_invalid('primary_key', 2)

    def test_score_is_invalid_raises_on_dirty_index(self, repo):
        assert not repo.score_is_invalid('x_range', 110)
        repo.flag_invalid(1, {})
        with pytest.raises(tc.DirtyIndex):
            repo.score_is_invalid('x_range', 110)

    def test_primary_key_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, {})
        repo.clear()
        assert not repo.primary_key_is_invalid(1)
        assert 1 not in repo.invalid_primary_keys

    def test_score_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, {'primary_key': 2, 'x_range': 110})
        repo.clear()
        assert not repo.score_is_invalid('primary_key', 2)
        assert not repo.score_is_invalid('x_range', 110)

    def test_index_not_dirty_after_clean(self, repo):
        repo.flag_invalid(1, {})
        repo.clear()
        repo.score_is_invalid('x_range', 110)

    def test_len_is_number_of_primary_keys(self, repo):
        assert len(repo) == 0
        repo.flag_invalid(1, {})
        repo.flag_invalid(2, {})
        assert len(repo) == 2
        repo.flag_invalid(3, {})
        assert len(repo) == 3
