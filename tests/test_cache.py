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

import numbers
import unittest.mock as um

from hamcrest import *
import pytest

import tablecache as tc


async def collect_async_iter(i):
    ls = []
    async for item in i:
        ls.append(item)
    return ls


class SpyIndexes(tc.PrimaryKeyIndexes):
    def __init__(self):
        super().__init__('pk', 'query_all_pks', 'query_some_pks')
        self.adjust_mock = um.Mock()
        self.observe_mock = um.Mock()

    def adjust(self, *args, **kwargs):
        result = super().adjust(*args, **kwargs)
        self.adjust_mock(*args, **kwargs)
        return result

    def observe(self, *args, **kwargs):
        result = super().observe(*args, **kwargs)
        self.observe_mock(*args, **kwargs)
        return result


class MultiIndexes(tc.Indexes[int]):
    def __init__(self):
        self._contains_all = False
        self._pks = set()
        self._range = None

    @property
    def score_functions(self):
        return {
            'primary_key': lambda **r: r['pk'] + 1,
            'x_range': lambda **r: r['x'] + 100}

    def primary_key_score(self, primary_key):
        return self.score_functions['primary_key'](pk=primary_key)

    def storage_records_spec(self, index_name, *args, **kwargs):
        if index_name == 'primary_key':
            return tc.StorageRecordsSpec(
                index_name, [tc.Interval(pk + 1, pk + 1.1) for pk in args])
        if index_name == 'x_range':
            ge = kwargs['min']
            lt = kwargs['max']
            mid = ge + (lt - ge) / 2
            return tc.StorageRecordsSpec(
                index_name, [
                    tc.Interval(ge + 100, mid + 100),
                    tc.Interval(mid + 100, lt + 100)])
        raise NotImplementedError

    def db_query_range(self, index_name, *args, **kwargs):
        if index_name == 'primary_key':
            if args:
                return 'query_some_pks', (args,)
            return 'query_all_pks', ()
        if index_name == 'x_range':
            return 'query_x_range', (kwargs['min'], kwargs['max'])
        raise NotImplementedError

    def adjust(self, index_name, *args, **kwargs):
        if index_name == 'primary_key':
            self._range = None
            if args:
                self._contains_all = False
                self._primary_keys = set(args)
            else:
                self._contains_all = True
                self._primary_keys = set()
            return tc.Adjustment(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval(float('-inf'), float('inf'))]),
                self.db_query_range('primary_key', *args))
        if index_name == 'x_range':
            self._contains_all = False
            self._primary_keys = set()
            self._range = (kwargs['min'], kwargs['max'])
            return tc.Adjustment(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval(float('-inf'), float('inf'))]),
                self.db_query_range('x_range', **kwargs))
        raise NotImplementedError

    def covers(self, index_name, *args, **kwargs):
        if index_name == 'primary_key':
            return (self._contains_all or
                    (args and all(pk in self._pks for pk in args)))
        if index_name == 'x_range':
            if self._contains_all:
                return True
            if self._range is None:
                return False
            loaded_ge, loaded_lt = self._range
            ge, lt = kwargs['min'], kwargs['max']
            return loaded_ge <= ge <= lt <= loaded_lt
        raise NotImplementedError

    def observe(self, record):
        self._pks.add(record['pk'])


class MockDbTable(tc.DbTable):
    def __init__(self):
        self.records = []

    async def get_records(self, query, *args):
        if not args:
            assert query == 'query_all_pks'
            def record_matches(_): return True
        elif query == 'query_some_pks':
            assert_that(
                args,
                all_of(
                    instance_of(tuple),
                    contains_exactly(instance_of(tuple))))

            def record_matches(r): return r['pk'] in args[0]
        else:
            assert query == 'query_x_range'
            assert_that(
                args,
                all_of(
                    instance_of(tuple),
                    contains_exactly(
                        instance_of(numbers.Real), instance_of(numbers.Real))))
            ge, lt = args
            def record_matches(r): return ge <= r['x'] < lt
        for record in self.records:
            if record_matches(record):
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable(tc.StorageTable):
    def __init__(
            self, conn, *, table_name, primary_key_name, attribute_codecs,
            score_functions):
        assert conn == 'conn_not_needed_for_mock'
        assert attribute_codecs == 'attribute_codecs_not_needed_for_mock'
        assert table_name == 'table_name_not_needed_for_mock'
        self._score_functions = score_functions
        self._primary_key_name = primary_key_name
        self.records = {}
        self._indexes = {}

    @property
    def table_name(self):
        return 'mock table'

    async def clear(self):
        self.records = {}
        self._indexes = {}

    async def put_record(self, record):
        primary_key = record[self._primary_key_name]
        self.records[primary_key] = record
        for index_name, score_function in self._score_functions.items():
            score = score_function(**record)
            self._indexes.setdefault(index_name, {}).setdefault(
                score, set()).add(primary_key)

    async def get_record(self, primary_key):
        return self._make_record(self.records[primary_key])

    async def get_records(self, records_spec):
        for record in self.records.values():
            if not records_spec.recheck_predicate(record):
                continue
            for interval in records_spec.score_intervals:
                score_function = self._score_functions[records_spec.index_name]
                if score_function(**record) in interval:
                    yield self._make_record(record)
                    break

    def _make_record(self, record):
        return record | {'source': 'storage'}

    async def delete_record(self, primary_key):
        del self.records[primary_key]
        self._delete_index_entries(primary_key)

    def _delete_index_entries(self, primary_key):
        for index in self._indexes.values():
            for score, primary_keys in list(index.items()):
                primary_keys.discard(primary_key)
                if not primary_keys:
                    del index[score]

    async def delete_records(self, records_spec):
        delete = [r async for r in self.get_records(records_spec)]
        for record in delete:
            await self.delete_record(record[self._primary_key_name])
        return len(delete)


class TestCachedTable:
    @pytest.fixture
    def indexes(self):
        return SpyIndexes()

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
        db_table.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key')
        assert_that(
            await table.get_record(1),
            has_entries(pk=1, k='v1', source='storage'))


    async def test_get_record_raises_on_nonexistent(self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key')
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_get_records_all(self, table, db_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(6))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(6)]))

    async def test_get_records_only_some(self, table, db_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))

    async def test_loads_only_specified_subset(
            self, table, db_table, get_storage_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        assert_that(
            await collect_async_iter(
                get_storage_table().get_records(
                    tc.StorageRecordsSpec(
                        'primary_key',
                        [tc.Interval(float('-inf'), float('inf'))]))),
            contains_inanyorder(*[has_entries(pk=i) for i in [2, 4]]))

    async def test_load_observes_loaded_records(
            self, table, db_table, indexes):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        expected_observations = await collect_async_iter(
            db_table.get_records('query_some_pks', (2, 4)))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(*[um.call(r) for r in expected_observations]))

    async def test_load_clears_storage_first(
            self, table, db_table, get_storage_table):
        db_table.records = [{'pk': 1, 'k': 'v1'}]
        get_storage_table().records = {2: {'pk': 2, 'k': 'v2'}}
        assert_that(await table.get_record(2), has_entries(k='v2'))
        await table.load('primary_key')
        assert_that(await table.get_record(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_load_adjusts_indexes(self, table, db_table, indexes):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        indexes.adjust_mock.assert_called_once_with('primary_key', 2, 4)

    async def test_load_raises_if_index_doesnt_support_adjusting(
            self, make_table, db_table):
        class Indexes(MultiIndexes):
            def adjust(self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().adjust(index_name, *index_args, **index_kwargs)
        table = make_table(Indexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        with pytest.raises(ValueError):
            await table.load('x_range', min=12, max=14)

    async def test_load_by_other_index(
            self, make_table, db_table, get_storage_table):
        table = make_table(MultiIndexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=12, max=14)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(2, 4)]))
        assert_that(
            await collect_async_iter(
                get_storage_table().get_records(
                    tc.StorageRecordsSpec(
                        'primary_key',
                        [tc.Interval(float('-inf'), float('inf'))]))),
            contains_inanyorder(*[has_entries(pk=i, x=i + 10)
                                  for i in range(2, 4)]))

    async def test_get_records_returns_db_state_if_not_cached(
            self, table, db_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', *range(2, 4))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 5))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2, 5)]))

    async def test_get_record_also_checks_db_in_case_not_cached(
            self, table, db_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', *range(2, 4))
        assert_that(await table.get_record(1), has_entries(pk=1, source='db'))

    async def test_get_record_doesnt_check_db_if_all_in_cache(
            self, table, db_table, get_storage_table):
        db_table.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 1)
        del get_storage_table().records[1]
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'v1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'v2'}]
        assert_that(await table.get_record(1), has_entries(pk=1, k='v1'))

    async def test_get_record_refreshes_existing_invalid_keys(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_records_refreshes_existing_invalid_keys(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_loads_new_invalid_keys(self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'a1'}, {'pk': 2, 'k': 'a2'}]
        await table.invalidate_record(2)
        assert_that(await table.get_record(2), has_entries(pk=2, k='a2'))

    async def test_get_records_loads_new_invalid_keys(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'a1'}, {'pk': 2, 'k': 'a2'}]
        await table.invalidate_record(2)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1, 2)),
            contains_inanyorder(
                has_entries(pk=1, k='a1'), has_entries(pk=2, k='a2')))

    async def test_get_record_only_refreshes_once(self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        await table.get_record(1)
        db_table.records = [{'pk': 1, 'k': 'c1'}]
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_records_only_refreshes_once(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        await collect_async_iter(table.get_records('primary_key', 1, 2))
        db_table.records = [{'pk': 1, 'k': 'c1'}]
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_deletes_invalid_keys(self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}, {'pk': 2, 'k': 'a2'}]
        await table.load('primary_key')
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_get_records_deletes_invalid_keys(
            self, table, db_table):
        db_table.records = [{'pk': i, 'k': f'a{i}'} for i in range(3)]
        await table.load('primary_key')
        db_table.records = [{'pk': 0, 'k': 'a0'}, {'pk': 2, 'k': 'a2'}]
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(3))),
            contains_inanyorder(
                has_entries(pk=0, k='a0'), has_entries(pk=2, k='a2')))

    async def test_get_records_uses_recheck_predicate(
            self, make_table, db_table):
        class RecheckOnlyIndexes(tc.PrimaryKeyIndexes):
            def __init__(self):
                super().__init__('pk', 'query_all_pks', 'query_some_pks')

            @property
            def score_functions(self):
                return {'primary_key': lambda **_: 0}

            def storage_records_spec(self, index_name, *primary_keys):
                return tc.StorageRecordsSpec(
                    index_name, [tc.Interval(0, 1)],
                    lambda r: r['pk'] in primary_keys)
        table = make_table(RecheckOnlyIndexes())
        db_table.records = [{'pk': i} for i in range(3)]
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 0, 2)),
            contains_inanyorder(
                has_entries(pk=0), has_entries(pk=2)))

    async def test_invalidate_record_observes_newly_added(
            self, table, db_table, indexes):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        new_record = {'pk': 2, 'k': 'a2'}
        db_table.records = [{'pk': 1, 'k': 'a1'}, new_record]
        await table.invalidate_record(2)
        indexes.observe_mock.assert_called_with(
            new_record | {'source': 'db'})

    async def test_invalidate_record_ignores_nonexistent_keys(
            self, table, db_table):
        db_table.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key')
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)
        assert_that(await table.get_record(1), has_entries(pk=1, k='a1'))

    async def test_adjust_cached_subset_prunes_old_data(self, table, db_table):
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', 0, 1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key', *range(2, 4))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(0, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(4)]))

    async def test_adjust_cached_subset_prunes_no_old_data(
            self, make_table, db_table):
        class Indexes(SpyIndexes):
            def adjust(self, *args, **kwargs):
                adjustment = super().adjust(*args)
                if 'delete_nothing' in kwargs:
                    assert adjustment.expire_spec is None
                return adjustment
        table = make_table(indexes=Indexes())
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', 0, 1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key', delete_nothing=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_loads_new_data(self, table, db_table):
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_loads_no_new_data(
            self, make_table, db_table):
        class Indexes(SpyIndexes):
            def adjust(self, index_name, *args, **kwargs):
                adjustment = super().adjust(index_name, *args)
                if 'load_nothing' in kwargs:
                    return tc.Adjustment(None, adjustment.expire_spec)
                return adjustment
        table = make_table(indexes=Indexes())
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust_cached_subset('primary_key', load_nothing=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))

    async def test_adjust_cached_subset_doesnt_introduce_duplicates(
            self, make_table, db_table):
        class Indexes(SpyIndexes):
            def adjust(self, index_name, *primary_keys):
                return tc.Adjustment(
                    None, self.db_query_range(index_name, *primary_keys))
        table = make_table(indexes=Indexes())
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        await table.adjust_cached_subset('primary_key', *range(4))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_cached_subset_observes_new_records(
            self, table, db_table, indexes):
        db_table.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        await table.adjust_cached_subset('primary_key', *range(4))
        expected_observations = await collect_async_iter(
            db_table.get_records('query_some_pks', (2, 3)))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(
                *[anything() for _ in range(4)],
                *[um.call(r) for r in expected_observations]))

    async def test_adjust_cached_subset_raises_if_index_doesnt_support(
            self, make_table, db_table):
        class Indexes(MultiIndexes):
            def adjust(self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().adjust(index_name, *index_args, **index_kwargs)
        table = make_table(Indexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(2))
        with pytest.raises(ValueError):
            await table.adjust_cached_subset('x_range', min=12, max=14)

    async def test_get_records_by_other_index(
            self, make_table, db_table):
        table = make_table(MultiIndexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(2, 4)]))

    async def test_get_records_raises_if_index_doesnt_support_covers(
            self, make_table, db_table):
        class Indexes(MultiIndexes):
            def covers(self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().adjust(index_name, *index_args, **index_kwargs)
        table = make_table(Indexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        with pytest.raises(ValueError):
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14))

    async def test_changing_scores_with_score_hint_dont_return_old_records(
            self, make_table, db_table):
        indexes = MultiIndexes()
        table = make_table(indexes)
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        db_table.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            new_score = indexes.score_functions['x_range'](x=primary_key + 100)
            await table.invalidate_record(primary_key, {'x_range': new_score})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)), empty())

    async def test_changing_scores_with_score_hint_return_new_records(
            self, make_table, db_table):
        indexes = MultiIndexes()
        table = make_table(indexes)
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        db_table.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            new_score = indexes.score_functions['x_range'](x=primary_key + 100)
            await table.invalidate_record(primary_key, {'x_range': new_score})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=100, max=106)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 100, source='storage')
                  for i in range(6)]))

    async def test_changing_scores_without_score_hint_dont_return_old_records(
            self, make_table, db_table):
        table = make_table(MultiIndexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        db_table.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            await table.invalidate_record(primary_key, {})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)), empty())

    async def test_changing_scores_without_score_hint_return_new_records(
            self, make_table, db_table):
        table = make_table(MultiIndexes())
        db_table.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key')
        db_table.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            await table.invalidate_record(primary_key, {})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=100, max=106)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 100, source='storage')
                  for i in range(6)]))


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

    def test_score_invalid(self, repo):
        assert not repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(float('-inf'), float('inf')))
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval(float('-inf'), float('inf')))
        repo.flag_invalid(1, {'primary_key': 2, 'x_range': 110})
        assert not repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(float('-inf'), 2))
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval(float('-inf'), 110))
        assert repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(2, 2.1))
        assert repo.interval_contains_invalid_score(
            'x_range', tc.Interval(110, 110.1))
        assert not repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(2.1, float('inf')))
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval(110.1, float('inf')))

    def test_score_invalid_raises_with_unknown_index_name(self, repo):
        with pytest.raises(KeyError):
            repo.interval_contains_invalid_score('no_such_index', 2)

    def test_primary_key_score_invalid_implicitly(self, repo):
        assert not repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(float('-inf'), float('inf')))
        repo.flag_invalid(1, {})
        assert repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(2, 2.1))

    def test_score_invalid_raises_on_dirty_index(self, repo):
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval(float('-inf'), float('inf')))
        repo.flag_invalid(1, {})
        with pytest.raises(tc.DirtyIndex):
            repo.interval_contains_invalid_score('x_range', 110)

    def test_primary_key_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, {})
        repo.clear()
        assert not repo.primary_key_is_invalid(1)
        assert 1 not in repo.invalid_primary_keys

    def test_score_not_invalid_after_clear(self, repo):
        repo.flag_invalid(1, {'primary_key': 2, 'x_range': 110})
        repo.clear()
        assert not repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(float('-inf'), float('inf')))
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval(float('-inf'), float('inf')))

    def test_index_not_dirty_after_clean(self, repo):
        repo.flag_invalid(1, {})
        repo.clear()
        repo.interval_contains_invalid_score(
            'x_range', tc.Interval(float('-inf'), float('inf')))

    def test_len_is_number_of_primary_keys(self, repo):
        assert len(repo) == 0
        repo.flag_invalid(1, {})
        repo.flag_invalid(2, {})
        assert len(repo) == 2
        repo.flag_invalid(3, {})
        assert len(repo) == 3
