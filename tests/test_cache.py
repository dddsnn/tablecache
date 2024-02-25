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
import dataclasses as dc
import numbers
import queue
import unittest.mock as um
import threading

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
        self.prepare_adjustment_mock = um.Mock(return_values=[])
        self.commit_adjustment_mock = um.Mock()
        self.observe_mock = um.Mock()

    def prepare_adjustment(self, *args, **kwargs):
        result = super().prepare_adjustment(*args, **kwargs)
        self.prepare_adjustment_mock(*args, **kwargs)
        self.prepare_adjustment_mock.return_values.append(result)
        self.prepare_adjustment_mock.return_value = result
        return result

    def commit_adjustment(self, *args, **kwargs):
        result = super().commit_adjustment(*args, **kwargs)
        self.commit_adjustment_mock(*args, **kwargs)
        return result

    def observe(self, *args, **kwargs):
        result = super().observe(*args, **kwargs)
        self.observe_mock(*args, **kwargs)
        return result


class MultiIndexes(tc.Indexes[int]):
    @dc.dataclass(frozen=True)
    class Adjustment(tc.Adjustment):
        primary_keys: set[numbers.Real]
        range: tuple[numbers.Real, numbers.Real]

    def __init__(self):
        self._contains_all = False
        self._primary_keys = set()
        self._range = None

    @property
    def index_names(self):
        return frozenset(['primary_key', 'x_range'])

    def score(self, index_name, record):
        if index_name == 'primary_key':
            return self.primary_key_score(record['pk'])
        elif index_name == 'x_range':
            return record['x'] + 100
        raise ValueError

    def primary_key_score(self, primary_key):
        return primary_key + 1

    def primary_key(self, record):
        try:
            return record['pk']
        except KeyError:
            raise ValueError

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

    def db_records_spec(self, index_name, *args, **kwargs):
        if index_name == 'primary_key':
            if args:
                return tc.QueryArgsDbRecordsSpec('query_some_pks', (args,))
            return tc.QueryArgsDbRecordsSpec('query_all_pks', ())
        if index_name == 'x_range':
            return tc.QueryArgsDbRecordsSpec(
                'query_x_range', (kwargs['min'], kwargs['max']))
        raise NotImplementedError

    def prepare_adjustment(self, index_name, *args, **kwargs):
        expire_spec = tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()])
        if index_name == 'primary_key':
            new_spec = self.db_records_spec('primary_key', *args)
            primary_keys = set(args)
            range_ = None
        elif index_name == 'x_range':
            new_spec = self.db_records_spec('x_range', **kwargs)
            primary_keys = None
            range_ = (kwargs['min'], kwargs['max'])
        else:
            raise NotImplementedError
        return self.Adjustment(expire_spec, new_spec, primary_keys, range_)

    def commit_adjustment(self, adjustment):
        if adjustment.primary_keys is not None:
            assert adjustment.range is None
            self._contains_all = adjustment.primary_keys == set()
            self._primary_keys = adjustment.primary_keys
        else:
            assert adjustment.range is not None
            self._contains_all = False
            self._primary_keys = set()
        self._range = adjustment.range

    def covers(self, index_name, *args, **kwargs):
        if self._contains_all:
            return True
        if index_name == 'primary_key':
            return args and all(pk in self._primary_keys for pk in args)
        if index_name == 'x_range':
            if self._range is None:
                return False
            loaded_ge, loaded_lt = self._range
            ge, lt = kwargs['min'], kwargs['max']
            return loaded_ge <= ge <= lt <= loaded_lt
        raise NotImplementedError

    def observe(self, record):
        self._primary_keys.add(record['pk'])


class MockDbAccess(tc.DbAccess):
    def __init__(self):
        self.records = []

    async def get_records(self, records_spec):
        if not records_spec.args:
            assert records_spec.query == 'query_all_pks'
            def record_matches(_): return True
        elif records_spec.query == 'query_some_pks':
            assert_that(
                records_spec.args,
                all_of(
                    instance_of(tuple),
                    contains_exactly(instance_of(tuple))))

            def record_matches(r): return r['pk'] in records_spec.args[0]
        else:
            assert records_spec.query == 'query_x_range'
            assert_that(
                records_spec.args,
                all_of(
                    instance_of(tuple),
                    contains_exactly(
                        instance_of(numbers.Real), instance_of(numbers.Real))))
            ge, lt = records_spec.args
            def record_matches(r): return ge <= r['x'] < lt
        for record in self.records:
            if record_matches(record):
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable(tc.StorageTable):
    def __init__(self, *, record_scorer):
        self._record_scorer = record_scorer
        self.records = {}
        self._indexes = {}
        self._scratch_records = {}
        self._scratch_pks_delete = set()
        self._num_scratch_ops = 0
        self._wait_merge = False
        self._merge_started_event = threading.Event()
        self._merge_continue_event = threading.Event()

    def _enable_merge_wait(self):
        self._wait_merge = True

    def _merge_wait_start(self):
        self._merge_started_event.wait()

    def _merge_continue(self):
        self._merge_continue_event.set()

    async def clear(self):
        self.records = {}
        self._indexes = {}

    async def put_record(self, record):
        primary_key = self._record_scorer.primary_key(record)
        self.records[primary_key] = record
        for index_name in self._record_scorer.index_names:
            score = self._record_scorer.score(index_name, record)
            self._indexes.setdefault(index_name, {}).setdefault(
                score, set()).add(primary_key)

    async def get_record(self, primary_key):
        return self._make_record(self.records[primary_key])

    async def get_records(self, records_spec):
        for record in self.records.values():
            if not records_spec.recheck_predicate(record):
                continue
            for interval in records_spec.score_intervals:
                score = self._record_scorer.score(
                    records_spec.index_name, record)
                if score in interval:
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
            await self.delete_record(self._record_scorer.primary_key(record))
        return len(delete)

    async def scratch_put_record(self, record):
        self._num_scratch_ops += 1
        primary_key = self._record_scorer.primary_key(record)
        self._scratch_records[primary_key] = self._make_record(record)
        self._scratch_pks_delete.discard(primary_key)

    async def scratch_discard_records(self, records_spec):
        self._num_scratch_ops += 1
        num_discarded = 0
        async for record in self.get_records(records_spec):
            primary_key = self._record_scorer.primary_key(record)
            self._scratch_records.pop(primary_key, None)
            self._scratch_pks_delete.add(primary_key)
            num_discarded += 1
        return num_discarded

    def scratch_merge(self):
        if self._wait_merge:
            self._merge_started_event.set()
        if self._wait_merge:
            self._merge_continue_event.wait()
        for primary_key in self._scratch_pks_delete:
            self.records.pop(primary_key, None)
        self.records.update(self._scratch_records)
        self._scratch_pks_delete.clear()
        self._scratch_records.clear()
        self._merge_started_event.clear()
        self._merge_continue_event.clear()


class TestCachedTable:
    @pytest.fixture
    def indexes(self):
        return SpyIndexes()

    @pytest.fixture
    def db_access(self):
        return MockDbAccess()

    @pytest.fixture
    def make_tables(self, indexes, db_access):
        def factory(indexes=indexes):
            storage_table = MockStorageTable(record_scorer=indexes)
            cached_table = tc.CachedTable(indexes, db_access, storage_table)
            return cached_table, storage_table

        return factory

    @pytest.fixture
    def table(self, make_tables):
        table, _ = make_tables()
        return table

    async def test_load_and_get_record(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await table.get_record(1),
            has_entries(pk=1, k='v1', source='storage'))

    async def test_get_record_raises_on_nonexistent(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_get_records_all(self, table, db_access):
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(6))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(6)]))

    async def test_get_records_only_some(self, table, db_access):
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))

    async def test_loads_only_specified_subset(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        assert_that(
            await collect_async_iter(
                storage_table.get_records(
                    tc.StorageRecordsSpec(
                        'primary_key', [tc.Interval.everything()]))),
            contains_inanyorder(*[has_entries(pk=i) for i in [2, 4]]))

    async def test_load_observes_loaded_records(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        expected_observations = await collect_async_iter(
            db_access.get_records(
                tc.QueryArgsDbRecordsSpec('query_some_pks', ((2, 4),))))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(*[um.call(r) for r in expected_observations]))

    async def test_load_clears_storage_first(self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': 1, 'k': 'v1'}]
        storage_table.records = {2: {'pk': 2, 'k': 'v2'}}
        assert_that(await storage_table.get_record(2), has_entries(k='v2'))
        await table.load('primary_key', all_primary_keys=True)
        assert_that(await table.get_record(1), has_entries(k='v1'))
        assert_that(await storage_table.get_record(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get_record(2)
        with pytest.raises(KeyError):
            await storage_table.get_record(2)

    async def test_load_adjusts_indexes(self, table, db_access, indexes):
        mock = um.Mock()
        mock.prepare = indexes.prepare_adjustment_mock
        mock.commit = indexes.commit_adjustment_mock
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        assert mock.mock_calls == [
            um.call.prepare('primary_key', 2, 4),
            um.call.commit(indexes.prepare_adjustment_mock.return_value)]

    async def test_load_calls_observes_after_commit_adjustment(
            self, table, db_access, indexes):
        mock = um.Mock()
        mock.prepare = indexes.prepare_adjustment_mock
        mock.commit = indexes.commit_adjustment_mock
        mock.observe = indexes.observe_mock
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 2, 4)
        assert mock.mock_calls == [
            um.call.prepare('primary_key', 2, 4),
            um.call.commit(indexes.prepare_adjustment_mock.return_value),
            um.call.observe(um.ANY), um.call.observe(um.ANY)]

    async def test_load_raises_if_index_doesnt_support_adjusting(
            self, make_tables, db_access):
        class Indexes(MultiIndexes):
            def prepare_adjustment(
                    self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().prepare_adjustment(
                    index_name, *index_args, **index_kwargs)
        table, _ = make_tables(Indexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        with pytest.raises(ValueError):
            await table.load('x_range', min=12, max=14)

    async def test_load_by_other_index(self, make_tables, db_access):
        table, storage_table = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=12, max=14)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(2, 4)]))
        assert_that(
            await collect_async_iter(
                storage_table.get_records(
                    tc.StorageRecordsSpec(
                        'primary_key', [tc.Interval.everything()]))),
            contains_inanyorder(*[has_entries(pk=i, x=i + 10)
                                  for i in range(2, 4)]))

    async def test_load_raises_if_already_loaded(self, table):
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            await table.load('primary_key', all_primary_keys=True)

    async def test_get_records_returns_db_state_if_not_cached(
            self, table, db_access):
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', *range(2, 4))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2, 5))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2, 5)]))

    async def test_get_record_also_checks_db_in_case_not_cached(
            self, table, db_access):
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', *range(2, 4))
        assert_that(await table.get_record(1), has_entries(pk=1, source='db'))

    async def test_get_record_doesnt_check_db_if_all_in_cache(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(6)]
        await table.load('primary_key', 1)
        del storage_table.records[1]
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'v2'}]
        assert_that(await table.get_record(1), has_entries(pk=1, k='v1'))

    async def test_get_record_refreshes_existing_invalid_keys(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_records_refreshes_existing_invalid_keys(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_only_refreshes_once(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        await table.get_record(1)
        db_access.records = [{'pk': 1, 'k': 'c1'}]
        assert_that(await table.get_record(1), has_entries(pk=1, k='b1'))

    async def test_get_records_only_refreshes_once(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        await table.invalidate_record(1)
        await collect_async_iter(table.get_records('primary_key', 1, 2))
        db_access.records = [{'pk': 1, 'k': 'c1'}]
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_record_deletes_invalid_keys(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}, {'pk': 2, 'k': 'a2'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.invalidate_record(2)
        with pytest.raises(KeyError):
            await table.get_record(2)

    async def test_get_records_deletes_invalid_keys(
            self, table, db_access):
        db_access.records = [{'pk': i, 'k': f'a{i}'} for i in range(3)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 0, 'k': 'a0'}, {'pk': 2, 'k': 'a2'}]
        await table.invalidate_record(1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(3))),
            contains_inanyorder(
                has_entries(pk=0, k='a0'), has_entries(pk=2, k='a2')))

    async def test_get_records_uses_recheck_predicate(
            self, make_tables, db_access):
        class RecheckOnlyIndexes(tc.PrimaryKeyIndexes):
            def __init__(self):
                super().__init__('pk', 'query_all_pks', 'query_some_pks')

            def score(self, index_name, record):
                return 0

            def primary_key_score(self, primary_key):
                return 0

            def storage_records_spec(self, index_name, *primary_keys):
                return tc.StorageRecordsSpec(
                    index_name, [tc.Interval(0, 1)],
                    lambda r: r['pk'] in primary_keys)
        table, _ = make_tables(RecheckOnlyIndexes())
        db_access.records = [{'pk': i} for i in range(3)]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 0, 2)),
            contains_inanyorder(
                has_entries(pk=0), has_entries(pk=2)))

    async def test_get_record_blocks_while_not_loaded(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        get_task = asyncio.create_task(table.get_record(1))
        load_wait_task = asyncio.create_task(table.loaded())
        await asyncio.sleep(0.001)
        assert not get_task.done()
        assert not load_wait_task.done()
        await table.load('primary_key', all_primary_keys=True)
        await get_task
        await load_wait_task

    async def test_get_records_blocks_while_not_loaded(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        get_task = asyncio.create_task(
            collect_async_iter(
                table.get_records('primary_key', all_primary_keys=True)))
        load_wait_task = asyncio.create_task(table.loaded())
        await asyncio.sleep(0.001)
        assert not get_task.done()
        assert not load_wait_task.done()
        await table.load('primary_key', all_primary_keys=True)
        await get_task
        await load_wait_task

    async def test_invalidate_record_raises_on_nonexistent(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(KeyError):
            await table.invalidate_record(2)

    async def test_invalidate_record_blocks_while_not_loaded(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        invalidate_task = asyncio.create_task(table.invalidate_record(1))
        load_wait_task = asyncio.create_task(table.loaded())
        await asyncio.sleep(0.001)
        assert not invalidate_task.done()
        assert not load_wait_task.done()
        await table.load('primary_key', all_primary_keys=True)
        await invalidate_task
        await load_wait_task

    async def test_adjust_discards_old_data(self, table, db_access):
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', 0, 1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust('primary_key', *range(2, 4))
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

    async def test_adjust_discards_no_old_data(self, make_tables, db_access):
        class Indexes(SpyIndexes):
            def prepare_adjustment(self, *args, **kwargs):
                delete_nothing = kwargs.pop('delete_nothing', False)
                adjustment = super().prepare_adjustment(*args, **kwargs)
                if delete_nothing:
                    assert adjustment.expire_spec is None
                return adjustment
        table, _ = make_tables(indexes=Indexes())
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', 0, 1)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust(
            'primary_key', all_primary_keys=True, delete_nothing=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_loads_new_data(self, table, db_access):
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_loads_no_new_data(self, make_tables, db_access):
        class Indexes(SpyIndexes):
            def prepare_adjustment(self, *args, **kwargs):
                load_nothing = kwargs.pop('load_nothing', False)
                adjustment = super().prepare_adjustment(*args, **kwargs)
                if load_nothing:
                    adjustment = dc.replace(adjustment, new_spec=None)
                return adjustment
        table, _ = make_tables(indexes=Indexes())
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))
        await table.adjust(
            'primary_key', all_primary_keys=True, load_nothing=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2)]))

    async def test_adjust_doesnt_introduce_duplicates(
            self, make_tables, db_access):
        class Indexes(SpyIndexes):
            def adjust(self, index_name, *primary_keys):
                return tc.Adjustment(
                    None, self.db_records_spec(index_name, *primary_keys))
        table, _ = make_tables(indexes=Indexes())
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        await table.adjust('primary_key', *range(4))
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_observes_new_records(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        await table.adjust('primary_key', *range(4))
        expected_observations = await collect_async_iter(
            db_access.get_records(
                tc.QueryArgsDbRecordsSpec('query_some_pks', ((2, 3),))))
        assert_that(
            indexes.observe_mock.call_args_list,
            contains_inanyorder(
                *[anything() for _ in range(4)],
                *[um.call(r) for r in expected_observations]))

    async def test_adjust_observes_new_records_after_commit_adjustment(
            self, table, db_access, indexes):
        mock = um.Mock()
        mock.prepare = indexes.prepare_adjustment_mock
        mock.commit = indexes.commit_adjustment_mock
        mock.observe = indexes.observe_mock
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        await table.adjust('primary_key', *range(4))
        assert mock.mock_calls == [
            um.call.prepare('primary_key', *range(2)),
            um.call.commit(indexes.prepare_adjustment_mock.return_values[0]),
            um.call.observe(um.ANY), um.call.observe(um.ANY),
            um.call.prepare('primary_key', *range(4)),
            um.call.commit(indexes.prepare_adjustment_mock.return_values[1]),
            um.call.observe(um.ANY), um.call.observe(um.ANY),
            um.call.observe(um.ANY), um.call.observe(um.ANY)]

    async def test_adjust_by_other_index(self, make_tables, db_access):
        table, _ = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(4))
        await table.adjust('x_range', min=12, max=16)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=16)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(2, 6)]))
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=10, max=16)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='db')
                  for i in range(6)]))

    async def test_adjust_raises_if_index_doesnt_support_adjusting(
            self, make_tables, db_access):
        class Indexes(MultiIndexes):
            def prepare_adjustment(
                    self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().prepare_adjustment(
                    index_name, *index_args, **index_kwargs)
        table, _ = make_tables(Indexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(2))
        with pytest.raises(ValueError):
            await table.adjust('x_range', min=12, max=14)

    async def test_load_doesnt_use_scratch_space(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        assert storage_table._num_scratch_ops == 0

    async def test_adjust_uses_scratch_space_for_discarding(
            self, make_tables, db_access):
        # Ok, I know this looks a bit janky, but we need to assert things about
        # the cache just before StorageTable.scratch_merge() returns and just
        # after. And since that's not async, we have to wait on an event in our
        # mock which blocks the entire main thread. So we have to start a
        # second thread that makes the asserts while the main thread is frozen,
        # and then unfreezes it.
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            adjust_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await collect_async_iter(
                        table.get_records('primary_key', *range(2))),
                    contains_inanyorder(
                        *[has_entries(pk=i, source='storage')
                          for i in range(2)]))
            except Exception as e:
                exceptions.append(e)
            assert not adjust_task.done()
            storage_table._merge_continue()
        t = threading.Thread(target=asyncio.run, args=(assert_pre_merge(),))
        t.start()
        adjust_task = asyncio.create_task(
            table.adjust('primary_key', *range(2, 4)))
        task_queue.put(adjust_task)
        await adjust_task
        t.join()
        for e in exceptions:
            raise e
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(2))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2)]))

    async def test_adjust_uses_scratch_space_for_adding(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(2))
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            adjust_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await collect_async_iter(
                        table.get_records('primary_key', *range(4))),
                    contains_inanyorder(
                        *[has_entries(pk=i, source='db')for i in range(4)]))
            except Exception as e:
                exceptions.append(e)
            assert not adjust_task.done()
            storage_table._merge_continue()
        t = threading.Thread(target=asyncio.run, args=(assert_pre_merge(),))
        t.start()
        adjust_task = asyncio.create_task(
            table.adjust('primary_key', *range(4)))
        task_queue.put(adjust_task)
        await adjust_task
        t.join()
        for e in exceptions:
            raise e
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(4)]))

    async def test_adjust_blocks_while_not_loaded(self, table, db_access):
        db_access.records = [{'pk': i} for i in range(4)]
        adjust_task = asyncio.create_task(
            table.adjust('primary_key', *range(2, 4)))
        load_wait_task = asyncio.create_task(table.loaded())
        await asyncio.sleep(0.001)
        assert not adjust_task.done()
        assert not load_wait_task.done()
        await table.load('primary_key', *range(2))
        await adjust_task
        await load_wait_task

    async def test_adjust_refreshes_first(self, table, db_access):
        db_access.records = [{'pk': i, 's': f'a{i}'} for i in range(2)]
        await table.load('primary_key', 0)
        db_access.records = [{'pk': i, 's': f'b{i}'} for i in range(4)]
        await table.invalidate_record(0)
        await table.adjust('primary_key', 1)
        assert_that(
            await table.get_record(0), has_entries(s='b0', source='db'))

    async def test_get_records_by_other_index(
            self, make_tables, db_access):
        table, _ = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(2, 4)]))

    async def test_get_records_raises_if_index_doesnt_support_covers(
            self, make_tables, db_access):
        class Indexes(MultiIndexes):
            def covers(self, index_name, *index_args, **index_kwargs):
                if index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().adjust(index_name, *index_args, **index_kwargs)
        table, _ = make_tables(Indexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14))

    async def test_changing_scores_with_score_hint_dont_return_old_records(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            new_score = indexes.score('x_range', {'x': primary_key + 100})
            await table.invalidate_record(primary_key, {'x_range': new_score})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)), empty())

    async def test_changing_scores_with_score_hint_return_new_records(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            new_score = indexes.score('x_range', {'x': primary_key + 100})
            await table.invalidate_record(primary_key, {'x_range': new_score})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=100, max=106)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 100, source='storage')
                  for i in range(6)]))

    async def test_changing_scores_without_score_hint_dont_return_old_records(
            self, make_tables, db_access):
        table, _ = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            await table.invalidate_record(primary_key, {})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)), empty())

    async def test_changing_scores_without_score_hint_return_new_records(
            self, make_tables, db_access):
        table, _ = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        for primary_key in range(6):
            await table.invalidate_record(primary_key, {})
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=100, max=106)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 100, source='storage')
                  for i in range(6)]))

    async def test_refresh_invalid_uses_scratch_space_for_discarding(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i} for i in range(3)]
        await table.invalidate_record(3)
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await storage_table.get_record(3), has_entries(pk=3))
            except Exception as e:
                exceptions.append(e)
            assert not refresh_task.done()
            storage_table._merge_continue()
        t = threading.Thread(target=asyncio.run, args=(assert_pre_merge(),))
        t.start()
        refresh_task = asyncio.create_task(table.refresh_invalid())
        task_queue.put(refresh_task)
        await refresh_task
        t.join()
        for e in exceptions:
            raise e
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_refresh_invalid_uses_scratch_space_for_updating(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': 0, 's': 'x'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 0, 's': 'y'}]
        await table.invalidate_record(0)
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await storage_table.get_record(0), has_entries(s='x'))
            except Exception as e:
                exceptions.append(e)
            assert not refresh_task.done()
            storage_table._merge_continue()
        t = threading.Thread(target=asyncio.run, args=(assert_pre_merge(),))
        t.start()
        refresh_task = asyncio.create_task(table.refresh_invalid())
        task_queue.put(refresh_task)
        await refresh_task
        t.join()
        for e in exceptions:
            raise e
        assert_that(
            await table.get_record(0), has_entries(s='y', source='storage'))

    async def test_refresh_invalid_doesnt_block_gets_for_valid_keys(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i} for i in range(3)]
        await table.invalidate_record(3)
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(await table.get_record(2), has_entries(pk=2))
            except Exception as e:
                exceptions.append(e)
            assert not refresh_task.done()
            storage_table._merge_continue()
        t = threading.Thread(target=asyncio.run, args=(assert_pre_merge(),))
        t.start()
        refresh_task = asyncio.create_task(table.refresh_invalid())
        task_queue.put(refresh_task)
        await refresh_task
        t.join()
        for e in exceptions:
            raise e


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
            'primary_key', tc.Interval.everything())
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval.everything())
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
            'primary_key', tc.Interval.everything())
        repo.flag_invalid(1, {})
        assert repo.interval_contains_invalid_score(
            'primary_key', tc.Interval(2, 2.1))

    def test_score_invalid_raises_on_dirty_index(self, repo):
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval.everything())
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
            'primary_key', tc.Interval.everything())
        assert not repo.interval_contains_invalid_score(
            'x_range', tc.Interval.everything())

    def test_index_not_dirty_after_clean(self, repo):
        repo.flag_invalid(1, {})
        repo.clear()
        repo.interval_contains_invalid_score(
            'x_range', tc.Interval.everything())

    def test_len_is_number_of_primary_keys(self, repo):
        assert len(repo) == 0
        repo.flag_invalid(1, {})
        repo.flag_invalid(2, {})
        assert len(repo) == 2
        repo.flag_invalid(3, {})
        assert len(repo) == 3
