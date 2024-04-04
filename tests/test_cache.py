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
import collections.abc as ca
import itertools as it
import numbers
import operator as op
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
    class IndexSpec(tc.PrimaryKeyIndexes.IndexSpec):
        def __eq__(self, other):
            return (self.index_name == other.index_name and
                    self.primary_keys == other.primary_keys and
                    self.all_primary_keys == other.all_primary_keys)

    def __init__(self):
        super().__init__(
            op.itemgetter('pk'), 'query_all_pks', 'query_some_pks')
        self.prepare_adjustment_mock = um.Mock(return_values=[])
        self.commit_adjustment_mock = um.Mock()
        self.adjustments = []

    def prepare_adjustment(self, *args, **kwargs):
        self.prepare_adjustment_mock(*args, **kwargs)
        real_adjustment = super().prepare_adjustment(*args, **kwargs)
        self.adjustments.append(
            um.Mock(wraps=real_adjustment, **real_adjustment.__dict__))
        self.prepare_adjustment_mock.return_value = self.adjustments[-1]
        return self.adjustments[-1]

    def commit_adjustment(self, *args, **kwargs):
        result = super().commit_adjustment(*args, **kwargs)
        self.commit_adjustment_mock(*args, **kwargs)
        return result


class MultiIndexes(tc.Indexes[dict, int]):
    class IndexSpec(tc.Indexes[dict, int].IndexSpec):
        def __init__(
                self, index_name, *primary_keys, all_primary_keys=False,
                min=None, max=None):
            if index_name == 'primary_key':
                assert min is None and max is None
            elif index_name == 'x_range':
                assert (not primary_keys and not all_primary_keys and
                        None not in [min, max])
            else:
                raise ValueError
            super().__init__(index_name)
            self.primary_keys = set(primary_keys)
            self.all_primary_keys = all_primary_keys
            self.min = min
            self.max = max

    class Adjustment(tc.Adjustment):
        def __init__(
                self, expire_spec, load_spec, all_primary_keys, primary_keys,
                range):
            super().__init__(expire_spec, load_spec)
            self.all_primary_keys = all_primary_keys
            self.primary_keys = primary_keys
            self.range = range
            self.expired_records, self.loaded_records = [], []

        def observe_loaded(self, record):
            self.loaded_records.append(record)

    def __init__(self):
        self._contains_all = False
        self._primary_keys = set()
        self._range = None

    @property
    def index_names(self):
        return frozenset(['primary_key', 'x_range'])

    def score(self, index_name, record):
        if index_name == 'primary_key':
            return self.primary_key(record) + 1
        elif index_name == 'x_range':
            return record['x'] + 100
        raise ValueError

    def primary_key(self, record):
        try:
            return record['pk']
        except KeyError:
            raise ValueError

    def storage_records_spec(self, spec):
        if spec.index_name == 'primary_key':
            if spec.all_primary_keys:
                intervals = [tc.Interval.everything()]
            else:
                intervals = [
                    tc.Interval(pk + 1, pk + 1.1) for pk in spec.primary_keys]
            return tc.StorageRecordsSpec(spec.index_name, intervals)
        if spec.index_name == 'x_range':
            ge = spec.min
            lt = spec.max
            mid = ge + (lt - ge) / 2
            return tc.StorageRecordsSpec(
                spec.index_name, [
                    tc.Interval(ge + 100, mid + 100),
                    tc.Interval(mid + 100, lt + 100)])
        raise NotImplementedError

    def db_records_spec(self, spec):
        if spec.index_name == 'primary_key':
            if spec.all_primary_keys:
                return tc.QueryArgsDbRecordsSpec('query_all_pks', ())
            return tc.QueryArgsDbRecordsSpec(
                'query_some_pks', (spec.primary_keys,))
        if spec.index_name == 'x_range':
            return tc.QueryArgsDbRecordsSpec(
                'query_x_range', (spec.min, spec.max))
        raise NotImplementedError

    def prepare_adjustment(self, spec):
        expire_spec = tc.StorageRecordsSpec(
            'primary_key', [tc.Interval.everything()])
        load_spec = self.db_records_spec(spec)
        if spec.index_name == 'primary_key':
            primary_keys = spec.primary_keys
            range_ = None
        elif spec.index_name == 'x_range':
            primary_keys = None
            range_ = (spec.min, spec.max)
        else:
            raise NotImplementedError
        return self.Adjustment(
            expire_spec, load_spec, bool(spec.all_primary_keys), primary_keys,
            range_)

    def commit_adjustment(self, adjustment):
        self._primary_keys.difference_update(
            self.primary_key(r) for r in adjustment.expired_records)
        if adjustment.primary_keys is not None:
            assert adjustment.range is None
            self._contains_all = adjustment.all_primary_keys
        else:
            assert adjustment.range is not None
            self._contains_all = False
        self._primary_keys = adjustment.primary_keys or set()
        self._range = adjustment.range
        self._primary_keys.update(
            self.primary_key(r) for r in adjustment.loaded_records)

    def covers(self, spec):
        if self._contains_all:
            return True
        if spec.all_primary_keys:
            return False
        if spec.index_name == 'primary_key':
            return (
                spec.primary_keys and
                all(pk in self._primary_keys for pk in spec.primary_keys))
        if spec.index_name == 'x_range':
            if self._range is None:
                return False
            loaded_ge, loaded_lt = self._range
            ge, lt = spec.min, spec.max
            return loaded_ge <= ge <= lt <= loaded_lt
        raise NotImplementedError


class MockDbAccess(tc.DbAccess):
    def __init__(self):
        self.records = []

    async def get_records(self, records_spec):
        if not records_spec.args:
            assert records_spec.query == 'query_all_pks'
            def record_matches(_): return True
        elif records_spec.query == 'query_some_pks':
            assert_that(
                records_spec.args, contains_exactly(instance_of(ca.Iterable)))

            def record_matches(r): return r['pk'] in records_spec.args[0]
        else:
            assert records_spec.query == 'query_x_range'
            assert_that(
                records_spec.args,
                contains_exactly(
                    instance_of(numbers.Real), instance_of(numbers.Real)))
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

    @property
    def name(self):
        return 'mock_table'

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

    def _delete_index_entries(self, primary_key):
        for index in self._indexes.values():
            for score, primary_keys in list(index.items()):
                primary_keys.discard(primary_key)
                if not primary_keys:
                    del index[score]

    async def delete_records(self, records_spec):
        async for record in self.get_records(records_spec):
            await self.delete_record(self._record_scorer.primary_key(record))
            yield record

    async def scratch_put_record(self, record):
        self._num_scratch_ops += 1
        primary_key = self._record_scorer.primary_key(record)
        self._scratch_records[primary_key] = self._make_record(record)
        self._scratch_pks_delete.discard(primary_key)

    async def scratch_discard_records(self, records_spec):
        self._num_scratch_ops += 1
        async for record in self.get_records(records_spec):
            primary_key = self._record_scorer.primary_key(record)
            self._scratch_records.pop(primary_key, None)
            self._scratch_pks_delete.add(primary_key)
            yield record

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

    def table_has_invalid_records(self, table):
        return bool(table._invalid_record_repo)

    async def test_load_and_get_first_record(self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await table.get_first_record('primary_key', 1),
            has_entries(pk=1, k='v1', source='storage'))

    async def test_get_first_record_raises_on_nonexistent(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}, {'pk': 2, 'k': 'v2'}]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(KeyError):
            await table.get_first_record('primary_key', 3)

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
        expected_loaded = await collect_async_iter(
            db_access.get_records(
                tc.QueryArgsDbRecordsSpec('query_some_pks', ((2, 4),))))
        assert_that(
            indexes.adjustments, contains_exactly(has_properties(
                observe_expired=has_properties(call_args_list=[]),
                observe_loaded=has_properties(
                    call_args_list=contains_inanyorder(
                        *[um.call(r) for r in expected_loaded])))))

    async def test_load_clears_storage_first(self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': 1, 'k': 'v1'}]
        storage_table.records = {2: {'pk': 2, 'k': 'v2'}}
        assert_that(
            await collect_async_iter(storage_table.get_records(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval.only_containing(2)]))),
            contains_exactly(has_entries(k='v2')))
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await table.get_first_record('primary_key', 1),
            has_entries(k='v1'))
        assert_that(
            await collect_async_iter(storage_table.get_records(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval.only_containing(1)]))),
            contains_exactly(has_entries(k='v1')))
        with pytest.raises(KeyError):
            await table.get_first_record('primary_key', 2)
        assert_that(
            await collect_async_iter(storage_table.get_records(
                tc.StorageRecordsSpec(
                    'primary_key', [tc.Interval.only_containing(2)]))),
            empty())

    async def test_load_adjusts_indexes(self, table, db_access, indexes):
        mock = um.Mock()
        mock.prepare = indexes.prepare_adjustment_mock
        mock.commit = indexes.commit_adjustment_mock
        db_access.records = [{'pk': i} for i in range(6)]
        index_spec = indexes.IndexSpec('primary_key', 2, 4)
        await table.load(index_spec)
        assert mock.mock_calls == [
            um.call.prepare(index_spec),
            *[um.call.prepare().observe_loaded(um.ANY) for _ in range(2)],
            um.call.commit(indexes.prepare_adjustment_mock.return_value)]

    async def test_load_raises_if_index_doesnt_support_adjusting(
            self, make_tables, db_access):
        class Indexes(MultiIndexes):
            def prepare_adjustment(self, spec):
                if spec.index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().prepare_adjustment(spec)
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

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_access):
        db_access.records = [{'pk': 1, 'k': 'v1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'v2'}]
        assert_that(
            await table.get_first_record('primary_key', 1),
            has_entries(pk=1, k='v1'))

    async def test_get_records_refreshes_existing_invalid_keys(
            self, table, db_access, indexes):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 1)],
            [indexes.IndexSpec('primary_key', 1)])
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_records_only_refreshes_once(
            self, table, db_access, indexes):
        db_access.records = [{'pk': 1, 'k': 'a1'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 1, 'k': 'b1'}]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 1)],
            [indexes.IndexSpec('primary_key', 1)])
        await collect_async_iter(table.get_records('primary_key', 1, 2))
        db_access.records = [{'pk': 1, 'k': 'c1'}]
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 1, 2)),
            contains_inanyorder(has_entries(pk=1, k='b1')))

    async def test_get_records_deletes_invalid_keys(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'k': f'a{i}'} for i in range(3)]
        await table.load('primary_key', all_primary_keys=True)
        del db_access.records[1]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 1)],
            [indexes.IndexSpec('primary_key', 1)])
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', *range(3))),
            contains_inanyorder(
                has_entries(pk=0, k='a0'), has_entries(pk=2, k='a2')))

    async def test_get_records_uses_recheck_predicate(
            self, make_tables, db_access):
        class RecheckOnlyIndexes(tc.PrimaryKeyIndexes):
            def __init__(self):
                super().__init__(
                    op.itemgetter('pk'), 'query_all_pks', 'query_some_pks')

            def score(self, index_name, record):
                return 0

            def storage_records_spec(self, spec):
                return tc.StorageRecordsSpec(
                    spec.index_name, [tc.Interval(0, 1)],
                    lambda r: r['pk'] in spec.primary_keys)
        table, _ = make_tables(RecheckOnlyIndexes())
        db_access.records = [{'pk': i} for i in range(3)]
        await table.load('primary_key', all_primary_keys=True)
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', 0, 2)),
            contains_inanyorder(
                has_entries(pk=0), has_entries(pk=2)))

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

    async def test_invalidate_record_raises_if_not_loaded(
            self, table, indexes):
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', 1)],
                [indexes.IndexSpec('primary_key', 1)])

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
            class IndexSpec(SpyIndexes.IndexSpec):
                def __init__(self, *args, **kwargs):
                    self.delete_nothing = kwargs.pop('delete_nothing', False)
                    super().__init__(*args, **kwargs)

            def prepare_adjustment(self, spec):
                adjustment = super().prepare_adjustment(spec)
                if spec.delete_nothing:
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
            class IndexSpec(SpyIndexes.IndexSpec):
                def __init__(self, *args, **kwargs):
                    self.load_nothing = kwargs.pop('load_nothing', False)
                    super().__init__(*args, **kwargs)

            def prepare_adjustment(self, spec):
                adjustment = super().prepare_adjustment(spec)
                if spec.load_nothing:
                    adjustment.load_spec = None
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

    async def test_adjust_observes_expired_and_newly_loaded_records(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', *range(3))
        expected_expired = await collect_async_iter(
            table.get_records('primary_key', 0))
        expected_loaded = await collect_async_iter(
            db_access.get_records(
                tc.QueryArgsDbRecordsSpec('query_some_pks', ((3,),))))
        await table.adjust('primary_key', *range(1, 4))
        assert_that(
            indexes.adjustments, contains_exactly(
                anything(),
                has_properties(
                    observe_expired=has_properties(
                        call_args_list=contains_inanyorder(
                            *[um.call(r) for r in expected_expired])),
                    observe_loaded=has_properties(
                        call_args_list=contains_inanyorder(
                            *[um.call(r) for r in expected_loaded])))))

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
            def prepare_adjustment(self, spec):
                if spec.index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().prepare_adjustment(spec)
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

    async def test_adjust_refreshes_first(self, table, db_access, indexes):
        db_access.records = [{'pk': i, 's': f'a{i}'} for i in range(2)]
        await table.load('primary_key', 0)
        db_access.records = [{'pk': i, 's': f'b{i}'} for i in range(4)]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', 0)],
            [indexes.IndexSpec('primary_key', 0)])
        await table.adjust('primary_key', 1)
        assert_that(
            await table.get_first_record('primary_key', 0),
            has_entries(s='b0', source='db'))

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
            def covers(self, spec):
                if spec.index_name == 'x_range':
                    raise tc.UnsupportedIndexOperation
                return super().covers(spec)
        table, _ = make_tables(Indexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14))

    async def test_invalidate_records_raises_without_old_spec(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            table.invalidate_records(
                [], [indexes.IndexSpec('primary_key', *range(6))])

    async def test_invalidate_records_raises_without_new_spec(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(6))], [])

    async def test_invalidate_records_raises_with_dulicate_old_spec(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(6)),
                 indexes.IndexSpec('primary_key', *range(6))],
                [indexes.IndexSpec('primary_key', *range(6))])

    async def test_invalidate_records_raises_with_dulicate_new_spec(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(6))],
                [indexes.IndexSpec('primary_key', *range(6)),
                 indexes.IndexSpec('primary_key', *range(6))])

    async def test_invalidate_records_raises_with_old_spec_that_is_not_covered(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(4))
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(6))],
                [indexes.IndexSpec('primary_key', *range(4))])

    async def test_invalidate_records_raises_with_new_spec_that_is_not_covered(
            self, table, db_access, indexes):
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(4))
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(4))],
                [indexes.IndexSpec('primary_key', *range(6))])

    async def test_invalidate_records_raises_with_uncertain_old_coverage(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(4))
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(4)),
                 indexes.IndexSpec('x_range', min=10, max=14)],
                [indexes.IndexSpec('primary_key', *range(4))])

    async def test_invalidate_records_raises_with_uncertain_new_coverage(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', *range(4))
        with pytest.raises(ValueError):
            table.invalidate_records(
                [indexes.IndexSpec('primary_key', *range(4))],
                [indexes.IndexSpec('primary_key', *range(4)),
                 indexes.IndexSpec('x_range', min=10, max=14)])

    async def test_changing_scores_with_score_hint_dont_return_old_records(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=10, max=16)],
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=100, max=106)])
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
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=10, max=16)],
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=100, max=106)])
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
        table.invalidate_records(
            [MultiIndexes.IndexSpec('primary_key', *range(6))],
            [MultiIndexes.IndexSpec('primary_key', *range(6))])
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=12, max=14)), empty())

    async def test_changing_scores_without_score_hint_return_new_records(
            self, make_tables, db_access):
        table, _ = make_tables(MultiIndexes())
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        table.invalidate_records(
            [MultiIndexes.IndexSpec('primary_key', *range(6))],
            [MultiIndexes.IndexSpec('primary_key', *range(6))])
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=100, max=106)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 100, source='storage')
                  for i in range(6)]))

    async def test_refreshes_after_invalidate_records_on_affected_old(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)])
        assert self.table_has_invalid_records(table)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=11, max=14)),
            contains_inanyorder(
                has_entries(pk=1, x=11, source='storage')))
        assert not self.table_has_invalid_records(table)

    async def test_refreshes_after_invalidate_records_on_affected_new(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)])
        assert self.table_has_invalid_records(table)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=101, max=104)),
            contains_inanyorder(
                has_entries(pk=2, x=102, source='storage'),
                has_entries(pk=3, x=103, source='storage')))
        assert not self.table_has_invalid_records(table)

    async def test_doesnt_refresh_after_invalidate_records_on_unaffected_old(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)])
        assert self.table_has_invalid_records(table)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=14, max=16)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(4, 6)]))
        assert self.table_has_invalid_records(table)

    async def test_doesnt_refresh_after_invalidate_records_on_unaffected_new(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)])
        assert self.table_has_invalid_records(table)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=104, max=106)), empty())
        assert self.table_has_invalid_records(table)

    async def test_doesnt_refresh_automatically_after_invalidate_records(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)],
            force_refresh_on_next_read=False)
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=11, max=14)),
            contains_inanyorder(
                has_entries(pk=1, x=11, source='storage'),
                has_entries(pk=2, x=12, source='storage'),
                has_entries(pk=3, x=13, source='storage')))
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=101, max=104)), empty())
        assert self.table_has_invalid_records(table)

    async def test_refreshes_manually_after_non_automatic_invalidate_records(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('x_range', min=10, max=106)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=102, max=104)],
            force_refresh_on_next_read=False)
        await table.refresh_invalid()
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=11, max=14)),
            contains_inanyorder(
                has_entries(pk=1, x=11, source='storage')))
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=101, max=104)),
            contains_inanyorder(
                has_entries(pk=2, x=102, source='storage'),
                has_entries(pk=3, x=103, source='storage')))
        assert not self.table_has_invalid_records(table)

    async def test_invalidate_records_tolerates_overspecified_new(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        for i in range(2, 4):
            db_access.records[i]['x'] = i + 100
        assert not self.table_has_invalid_records(table)
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=12, max=14)],
            [indexes.IndexSpec('x_range', min=100, max=106)])
        assert_that(
            await collect_async_iter(
                table.get_records('primary_key', all_primary_keys=True)),
            contains_inanyorder(
                *it.chain([has_entries(pk=i, x=i + 10, source='storage')
                           for i in [0, 1, 4, 5]],
                          [has_entries(pk=i, x=i + 100, source='storage')
                           for i in [2, 3]])))

    async def test_invalidate_records_can_add(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': 0, 'x': 10}, {'pk': 2, 'x': 12}]
        await table.load('x_range', min=10, max=13)
        db_access.records.append({'pk': 1, 'x': 11})
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=10, max=13)],
            [indexes.IndexSpec('x_range', min=10, max=13)])
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=10, max=13)),
            contains_inanyorder(
                *[has_entries(pk=i, x=i + 10, source='storage')
                  for i in range(3)]))

    async def test_invalidate_records_can_delete(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(3)]
        await table.load('x_range', min=10, max=13)
        del db_access.records[1]
        table.invalidate_records(
            [indexes.IndexSpec('x_range', min=10, max=13)],
            [indexes.IndexSpec('x_range', min=10, max=13)])
        assert_that(
            await collect_async_iter(
                table.get_records('x_range', min=10, max=13)),
            contains_inanyorder(
                has_entries(pk=0, x=10, source='storage'),
                has_entries(pk=2, x=12, source='storage')))

    async def test_invalidate_records_uses_first_specs_for_refresh(
            self, make_tables, db_access):
        indexes = MultiIndexes()
        table, _ = make_tables(indexes)
        db_access.records = [{'pk': i, 'x': i + 10} for i in range(6)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i, 'x': i + 100} for i in range(6)]
        table.invalidate_records(
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=10, max=11)],
            [indexes.IndexSpec('primary_key', *range(6)),
             indexes.IndexSpec('x_range', min=100, max=101)])
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
        table.invalidate_records(
            [MultiIndexes.IndexSpec('primary_key', 3)],
            [MultiIndexes.IndexSpec('primary_key', 3)])
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await collect_async_iter(storage_table.get_records(
                        tc.StorageRecordsSpec(
                            'primary_key', [tc.Interval.only_containing(3)]))),
                    contains_exactly(has_entries(pk=3)))
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
            await table.get_first_record('primary_key', 3)

    async def test_refresh_invalid_uses_scratch_space_for_updating(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': 0, 's': 'x'}]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': 0, 's': 'y'}]
        table.invalidate_records(
            [MultiIndexes.IndexSpec('primary_key', 0)],
            [MultiIndexes.IndexSpec('primary_key', 0)])
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await collect_async_iter(storage_table.get_records(
                        tc.StorageRecordsSpec(
                            'primary_key', [tc.Interval.only_containing(0)]))),
                    contains_exactly(has_entries(s='x')))
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
            await table.get_first_record('primary_key', 0),
            has_entries(s='y', source='storage'))

    async def test_refresh_invalid_doesnt_block_gets_for_valid_keys(
            self, make_tables, db_access):
        table, storage_table = make_tables()
        db_access.records = [{'pk': i} for i in range(4)]
        await table.load('primary_key', all_primary_keys=True)
        db_access.records = [{'pk': i} for i in range(3)]
        table.invalidate_records(
            [MultiIndexes.IndexSpec('primary_key', 3)],
            [MultiIndexes.IndexSpec('primary_key', 3)])
        storage_table._enable_merge_wait()
        exceptions = []
        task_queue = queue.Queue()

        async def assert_pre_merge():
            refresh_task = task_queue.get()
            storage_table._merge_wait_start()
            try:
                assert_that(
                    await collect_async_iter(storage_table.get_records(
                        tc.StorageRecordsSpec(
                            'primary_key', [tc.Interval.only_containing(2)]))),
                    contains_exactly(has_entries(pk=2)))
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
    def indexes(self):
        return MultiIndexes()

    @pytest.fixture
    def pk_interval(self, indexes):
        def factory(min, max=None):
            if max is None:
                return tc.Interval.only_containing(
                    indexes.score('primary_key', {'pk': min}))
            return tc.Interval(
                indexes.score('primary_key', {'pk': min}),
                indexes.score('primary_key', {'pk': max}))
        return factory

    @pytest.fixture
    def x_range_interval(self, indexes):
        def factory(min, max):
            return tc.Interval(
                indexes.score('x_range', {'x': min}),
                indexes.score('x_range', {'x': max}))
        return factory

    @pytest.fixture
    def repo(self, indexes):
        import tablecache.cache
        return tablecache.cache.InvalidRecordRepository(indexes)

    def test_interval_invalid(
            self, repo, indexes, pk_interval, x_range_interval):
        assert not repo.interval_intersects_invalid(
            'primary_key', tc.Interval.everything())
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=10, max=20)},
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=110, max=120)},
            'primary_key', 'primary_key', True)
        assert not repo.interval_intersects_invalid(
            'primary_key', pk_interval(float('-inf'), 2))
        assert not repo.interval_intersects_invalid(
            'x_range', x_range_interval(float('-inf'), 10))
        assert not repo.interval_intersects_invalid(
            'x_range', x_range_interval(20, 110))
        assert not repo.interval_intersects_invalid(
            'x_range', x_range_interval(120, float('inf')))
        assert repo.interval_intersects_invalid('primary_key', pk_interval(2))
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(0, 15))
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(0, 30))
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(15, 16))
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(15, 25))
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(115, 125))
        assert not repo.interval_intersects_invalid(
            'primary_key', pk_interval(2.1, float('inf')))

    def test_interval_invalid_raises_if_index_for_refresh_not_given(
            self, repo, indexes):
        with pytest.raises(KeyError):
            repo.flag_invalid(
                {'primary_key': indexes.IndexSpec('primary_key', 2)},
                {'primary_key': indexes.IndexSpec('primary_key', 2)},
                'x_range', 'x_range', True)

    def test_interval_invalid_on_dirty_index(
            self, repo, indexes, x_range_interval):
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            'primary_key', 'primary_key', True)
        assert repo.interval_intersects_invalid(
            'x_range', x_range_interval(0, 5))

    def test_interval_invalid_without_consider_not_in_intersects(
            self, repo, indexes):
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=10, max=20)},
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=110, max=120)},
            'primary_key', 'primary_key', False)
        assert not repo.interval_intersects_invalid(
            'primary_key', tc.Interval.everything())
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())

    def test_interval_invalid_without_consider_doesnt_dirty_index(
            self, repo, indexes):
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            'primary_key', 'primary_key', False)
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())

    @pytest.mark.parametrize('consider_in_intersects_check', [True, False])
    def test_specs_for_refresh(
            self, repo, indexes, consider_in_intersects_check):
        assert_that(repo.specs_for_refresh(), empty())
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            'primary_key', 'primary_key', consider_in_intersects_check)
        assert_that(
            repo.specs_for_refresh(),
            contains_inanyorder(
                contains_exactly(
                    has_properties(index_name='primary_key', primary_keys={2}),
                    has_properties(index_name='primary_key', primary_keys={2})
                )))
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 3),
             'x_range': indexes.IndexSpec('x_range', min=10, max=20)},
            {'primary_key': indexes.IndexSpec('primary_key', 3),
             'x_range': indexes.IndexSpec('x_range', min=110, max=120)},
            'primary_key', 'x_range', True)
        assert_that(
            repo.specs_for_refresh(),
            contains_inanyorder(
                contains_exactly(
                    has_properties(index_name='primary_key', primary_keys={2}),
                    has_properties(index_name='primary_key', primary_keys={2})
                ), contains_exactly(
                    has_properties(index_name='primary_key', primary_keys={3}),
                    has_properties(index_name='x_range', min=110, max=120)
                )))

    def test_interval_not_invalid_after_clear(self, repo, indexes):
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=10, max=20)},
            {'primary_key': indexes.IndexSpec('primary_key', 2),
             'x_range': indexes.IndexSpec('x_range', min=110, max=120)},
            'primary_key', 'primary_key', True)
        assert repo.interval_intersects_invalid(
            'primary_key', tc.Interval.everything())
        assert repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())
        repo.clear()
        assert not repo.interval_intersects_invalid(
            'primary_key', tc.Interval.everything())
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())

    def test_index_not_dirty_after_clean(self, repo, indexes):
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            'primary_key', 'primary_key', True)
        assert repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())
        repo.clear()
        assert not repo.interval_intersects_invalid(
            'x_range', tc.Interval.everything())

    @pytest.mark.parametrize('consider_in_intersects_check', [True, False])
    def test_len_is_number_of_specs_for_refresh(
            self, repo, indexes, consider_in_intersects_check):
        assert len(repo) == 0
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            {'primary_key': indexes.IndexSpec('primary_key', 2)},
            'primary_key', 'primary_key', consider_in_intersects_check)
        assert len(repo) == 1
        repo.flag_invalid(
            {'primary_key': indexes.IndexSpec('primary_key', 3),
             'x_range': indexes.IndexSpec('x_range', min=10, max=20)},
            {'primary_key': indexes.IndexSpec('primary_key', 3),
             'x_range': indexes.IndexSpec('x_range', min=110, max=120)},
            'primary_key', 'x_range', consider_in_intersects_check)
        assert len(repo) == 2
        repo.clear()
        assert len(repo) == 0
