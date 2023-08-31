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

from hamcrest import *
import pytest

import tablecache as tc


async def collect_async_iter(i):
    l = []
    async for item in i:
        l.append(item)
    return l


class MockDbTable:
    def __init__(self):
        self.records = {}

    async def get_records(self, primary_keys):
        for key, record in self.records.items():
            if key in primary_keys:
                yield self._make_record(record)

    async def get_record(self, primary_key):
        return self._make_record(self.records[primary_key])

    async def get_record_range(self, key_range):
        for key, record in self.records.items():
            if key_range.score_ge <= key < key_range.score_lt:
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable:
    def __init__(self, primary_key_name):
        self.primary_key_name = primary_key_name
        self.records = {}

    async def clear(self):
        self.records = {}

    async def put_record(self, record):
        primary_key = record[self.primary_key_name]
        self.records[primary_key] = record

    async def get_record(self, primary_key):
        return self._make_record(self.records[primary_key])

    async def get_record_range(self, key_range):
        for key, record in self.records.items():
            if key_range.score_ge <= key < key_range.score_lt:
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'storage'}


class TestCachedTable:
    @pytest.fixture
    def db_table(self):
        return MockDbTable()

    @pytest.fixture
    def storage_table(self):
        return MockStorageTable('pk')

    @pytest.fixture
    def make_table(self, db_table, storage_table):
        def factory(cache_range=tc.AllRange()):
            return tc.CachedTable(db_table, storage_table, cache_range)

        return factory

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
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_get_record_raises_on_nonexistent(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        with pytest.raises(KeyError):
            await table.get_record(3)

    async def test_get_record_range_all(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load()
        assert_that(
            await collect_async_iter(table.get_record_range(tc.AllRange())),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(6)]))

    async def test_get_record_range_only_some(self, table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        await table.load()
        assert_that(
            await
            collect_async_iter(table.get_record_range(tc.NumberRange(2, 4))),
            contains_inanyorder(
                *[has_entries(pk=i, source='storage') for i in range(2, 4)]))

    async def test_loads_only_specified_range(
            self, make_table, db_table, storage_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        table = make_table(tc.NumberRange(2, 4))
        await table.load()
        assert_that(
            await
            collect_async_iter(storage_table.get_record_range(tc.AllRange())),
            contains_inanyorder(*[has_entries(pk=i) for i in range(2, 4)]))

    async def test_load_clears_storage_first(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        assert_that(await table.get_record(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get_record(2)
        db_table.records = {2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        with pytest.raises(KeyError):
            await table.get_record(1)
        assert_that(await table.get_record(2), has_entries(k='v2'))

    async def test_get_record_range_returns_db_state_if_range_not_cached(
            self, make_table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        table = make_table(tc.NumberRange(2, 4))
        await table.load()
        assert_that(
            await
            collect_async_iter(table.get_record_range(tc.NumberRange(2, 5))),
            contains_inanyorder(
                *[has_entries(pk=i, source='db') for i in range(2, 5)]))

    async def test_get_record_also_checks_db_in_case_out_of_range(
            self, make_table, db_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        table = make_table(tc.NumberRange(2, 4))
        await table.load()
        assert_that(await table.get_record(1), has_entries(pk=1, source='db'))

    async def test_get_record_doesnt_check_db_if_all_in_cache(
            self, make_table, db_table, storage_table):
        db_table.records = {i: {'pk': i} for i in range(6)}
        table = make_table(tc.AllRange())
        await table.load()
        del storage_table.records[1]
        with pytest.raises(KeyError):
            await table.get_record(1)

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='v1'))

    async def test_refreshes_invalidated_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        await table.invalidate(1)
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get_record(1), has_entries(pk=1, k='v2'))
