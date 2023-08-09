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


class MockDbTable:
    def __init__(self):
        self.records = {}

    async def all(self):
        for record in self.records.values():
            yield self._make_record(record)

    async def get(self, primary_keys):
        for key, record in self.records.items():
            if key in primary_keys:
                yield self._make_record(record)

    def _make_record(self, record):
        return record | {'source': 'db'}


class MockStorageTable:
    def __init__(self, primary_key_name):
        self.primary_key_name = primary_key_name
        self.records = {}

    async def clear(self):
        self.records = {}

    async def put(self, record):
        record_key = record[self.primary_key_name]
        self.records[record_key] = record

    async def get(self, record_key):
        return self.records[record_key] | {'source': 'storage'}


class TestCachedTable:
    @pytest.fixture
    def db_table(self):
        return MockDbTable()

    @pytest.fixture
    def table(self, db_table):
        return tc.CachedTable(db_table, MockStorageTable('pk'))

    async def test_load_and_get(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        assert_that(
            await table.get(1), has_entries(pk=1, k='v1', source='storage'))

    async def test_get_raises_if_not_loaded(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        with pytest.raises(KeyError):
            await table.get(1)

    async def test_get_raises_on_nonexistent(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}, 2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        with pytest.raises(KeyError):
            await table.get(3)

    async def test_load_clears_storage_first(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        assert_that(await table.get(1), has_entries(k='v1'))
        with pytest.raises(KeyError):
            await table.get(2)
        db_table.records = {2: {'pk': 2, 'k': 'v2'}}
        await table.load()
        with pytest.raises(KeyError):
            await table.get(1)
        assert_that(await table.get(2), has_entries(k='v2'))

    async def test_doesnt_automatically_reflect_db_state(
            self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get(1), has_entries(pk=1, k='v1'))

    async def test_refreshes_invalidated_keys(self, table, db_table):
        db_table.records = {1: {'pk': 1, 'k': 'v1'}}
        await table.load()
        await table.invalidate(1)
        db_table.records = {1: {'pk': 1, 'k': 'v2'}}
        assert_that(await table.get(1), has_entries(pk=1, k='v2'))
