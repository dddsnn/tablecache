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


class TestDbAccess:
    @pytest.fixture
    def access(self):
        class MockAccess(tc.DbAccess[dict, set[int]]):
            def __init__(self):
                self.records = {}

            async def get_records(self, primary_keys):
                for primary_key, record in self.records.items():
                    if primary_key in primary_keys:
                        yield record
        return MockAccess()

    async def test_get_record_raises_on_nonexistent(self, access):
        access.records[1] = {'pk': 1, 's': 'x'}
        with pytest.raises(KeyError):
            await access.get_record({2})

    async def test_get_record_gets_one(self, access):
        access.records[1] = {'pk': 1, 's': 'x'}
        assert_that(await access.get_record({1}), has_entries(pk=1, s='x'))

    async def test_get_record_gets_any(self, access):
        access.records[1] = {'pk': 1, 's': 'x1'}
        access.records[2] = {'pk': 2, 's': 'x2'}
        assert_that(
            await access.get_record({1, 2}),
            any_of(has_entries(pk=1, s='x1'), has_entries(pk=2, s='x2')))
