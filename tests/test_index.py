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

import pytest

import tablecache as tc


class TestStorageRecordsSpec:
    @pytest.mark.parametrize(
        'intervals', [
            [tc.Interval(0, 2), tc.Interval(1, 3)],
            [tc.Interval(4, 6), tc.Interval(0, 2), tc.Interval(5, 7)]])
    def test_raises_if_intervals_overlap(self, intervals):
        with pytest.raises(ValueError):
            tc.StorageRecordsSpec('', intervals)
