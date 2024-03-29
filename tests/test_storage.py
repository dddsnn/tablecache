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

import math

import pytest

import tablecache as tc


class TestInterval:
    def test_raises_on_bounds_not_in_order(self):
        with pytest.raises(ValueError):
            tc.Interval(2, 1)

    def test_contains(self):
        interval = tc.Interval(2, 5)
        assert 2 in interval
        assert math.nextafter(5, float('-inf')) in interval
        assert 1 not in interval
        assert 5 not in interval

    def test_empty_interval(self):
        assert 1 not in tc.Interval(1, 1)

    def test_intersects_on_empty(self):
        assert not tc.Interval(10, 10).intersects(tc.Interval(10, 10))

    def test_intersects_on_disjoint(self):
        assert not tc.Interval(0, 10).intersects(tc.Interval(20, 30))
        assert not tc.Interval(20, 30).intersects(tc.Interval(0, 10))

    def test_intersects_on_adjacent(self):
        assert not tc.Interval(0, 10).intersects(tc.Interval(10, 20))
        assert not tc.Interval(10, 20).intersects(tc.Interval(0, 10))

    def test_intersects_on_overlap(self):
        assert tc.Interval(10, 20).intersects(tc.Interval(5, 15))
        assert tc.Interval(10, 20).intersects(tc.Interval(15, 25))
        assert tc.Interval(5, 15).intersects(tc.Interval(10, 20))
        assert tc.Interval(15, 25).intersects(tc.Interval(10, 20))

    def test_intersects_on_contained(self):
        assert tc.Interval(10, 20).intersects(tc.Interval(12, 13))
        assert tc.Interval(12, 13).intersects(tc.Interval(10, 20))
        assert tc.Interval(10, 20).intersects(tc.Interval(10, 20))

    def test_covers_on_empty(self):
        assert tc.Interval(10, 10).covers(tc.Interval(10, 10))
        assert tc.Interval(20, 20).covers(tc.Interval(10, 10))

    def test_covers_on_intersection(self):
        assert not tc.Interval(0, 10).covers(tc.Interval(20, 30))
        assert not tc.Interval(20, 30).covers(tc.Interval(0, 10))
        assert not tc.Interval(0, 10).covers(tc.Interval(10, 20))
        assert not tc.Interval(10, 20).covers(tc.Interval(0, 10))
        assert not tc.Interval(10, 20).covers(tc.Interval(5, 15))
        assert not tc.Interval(10, 20).covers(tc.Interval(15, 25))
        assert not tc.Interval(5, 15).covers(tc.Interval(10, 20))
        assert not tc.Interval(15, 25).covers(tc.Interval(10, 20))

    def test_covers_on_self_contains_other(self):
        assert tc.Interval(10, 20).covers(tc.Interval(12, 13))
        assert tc.Interval(10, 20).covers(tc.Interval(10, 20))

    def test_covers_on_other_contains_self(self):
        assert not tc.Interval(12, 13).covers(tc.Interval(10, 20))


class TestStorageRecordsSpec:
    @pytest.mark.parametrize(
        'intervals', [
            [tc.Interval(0, 2), tc.Interval(1, 3)],
            [tc.Interval(4, 6), tc.Interval(0, 2), tc.Interval(5, 7)]])
    def test_raises_if_intervals_overlap(self, intervals):
        with pytest.raises(ValueError):
            tc.StorageRecordsSpec('', intervals)
