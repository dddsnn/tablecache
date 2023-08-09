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

import sys

import pytest

import tablecache as tc


@pytest.mark.parametrize('s', ['', 'foo', 'äöüß'])
def test_str(s):
    encoded = tc.encode_str(s)
    assert isinstance(encoded, bytes)
    decoded = tc.decode_str(encoded)
    assert decoded == s


@pytest.mark.parametrize('i', [0, 1, -1, sys.maxsize + 1])
def test_int_as_str(i):
    encoded = tc.encode_int_as_str(i)
    assert isinstance(encoded, bytes)
    decoded = tc.decode_int_as_str(encoded)
    assert decoded == i
