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


class TestStringCodec:
    @pytest.mark.parametrize('s', ['', 'foo', 'äöüß'])
    def test_encode_decode_identity(self, s):
        codec = tc.StringCodec()
        encoded = codec.encode(s)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == s


class TestIntAsStringCodec:
    @pytest.mark.parametrize('i', [0, 1, -1, sys.maxsize + 1])
    def test_encode_decode_identity(self, i):
        codec = tc.IntAsStringCodec()
        encoded = codec.encode(i)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == i
