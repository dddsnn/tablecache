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
    @pytest.mark.parametrize('value', ['', 'foo', 'äöüß'])
    def test_encode_decode_identity(self, value):
        codec = tc.StringCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize('value', [0, 1, b'not a string'])
    def test_encode_raises_on_invalid(self, value):
        codec = tc.StringCodec()
        with pytest.raises(ValueError):
            codec.encode(value)


class TestIntAsStringCodec:
    @pytest.mark.parametrize('value', [0, 1, -1, sys.maxsize + 1])
    def test_encode_decode_identity(self, value):
        codec = tc.IntAsStringCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize('value', ['not a number', 1.1])
    def test_encode_raises_on_invalid(self, value):
        codec = tc.IntAsStringCodec()
        with pytest.raises(ValueError):
            codec.encode(value)

    @pytest.mark.parametrize(
        'encoded', [b'not a number', b'', b'\x00', b'1.1'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.IntAsStringCodec()
        with pytest.raises(ValueError):
            codec.decode(encoded)
