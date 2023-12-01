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

import datetime
import math
import struct
import sys
import uuid

import pytest

import tablecache as tc


class TestNullable:
    @pytest.mark.parametrize('value', [0, 1, 234])
    def test_encode_decode_non_null_values(self, value):
        codec = tc.Nullable(tc.IntAsStringCodec())
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    def test_encode_decode_null(self):
        codec = tc.Nullable(tc.IntAsStringCodec())
        encoded = codec.encode(None)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded is None

    def test_encode_raises_from_underlying_codec(self):
        codec = tc.Nullable(tc.IntAsStringCodec())
        with pytest.raises(ValueError):
            codec.encode('not a number')

    @pytest.mark.parametrize('encoded', [b'', b'\x00not a number'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.Nullable(tc.IntAsStringCodec())
        with pytest.raises(ValueError):
            codec.decode(encoded)


class TestArray:
    def test_encode_decode_on_empty(self):
        codec = tc.Array(tc.StringCodec())
        encoded = codec.encode([])
        assert isinstance(encoded, bytes)
        assert codec.decode(encoded) == []

    @pytest.mark.parametrize('value', [['a'], ['a', 'b']])
    def test_encode_decode(self, value):
        codec = tc.Array(tc.StringCodec())
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    def test_encode_decode_handles_empty_element(self):
        codec = tc.Array(tc.StringCodec())
        encoded = codec.encode(['', 'a'])
        assert isinstance(encoded, bytes)
        assert codec.decode(encoded) == ['', 'a']

    @pytest.mark.parametrize('value', [1, 'asd'])
    def test_encode_raises_on_non_list(self, value):
        codec = tc.Array(tc.StringCodec())
        with pytest.raises(ValueError):
            codec.encode(value)

    def test_encode_raises_from_underlying_codec(self):
        codec = tc.Array(tc.StringCodec())
        with pytest.raises(ValueError):
            codec.encode([1])

    def test_encode_raises_on_oversized_value(self):
        codec = tc.Array(tc.StringCodec())
        with pytest.raises(ValueError):
            codec.encode(65536 * 'x')

    @pytest.mark.parametrize('encoded', [b'x00', b'\x00\x01', b'\x00\x01\x42'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.Array(tc.UnsignedInt16Codec())
        with pytest.raises(ValueError):
            codec.decode(encoded)


class TestBoolCodec:
    @pytest.mark.parametrize('value', [True, False])
    def test_encode_decode_identity(self, value):
        codec = tc.BoolCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize('value', [None, 0, 1, '', 'foo', bool])
    def test_encode_raises_on_invalid(self, value):
        codec = tc.BoolCodec()
        with pytest.raises(ValueError):
            codec.encode(value)

    @pytest.mark.parametrize('encoded', [b'', b'\x00\x00', b'\x02', b'\xff'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.BoolCodec()
        with pytest.raises(ValueError):
            codec.decode(encoded)


class TestStringCodec:
    @pytest.mark.parametrize('value', ['', 'foo', 'äöüß'])
    def test_encode_decode_identity(self, value):
        codec = tc.StringCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize('value', [None, 0, 1, b'not a string'])
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

    @pytest.mark.parametrize('value', [None, 'not a number', 1.1, int])
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


class TestFloatAsStringCodec:
    @pytest.mark.parametrize(
        'value', [
            0, 1, -1, 1.5, -0.1, sys.float_info.max, -sys.float_info.max,
            float('inf'),
            float('-inf')])
    def test_encode_decode_identity(self, value):
        codec = tc.FloatAsStringCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize(
        'nan_hex', [
            '7ff8000000000000', 'fff8000000000000', '7ff0000000000001',
            'fff0000000000001', 'ffffffffffffffff', '7fffffffffffffff'])
    def test_all_nans_are_default_nan(self, nan_hex):
        value, = struct.unpack('>d', bytes.fromhex(nan_hex))
        assert math.isnan(value)
        codec = tc.FloatAsStringCodec()
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert math.isnan(decoded)
        assert struct.pack('>d', decoded).hex() == '7ff8000000000000'

    @pytest.mark.parametrize('value', [None, 'not a number', float])
    def test_encode_raises_on_invalid(self, value):
        codec = tc.FloatAsStringCodec()
        with pytest.raises(ValueError):
            codec.encode(value)

    @pytest.mark.parametrize('encoded', [b'not a number', b'', b'\x00'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.FloatAsStringCodec()
        with pytest.raises(ValueError):
            codec.decode(encoded)


@pytest.mark.parametrize(
    'codec,num_bytes,min_value,max_value', [
        (tc.SignedInt8Codec(), 1, -2**7, 2**7 - 1),
        (tc.SignedInt16Codec(), 2, -2**15, 2**15 - 1),
        (tc.SignedInt32Codec(), 4, -2**31, 2**31 - 1),
        (tc.SignedInt64Codec(), 8, -2**63, 2**63 - 1),
        (tc.UnsignedInt8Codec(), 1, 0, 2**8 - 1),
        (tc.UnsignedInt16Codec(), 2, 0, 2**16 - 1),
        (tc.UnsignedInt32Codec(), 4, 0, 2**32 - 1),
        (tc.UnsignedInt64Codec(), 8, 0, 2**64 - 1),])
class TestEncodedIntCodecs:
    def test_encode_decode_identity(
            self, codec, num_bytes, min_value, max_value):
        for value in [0, min_value, min_value + 1, max_value - 1, max_value]:
            encoded = codec.encode(value)
            assert isinstance(encoded, bytes)
            assert len(encoded) == num_bytes
            decoded = codec.decode(encoded)
            assert decoded == value

    def test_encode_raises_on_invalid(
            self, codec, num_bytes, min_value, max_value):
        for value in [None, min_value - 1, max_value + 1, 1.5, 'not a number',
                      int]:
            with pytest.raises(ValueError):
                codec.encode(value)

    def test_decode_raises_on_invalid(
            self, codec, num_bytes, min_value, max_value):
        for encoded_length in [0, num_bytes - 1, num_bytes + 1]:
            encoded = encoded_length * b'\x00'
            with pytest.raises(ValueError):
                codec.decode(encoded)


@pytest.mark.parametrize(
    'codec,num_bytes,min_value,max_value', [
        (
            tc.Float32Codec(),
            4,
            struct.unpack('>f', bytes.fromhex('ff7fffff'))[0],
            struct.unpack('>f', bytes.fromhex('7f7fffff'))[0],
        ),
        (tc.Float64Codec(), 8, -sys.float_info.max, sys.float_info.max),])
class TestEncodedFloatCodecs:
    def test_encode_decode_identity(
            self, codec, num_bytes, min_value, max_value):
        for value in [
                0,
                min_value,
                -1.5,
                1.5,
                max_value,
                float('inf'),
                float('-inf'),]:
            encoded = codec.encode(value)
            assert isinstance(encoded, bytes)
            assert len(encoded) == num_bytes
            decoded = codec.decode(encoded)
            assert decoded == value

    def test_encode_raises_on_invalid(
            self, codec, num_bytes, min_value, max_value):
        values = [None, 'not a number', float]
        if max_value < sys.float_info.max:
            values += [
                math.nextafter(min_value, float('-inf')),
                math.nextafter(max_value, float('inf'))]
        for value in values:
            with pytest.raises(ValueError):
                codec.encode(value)

    def test_decode_raises_on_invalid(
            self, codec, num_bytes, min_value, max_value):
        for encoded_length in [0, num_bytes - 1, num_bytes + 1]:
            encoded = encoded_length * b'\x00'
            with pytest.raises(ValueError):
                codec.decode(encoded)

    @pytest.mark.parametrize('sign_nibble', ['7', 'f'])
    @pytest.mark.parametrize('least_significant_bytes', ['0001', 'ffff'])
    def test_preserves_signalling_nans_mostly(
            self, codec, num_bytes, min_value, max_value, sign_nibble,
            least_significant_bytes):
        if num_bytes == 4:
            base_nan_hex = 'sfc0llll'
            struct_format = '>f'
        elif num_bytes == 8:
            base_nan_hex = 'sff800000000llll'
            struct_format = '>d'
        nan_hex = sign_nibble + base_nan_hex[1:-4] + least_significant_bytes
        value, = struct.unpack(struct_format, bytes.fromhex(nan_hex))
        assert math.isnan(value)
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert math.isnan(decoded)
        assert struct.pack(struct_format, decoded).hex() == nan_hex


class TestUuidCodec:
    @pytest.mark.parametrize('hex', [16 * '00', 15 * '00' + '01', 16 * 'ff'])
    def test_encode_decode_identity(self, hex):
        codec = tc.UuidCodec()
        value = uuid.UUID(hex=hex)
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    @pytest.mark.parametrize('value', [None, 16 * '00', 16 * b'\x00', 's', 1])
    def test_encode_raises_on_invalid(self, value):
        codec = tc.UuidCodec()
        with pytest.raises(ValueError):
            codec.encode(value)

    @pytest.mark.parametrize(
        'encoded', [b'', b'\x00', 15 * b'\x00', 17 * b'\x00', 16 * '00'])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.UuidCodec()
        with pytest.raises(ValueError):
            codec.decode(encoded)


class TestUtcDatetimeCodec:
    @pytest.mark.parametrize('ts', [0, 1.5, 1000, -100000, 1700000000])
    def test_encode_decode_identity(self, ts):
        codec = tc.UtcDatetimeCodec()
        value = datetime.datetime.fromtimestamp(ts, datetime.timezone.utc)
        encoded = codec.encode(value)
        assert isinstance(encoded, bytes)
        decoded = codec.decode(encoded)
        assert decoded == value

    def test_encodes_naive_datetimes_to_utc(self):
        codec = tc.UtcDatetimeCodec()
        value = datetime.datetime.fromtimestamp(
            10000, tz=datetime.timezone.utc).replace(tzinfo=None)
        encoded = codec.encode(value)
        assert codec.decode(encoded) == datetime.datetime.fromtimestamp(
            10000, tz=datetime.timezone.utc)

    @pytest.mark.parametrize('value', [None, 0, datetime.datetime])
    def test_encode_raises_on_non_datetime(self, value):
        codec = tc.UtcDatetimeCodec()
        with pytest.raises(ValueError):
            codec.encode(value)

    def test_encode_raises_on_non_utc(self):
        codec = tc.UtcDatetimeCodec()
        not_utc = datetime.timezone(datetime.timedelta(seconds=3600))
        with pytest.raises(ValueError):
            codec.encode(datetime.datetime.fromtimestamp(1000, tz=not_utc))

    @pytest.mark.parametrize(
        'encoded', [
            b'', b'\x00', 7 * b'\x00', 9 * b'\x00',
            struct.pack('>d', float('inf')),
            struct.pack('>d', float('nan'))])
    def test_decode_raises_on_invalid(self, encoded):
        codec = tc.UtcDatetimeCodec()
        with pytest.raises(ValueError):
            codec.decode(encoded)
