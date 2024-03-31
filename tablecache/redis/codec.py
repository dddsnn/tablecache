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

import abc
import datetime
import math
import numbers
import struct
import typing as t
import uuid


class Codec[T](abc.ABC):
    """
    Abstract base for codecs.

    A codec can encode certain values to bytes, then decode those back to the
    original value.
    """

    @abc.abstractmethod
    def encode(self, value: T) -> bytes:
        """
        Encode the value to bytes.

        :param value: The value to encode.
        :return: A representation of the input value as bytes.
        :raise ValueError: If the input value is invalid and can't be encoded.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, bs: bytes) -> T:
        """
        Decode the bytes to a value.

        :param bs: A :py:class:`bytes` object containing an encoded value.
        :return: The decoded value
        :raise ValueError: If the input is invalid and can't be decoded.
        """
        raise NotImplementedError


class Nullable[T](Codec[t.Optional[T]]):
    """
    Wrapper codec that allows representing nullable values.

    Encodes optional values by using an inner codec for values, and a marker
    for None.
    """

    def __init__(self, value_codec: Codec[T]):
        """
        :param value_codec: Wrapped codec to encode and decode values.
        """
        self._value_codec = value_codec

    @t.override
    def encode(self, value: t.Optional[T]) -> bytes:
        if value is None:
            return b'\x00'
        return b'\x01' + self._value_codec.encode(value)

    @t.override
    def decode(self, bs: bytes) -> t.Optional[T]:
        if bs == b'\x00':
            return None
        return self._value_codec.decode(bs[1:])


class Array[T](Codec[list[T]]):
    """
    Wrapper codec that allows representing arrays (i.e. lists).

    Encodes elements using an inner codec. The length of each element is
    encoded using a 16-bit unsigned integer, so elements must not be over 65535
    bytes long.
    """

    def __init__(self, value_codec: Codec[T]):
        """
        :param value_codec: Wrapped codec to encode and decode array values.
        """
        self._value_codec = value_codec
        self._length_codec = UnsignedInt16Codec()

    @t.override
    def encode(self, values: list[T]) -> bytes:
        if not isinstance(values, list):
            raise ValueError('Value must be a list.')
        encoded = bytearray()
        for value in values:
            encoded_value = self._value_codec.encode(value)
            encoded += self._length_codec.encode(len(encoded_value))
            encoded += encoded_value
        return bytes(encoded)

    @t.override
    def decode(self, bs: bytes) -> list[T]:
        values = []
        i = 0
        while i < len(bs):
            value_length = self._length_codec.decode(bs[i:i + 2])
            value = self._value_codec.decode(bs[i + 2:i + 2 + value_length])
            values.append(value)
            i += 2 + value_length
        return values


class BoolCodec(Codec[bool]):
    """Codec that represents bools as single bytes."""
    @t.override
    def encode(self, value: bool) -> bytes:
        if not isinstance(value, bool):
            raise ValueError('Value is not a bool.')
        return b'\x01' if value else b'\x00'

    @t.override
    def decode(self, bs: bytes) -> bool:
        if bs == b'\x00':
            return False
        if bs == b'\x01':
            return True
        raise ValueError('Invalid bool representation.')


class StringCodec(Codec[str]):
    """Simple str<->bytes codec (UTF-8)."""
    @t.override
    def encode(self, value: str) -> bytes:
        if not isinstance(value, str):
            raise ValueError('Value is not a string.')
        return value.encode()

    @t.override
    def decode(self, bs: bytes) -> str:
        return bs.decode()


class IntAsStringCodec(Codec[int]):
    """Codec that represents ints as strings."""
    @t.override
    def encode(self, value: int) -> bytes:
        if not isinstance(value, int):
            raise ValueError('Value is not an int.')
        return str(value).encode()

    @t.override
    def decode(self, bs: bytes) -> int:
        return int(bs.decode())


class FloatAsStringCodec(Codec[numbers.Real]):
    """
    Codec that represents floats as strings.

    Handles infinities and NaNs, but makes no distinction between signalling
    NaNs (all NaNs are decoded to quiet NaNs).
    """
    @t.override
    def encode(self, value: numbers.Real) -> bytes:
        if not isinstance(value, numbers.Real):
            raise ValueError('Value is not a real number.')
        return str(value).encode()

    @t.override
    def decode(self, bs: bytes) -> numbers.Real:
        return float(bs.decode())


class EncodedNumberCodec[T: numbers.Number](Codec[T]):
    """Codec that encodes numbers to bytes directly."""

    def __init__(self, struct_format: str) -> None:
        self._struct_format = struct_format

    @t.override
    def encode(self, value: T) -> bytes:
        try:
            return struct.pack(self._struct_format, value)
        except struct.error as e:
            raise ValueError('Unable to encode number.') from e

    @t.override
    def decode(self, bs: bytes) -> T:
        try:
            value, = struct.unpack(self._struct_format, bs)
        except struct.error as e:
            raise ValueError('Unable to decode number.') from e
        return value


class EncodedIntCodec(EncodedNumberCodec[int]):
    pass


class SignedInt8Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>b')


class SignedInt16Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>h')


class SignedInt32Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>i')


class SignedInt64Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>q')


class UnsignedInt8Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>B')


class UnsignedInt16Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>H')


class UnsignedInt32Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>I')


class UnsignedInt64Codec(EncodedIntCodec):
    def __init__(self) -> None:
        super().__init__('>Q')


class EncodedFloatCodec(EncodedNumberCodec[numbers.Real]):
    """
    Codec that encodes floats to bytes directly.

    Infinities and NaNs are handled. Signalling NaNs mostly work, with the
    exception that the most significant bit of the signalling part is always 1
    (i.e. single-precision NaNs always start with ``7fc`` or ``ffc``, and
    double precision with ``7ff8`` or ``fff8``).
    """


class Float32Codec(EncodedFloatCodec):
    _min_value, = struct.unpack('>f', bytes.fromhex('ff7fffff'))
    _max_value, = struct.unpack('>f', bytes.fromhex('7f7fffff'))

    def __init__(self) -> None:
        super().__init__('>f')

    @t.override
    def encode(self, value: numbers.Real) -> bytes:
        encoded = super().encode(value)
        if not math.isinf(value) and (value < self._min_value or
                                      value > self._max_value):
            raise ValueError('Value is outside of float32 range.')
        return encoded


class Float64Codec(EncodedFloatCodec):
    def __init__(self) -> None:
        super().__init__('>d')


class UuidCodec(Codec[uuid.UUID]):
    """Codec for UUIDs."""
    @t.override
    def encode(self, value: uuid.UUID) -> bytes:
        if not isinstance(value, uuid.UUID):
            raise ValueError('Value is not a UUID.')
        return value.bytes

    @t.override
    def decode(self, bs: bytes) -> uuid.UUID:
        return uuid.UUID(bytes=bs)


class UtcDatetimeCodec(Codec[datetime.datetime]):
    """
    Codec for UTC datetimes.

    Encodes values as an epoch timestamp in a double precision float, so
    precision is limited to that value range.

    Only timezone-naive datetimes ones in timezone UTC can be encoded. Any
    other value results in a ValueError. Naive datetimes are treated as though
    they are UTC. When decoding datetimes in UTC are returned.
    """

    def __init__(self):
        self._float_codec = Float64Codec()

    @t.override
    def encode(self, value: datetime.datetime) -> bytes:
        if not isinstance(value, datetime.datetime):
            raise ValueError('Value is not a datetime.')
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        elif value.tzinfo != datetime.timezone.utc:
            raise ValueError('Datetime is not in UTC.')
        try:
            return self._float_codec.encode(value.timestamp())
        except Exception as e:
            raise ValueError('Unable to encode timestamp as float.') from e

    @t.override
    def decode(self, bs: bytes) -> datetime.datetime:
        try:
            timestamp = self._float_codec.decode(bs)
            return datetime.datetime.fromtimestamp(
                timestamp, tz=datetime.timezone.utc)
        except Exception as e:
            raise ValueError('Unable to decode timestamp float.') from e
