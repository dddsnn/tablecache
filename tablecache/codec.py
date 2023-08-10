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
import typing as t


class Codec(abc.ABC):
    T = t.TypeVar('T')

    @abc.abstractmethod
    def encode(self, value: T) -> bytes:
        raise NotImplementedError

    @abc.abstractmethod
    def decode(self, bs: bytes) -> T:
        raise NotImplementedError


class StringCodec(Codec):
    """Simple str<->bytest codec (UTF-8)."""
    def encode(self, value: str) -> bytes:
        return value.encode()

    def decode(self, bs: bytes) -> str:
        return bs.decode()


class IntAsStringCodec(Codec):
    """Codec that represents ints as strings."""
    def encode(self, value: int) -> bytes:
        return str(value).encode()

    def decode(self, bs: bytes) -> int:
        return int(bs.decode())
