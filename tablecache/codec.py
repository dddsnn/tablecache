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


def encode_str(s: str) -> bytes:
    """Encode a string as bytes."""
    return s.encode()


def decode_str(bs: bytes) -> str:
    """Decode bytes containing a string."""
    return bs.decode()


def encode_int_as_str(value: int) -> bytes:
    """Encode a stringified int as bytes."""
    return str(value).encode()


def decode_int_as_str(bs: bytes) -> int:
    """Decode bytes containing a stringified int."""
    return int(bs.decode())
