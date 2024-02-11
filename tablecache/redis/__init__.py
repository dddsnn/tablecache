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

try:
    import aiorwlock
    import redis.asyncio
except ImportError as e:
    raise Exception(
        'Please install tablecache[redis] to use tablecache.redis.') from e

from tablecache.redis.codec import (
    Array,
    BoolCodec,
    Codec,
    IntAsStringCodec,
    Float32Codec,
    Float64Codec,
    FloatAsStringCodec,
    Nullable,
    SignedInt8Codec,
    SignedInt16Codec,
    SignedInt32Codec,
    SignedInt64Codec,
    StringCodec,
    UnsignedInt8Codec,
    UnsignedInt16Codec,
    UnsignedInt32Codec,
    UnsignedInt64Codec,
    UtcDatetimeCodec,
    UuidCodec,
)
from tablecache.redis.storage import (
    AttributeCodecs, RedisCodingError, RedisTable)
