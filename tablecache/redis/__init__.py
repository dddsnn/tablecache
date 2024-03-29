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

"""
This submodule provides the :py:class:`.RedisTable`, an implementation of
:py:class:`.StorageTable` that stores its records in Redis. It uses the
:external:py:class:`redis.asyncio <redis.asyncio.connection.Connection>`
module, which gets pulled in as a dependency when the ``redis`` extra is
selected.

Records in Redis need to be encoded as byte strings. To this end, this module
also contains many implementations of the abstract :py:class:`.Codec` for
various data types. These also include the wrapper types :py:class:`.Nullable`
and :py:class:`.Array`.

While storing data in Redis seemed like a good idea a while ago, with changes
in the library's design concerning indexing, this implementation may be slower
than the local storage. Supporting multiple indexes requires at least one round
trip to get the relevant primary keys of records (one per score interval in the
records spec), then another to actually get the records. Deletions actually
have to fetch all records first in order to clean up the indexes, then delete
one by one.
"""

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
