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

"""
Simple cache for unwieldily joined relations.

``tablecache`` is a small library that caches tables in a slow database (or,
more likely, big joins of many tables) in a faster storage. It allows you to
define ways to index the data and then query ranges of records performantly.
Not all records have to be loaded into the cache, and ones that aren't are
transparently fetched from the underlying database instead. Cache entries that
become invalid must be marked as invalid, but are then refreshed automatically.
"""

__version__ = '4.2.0'

from tablecache.cache import CachedTable
from tablecache.db import DbAccess, DbRecordsSpec, QueryArgsDbRecordsSpec
from tablecache.index import (
    Adjustment,
    AllIndexes,
    Indexes,
    PrimaryKeyIndexes,
    PrimaryKeyRangeIndexes,
    RecordScorer,
    UnsupportedIndexOperation)
from tablecache.storage import Interval, StorageRecordsSpec, StorageTable
from tablecache.types import PrimaryKey, RecheckPredicate, Score
