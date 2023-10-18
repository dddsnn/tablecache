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

import logging
import redis.asyncio as redis
import typing as t

import tablecache.db as db
import tablecache.storage as storage
import tablecache.subset as ss
import tablecache.types as tp

_logger = logging.getLogger(__name__)


class CachedTable[PrimaryKey, CachedSubset: ss.CachedSubset]:
    """
    A cached table.

    Maintains records from a relatively slow storage (db_table) in a relatively
    fast RedisTable. Not thread-safe.

    The cache has to be loaded with load() before anything meaningful can
    happen. Many methods will raise a ValueError if this wasn't done. Calling
    load() more than once also raises a ValueError.
    """
    def __init__(
            self, cached_subset_class: type[CachedSubset],
            db_table: db.DbTable, *, primary_key_name: str,
            attribute_codecs: storage.AttributeCodecs, redis_conn: redis.Redis,
            redis_table_name: str) -> None:
        """
        :param cached_subset_class: The type of subset to use. This class is
            used in various methods that deal with subsets, like load() and
            get_record_subset(). These methods take args and kwargs to
            instantiate this class. This implies that the type of subset chosen
            here determines how the cache can be queried, and in particular
            that using the convenient All subset means you can only get
            individual records by primary key, or all records. The subset
            instantiated by load() is also used to keep track of which values
            within the subset are actually currently cached.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in attribute_codecs.
        :param attribute_codecs: Dictionary of codecs for record attributes.
            Must map attribute names (string) to tablecache.Codec instances
            that are able to en-/decode the corresponding values. Only
            attributes present here are stored.
        :param redis_conn: An async Redis connection. Used in the construction
            of a RedisTable. The connection will not be closed and needs to be
            cleaned up from the outside.
        :param redis_table_name: The name of the table, used as a prefix for
            keys in Redis. Must be unique within the Redis instance.
        """
        self._cached_subset_class = cached_subset_class
        self._db_table = db_table
        self._storage_table = storage.RedisTable(
            redis_conn, table_name=redis_table_name,
            primary_key_name=primary_key_name,
            attribute_codecs=attribute_codecs,
            score_function=cached_subset_class.record_score)
        self._cached_subset = None
        self._invalid_record_repo = InvalidRecordRepository()

    @property
    def cached_subset(self) -> CachedSubset:
        """The subset currently cached."""
        if not self._cached_subset:
            raise ValueError('Cache has not been loaded.')
        return self._cached_subset

    async def load(
            self, *cached_subset_args: list[t.Any],
            **cached_subset_kwargs: dict[str, t.Any]) -> None:
        """
        Load all relevant data from the DB into storage.

        Instantiates a cached subset from the configured class with the given
        args and kwargs and loads all records matching it. Clears the storage
        first.

        Raises a ValueError if the cache was already loaded.
        """
        if self._cached_subset:
            raise ValueError('Cache has already been loaded.')
        self._cached_subset = self._cached_subset_class(
            *cached_subset_args, **cached_subset_kwargs)
        _logger.info(
            f'Clearing and loading {self.cached_subset} of table '
            f'{self._storage_table.table_name}.')
        await self._storage_table.clear()
        num_loaded = await self._load_subset(self.cached_subset)
        _logger.info(f'Loaded {num_loaded} records.')

    async def _load_subset(self, subset: ss.Subset) -> None:
        num_loaded = 0
        async for record in self._db_table.get_record_subset(subset):
            await self._storage_table.put_record(record)
            self.cached_subset.observe(record)
            num_loaded += 1
        return num_loaded

    async def adjust_cached_subset(
            self, **subset_adjust_kwargs: dict[str, t.Any]) -> None:
        """
        Adjust the cached subset.

        Passes through the arguments to the cached subset's adjust(), and then
        deletes old and loads new records according to the result.
        """
        adjustment = self.cached_subset.adjust(**subset_adjust_kwargs)
        _logger.info(
            f'Adjusting table {self._storage_table.table_name} to '
            f'{self.cached_subset}.')
        num_deleted = await self._storage_table.delete_record_subset(
            adjustment.expire_intervals)
        num_loaded = await self._load_subset(adjustment.new_subset)
        _logger.info(
            f'Deleted {num_deleted} records and loaded {num_loaded} ones.')

    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Get a record from storage by primary key.

        In case the key has been marked as invalid, ensures the data is fresh
        first.

        In case the primary key doesn't exist in cache, also tries the DB in
        case the key is from outside the cached subset. This implies that
        querying keys that may not exist is potentially costly. There is
        however a special case if the cached subset is All (the trivial subset
        matching everything), where the DB is not checked (since everything is
        cached).

        Raises a KeyError if the key doesn't exist.
        """
        if primary_key in self._invalid_record_repo.invalid_primary_keys:
            await self._refresh_invalid()
        try:
            return await self._storage_table.get_record(primary_key)
        except KeyError:
            if isinstance(self.cached_subset, ss.All):
                raise
            return await self._db_table.get_record(primary_key)

    async def get_record_subset(
            self, *subset_args: list[t.Any],
            **subset_kwargs: dict[str, t.Any]) -> tp.Records:
        """
        Asynchronously iterate over records from a subset.

        Iterates over cached records from the given subset, but only if it is
        fully contained in the configured cache subset (i.e. no records are
        missing). Otherwise, queries the DB for the entire subset and yields
        those records. This implies that querying a subset that isn't
        completely in cache (even if just by a little bit) is expensive.
        """
        subset = self._cached_subset_class(*subset_args, **subset_kwargs)
        if any(s in subset for s in self._invalid_record_repo.invalid_scores):
            await self._refresh_invalid()
        if self.cached_subset.covers(subset):
            source = self._storage_table
        else:
            source = self._db_table
        async for record in source.get_record_subset(subset):
            yield record

    async def invalidate_record(self, primary_key: PrimaryKey) -> None:
        """
        Mark a single record in storage as invalid.

        Data belonging to an invalidated key is guaranteed to be fetched from
        the DB again before being served to a client. Keys that are no longer
        found in the DB are deleted. Keys that aren't in cache are ignored.

        Implementation note: refreshed records aren't observed for the cached
        subset again. Since record scores aren't allowed to change as per the
        Subset contract, this isn't an issue.
        """
        try:
            record = await self._storage_table.get_record(primary_key)
        except KeyError:
            _logger.debug(
                f'Ignoring attempt to invalidate primary key {primary_key} '
                'which doesn\'t exist.')
            return
        score = self.cached_subset.record_score(record)
        self._invalid_record_repo.flag_invalid(primary_key, score)

    async def _refresh_invalid(self) -> None:
        _logger.info(
            f'Refreshing {len(self._invalid_record_repo)} invalid keys.')
        for key in self._invalid_record_repo.invalid_primary_keys:
            await self._storage_table.delete_record(key)
        async for record in self._db_table.get_records(
                self._invalid_record_repo.invalid_primary_keys):
            await self._storage_table.put_record(record)
        self._invalid_record_repo.clear()


class InvalidRecordRepository:
    def __init__(self):
        self.invalid_primary_keys = set()
        self.invalid_scores = set()

    def __len__(self):
        return len(self.invalid_primary_keys)

    def flag_invalid(self, primary_key, score):
        self.invalid_primary_keys.add(primary_key)
        self.invalid_scores.add(score)

    def primary_key_is_invalid(self, primary_key):
        return primary_key in self.invalid_primary_keys

    def clear(self):
        self.invalid_primary_keys.clear()
        self.invalid_scores.clear()
