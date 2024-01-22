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
import numbers
import redis.asyncio as redis
import typing as t

import tablecache.db as db
import tablecache.index as index
import tablecache.redis
import tablecache.types as tp

_logger = logging.getLogger(__name__)


async def _async_generator(xs):
    for x in xs:
        yield x


class DirtyIndex(Exception):
    """
    Raised to indicate that the queried index is dirty and the cache must be
    refreshed.
    """


class CachedTable[PrimaryKey]:
    """
    A cached table.

    Maintains records from a relatively slow storage (db_table) in a relatively
    fast RedisTable. Not thread-safe.

    The cache has to be loaded with load() before anything meaningful can
    happen. Many methods will raise a ValueError if this wasn't done. Calling
    load() more than once also raises a ValueError.
    """

    def __init__(
            self,
            indexes: index.Indexes,
            db_table: db.DbTable,
            *,
            primary_key_name: str,
            attribute_codecs: tablecache.redis.AttributeCodecs,
            redis_conn: redis.Redis,
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
        self._indexes = indexes
        self._db_table = db_table
        self._primary_key_name = primary_key_name
        self._storage_table = tablecache.redis.RedisTable(
            redis_conn, table_name=redis_table_name,
            primary_key_name=primary_key_name,
            attribute_codecs=attribute_codecs,
            score_functions=indexes.score_functions)
        self._invalid_record_repo = InvalidRecordRepository(indexes)

    async def load(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> None:
        """
        Load all relevant data from the DB into storage.

        Instantiates a cached subset from the configured class with the given
        args and kwargs and loads all records matching it. Clears the storage
        first.

        Raises a ValueError if the cache was already loaded.
        """
        _logger.info(
            f'Clearing and loading {self._indexes} of table '
            f'{self._storage_table.table_name}.')
        await self._storage_table.clear()
        try:
            adjustment = self._indexes.adjust(
                index_name, *index_args, **index_kwargs)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                f'Indexes don\'t support adjusting by {index_name}.') from e
        num_loaded = await self._load_subset(adjustment.new_spec)
        _logger.info(f'Loaded {num_loaded} records.')

    async def _load_subset(self, db_records_spec):
        num_loaded = 0
        async for record in self._db_table.get_records(db_records_spec):
            await self._storage_table.put_record(record)
            self._indexes.observe(record)
            num_loaded += 1
        return num_loaded

    async def adjust_cached_subset(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> None:
        """
        Adjust the cached subset.

        Passes through the arguments to the cached subset's adjust(), and then
        deletes old and loads new records according to the result.
        """
        try:
            adjustment = self._indexes.adjust(
                index_name, *index_args, **index_kwargs)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                f'Indexes don\'t support adjusting by {index_name}.') from e
        if adjustment.expire_spec:
            num_deleted = await self._storage_table.delete_records(
                adjustment.expire_spec)
        else:
            num_deleted = 0
        if adjustment.new_spec:
            num_loaded = await self._load_subset(adjustment.new_spec)
        else:
            num_loaded = 0
        if num_deleted or num_loaded:
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
        if self._invalid_record_repo.primary_key_is_invalid(primary_key):
            await self._refresh_invalid()
        try:
            return await self._storage_table.get_record(primary_key)
        except KeyError:
            if self._indexes.covers('primary_key', primary_key):
                raise
            db_records_spec = self._indexes.db_records_spec(
                'primary_key', primary_key)
            return await self._db_table.get_record(db_records_spec)

    async def get_records(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> tp.Records:
        """
        Asynchronously iterate over records from a subset.

        Iterates over cached records from the given subset, but only if it is
        fully contained in the configured cache subset (i.e. no records are
        missing). Otherwise, queries the DB for the entire subset and yields
        those records. This implies that querying a subset that isn't
        completely in cache (even if just by a little bit) is expensive.
        """
        try:
            get_from_storage = self._indexes.covers(
                index_name, *index_args, **index_kwargs)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                f'Indexes don\'t support coverage check on {index_name}.'
            ) from e
        if get_from_storage:
            if len(self._invalid_record_repo) == 0:
                records = self._storage_table.get_records(
                    self._indexes.storage_records_spec(
                        index_name, *index_args, **index_kwargs))
            else:
                records = await self._check_and_get_records_from_storage(
                    index_name, *index_args, **index_kwargs)
        else:
            db_records_spec = self._indexes.db_records_spec(
                index_name, *index_args, **index_kwargs)
            records = self._db_table.get_records(db_records_spec)
        async for record in records:
            yield record

    async def _check_and_get_records_from_storage(
            self, index_name, *index_args, **index_kwargs):
        records_spec = self._indexes.storage_records_spec(
            index_name, *index_args, **index_kwargs)
        if not self._intervals_are_valid(records_spec):
            return await self._refresh_and_get_records_from_storage(
                index_name, *index_args, **index_kwargs)
        records = []
        async for record in self._storage_table.get_records(
                records_spec):
            if self._invalid_record_repo.primary_key_is_invalid(
                    record[self._primary_key_name]):
                return await self._refresh_and_get_records_from_storage(
                    index_name, *index_args, **index_kwargs)
            records.append(record)
        return _async_generator(records)

    def _intervals_are_valid(self, records_spec):
        for interval in records_spec.score_intervals:
            try:
                if self._invalid_record_repo.interval_contains_invalid_score(
                        records_spec.index_name, interval):
                    return False
            except DirtyIndex:
                return False
        return True

    async def _refresh_and_get_records_from_storage(
            self, index_name, *index_args, **index_kwargs):
        await self._refresh_invalid()
        return self._storage_table.get_records(
            self._indexes.storage_records_spec(
                index_name, *index_args, **index_kwargs))

    async def invalidate_record(
            self, primary_key: PrimaryKey,
            new_scores: t.Optional[t.Mapping[str, numbers.Real]] = None
    ) -> None:
        """
        Mark a single record in storage as invalid.

        The record with the given primary key is marked as not existing in
        storage with the same data as in DB. This could be either because the
        record was updated in the DB, or newly added altogether. Data belonging
        to an invalidated key is guaranteed to be fetched from the DB again
        before being served to a client. Keys that are no longer found in the
        DB are deleted. Keys that aren't in cache at all are loaded.

        Internally, the score of the record is required to mark it invalid for
        subset queries. The score may be given as the score_hint parameter. The
        implementation will trust that this parameter is correct, and supplying
        a wrong one will lead to to record not being properly invalidated. When
        not supplying one at all, the current record is first fetched from
        storage in order to calculate the score at a small extra cost.

        Implementation note: updated and deleted records aren't observed for
        the cached subset again. As long as record scores aren't allowed to
        change even when the record does (as per the Subset contract), this
        isn't an issue. Records that were newly added are observed so the
        subset can add their scores.

        """
        new_scores = new_scores or {}
        try:
            await self._storage_table.get_record(primary_key)
            self._invalid_record_repo.flag_invalid(primary_key, new_scores)
        except KeyError:
            await self._invalidate_add_new(primary_key)

    async def _invalidate_add_new(self, primary_key):
        try:
            db_records_spec = self._indexes.db_records_spec(
                'primary_key', primary_key)
            record = await self._db_table.get_record(db_records_spec)
            await self._storage_table.put_record(record)
            self._indexes.observe(record)
        except KeyError:
            _logger.debug(
                f'Ignoring attempt to invalidate primary key {primary_key} '
                'which doesn\'t exist.')

    async def _refresh_invalid(self) -> None:
        _logger.info(
            f'Refreshing {len(self._invalid_record_repo)} invalid keys.')
        for key in self._invalid_record_repo.invalid_primary_keys:
            try:
                await self._storage_table.delete_record(key)
            except KeyError:
                pass
        db_records_spec = self._indexes.db_records_spec(
            'primary_key', *self._invalid_record_repo.invalid_primary_keys)
        updated_records = self._db_table.get_records(db_records_spec)
        async for record in updated_records:
            await self._storage_table.put_record(record)
        self._invalid_record_repo.clear()


class InvalidRecordRepository[PrimaryKey]:
    """
    A repository of invalid records.

    Keeps track of which records have been marked as invalid, along with their
    scores for all indexes.
    """

    def __init__(self, indexes: index.Indexes) -> None:
        self._primary_key_score = indexes.primary_key_score
        self.invalid_primary_keys = set()
        self._invalid_scores = {n: set() for n in indexes.index_names}
        self._dirty_indexes = set()

    def __len__(self) -> int:
        return len(self.invalid_primary_keys)

    def flag_invalid(
            self, primary_key: PrimaryKey,
            scores: t.Mapping[str, numbers.Real]) -> None:
        """
        Flag a record as invalid.

        scores maps index names to the record's respective score for that
        index. These scores are trusted, as there is no way of calculating them
        here.

        Not all scores have to be provided, but for the ones that aren't the
        respective index is marked as dirty. This means that future calls to
        score_is_invalid() for that index will raise an exception, since there
        is no longer a way to check whether the score is valid or not. One
        exception to this is the special primary_key index, the score of which
        can be calculated if it is missing.
        """
        self.invalid_primary_keys.add(primary_key)
        if 'primary_key' not in scores:
            scores['primary_key'] = self._primary_key_score(
                primary_key)
        for index_name, invalid_scores in self._invalid_scores.items():
            try:
                invalid_scores.add(scores[index_name])
            except KeyError:
                self._dirty_indexes.add(index_name)

    def primary_key_is_invalid(self, primary_key: PrimaryKey) -> bool:
        """Check whether a primary key is invalid."""
        return primary_key in self.invalid_primary_keys

    def interval_contains_invalid_score(
            self, index_name: str, interval: index.Interval) -> bool:
        """
        Check whether the interval contains an invalid score.

        If the queried index has been marked as dirty, raises DirtyIndex.
        """
        if index_name in self._dirty_indexes:
            raise DirtyIndex
        return any(
            score in interval for score in self._invalid_scores[index_name])

    def clear(self) -> None:
        """Reset the state."""
        self.invalid_primary_keys.clear()
        for invalid_scores in self._invalid_scores.values():
            invalid_scores.clear()
        self._dirty_indexes.clear()
