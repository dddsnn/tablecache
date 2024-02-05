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
import typing as t

import tablecache.db as db
import tablecache.index as index
import tablecache.storage as storage
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

    Caches a (sub-)set of records that can only be accessed relatively
    slowly (DB) in a relatively fast storage. Not thread-safe.

    Serves single records by their primary key, or sets of records that can be
    specified as arguments to an Indexes instance. Transparently serves them
    from fast storage if available, or from the DB otherwise. The cache has to
    be loaded with load() to add the desired subset to storage.

    The DB state is not reflected automatically. If a record in the DB changes
    (or is deleted, or was newly added), invalidate_record() needs to be called
    for the cache to reflect that. This doesn't neceessarily trigger an
    immediate refresh, but it gurantees that the updated record is loaded from
    the DB before it is served the next time.

    Which subset of the records in DB is cached can be changed by calling
    adjust(). This operation asks a specified index what to delete and what to
    load new, so it is implementation-specific to the chosen index.

    In general, many of the cache's methods take an index name along with index
    args and kwargs. These all query the chache's Indexes instance, which acts
    as the abstraction between DB and storage. The Indexes make it possible to
    specify the same subset of records in both.
    """

    def __init__(
            self, indexes: index.Indexes, db_access: db.DbAccess,
            storage_table: storage.StorageTable, *, primary_key_name: str
    ) -> None:
        """
        :param indexes: An Indexes instance that is used to translate query
            arguments into ways of loading actual records, as well as keeping
            track of which records are in storage.
        :param db_access: The DB access.
        :param storage_table: The storage table.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in attribute_codecs.
        """
        self._indexes = indexes
        self._db_access = db_access
        self._storage_table = storage_table
        self._primary_key_name = primary_key_name
        self._invalid_record_repo = InvalidRecordRepository(indexes)
        self.loaded = False

    async def load(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> None:
        """
        Clear storage and load all relevant data from the DB into storage.

        This is very similar to adjust(), except that the storage is cleared
        first, and a ValueError is raised if the cache was already loaded.

        Like adjust(), calls the cache's indexes' adjust() to let them know
        which records are being loaded, and observe() for each record as it is
        loaded.

        Raises a ValueError if the specified index doesn't support adjusting.
        """
        if self.loaded:
            raise ValueError(
                'Already loaded. Use adjust() to change cached records.')
        _logger.info(
            f'Clearing and loading {self._indexes} of table '
            f'{self._storage_table.table_name}.')
        await self._storage_table.clear()
        num_deleted, num_loaded = await self._adjust(
            index_name, *index_args, **index_kwargs)
        self.loaded = True
        if num_deleted:
            _logger.warning(
                f'Deleted {num_deleted} records during loaded, after clearing '
                'the table (this is likely a benign defect in the Indexes '
                'implementation).')
        _logger.info(f'Loaded {num_loaded} records.')

    async def _adjust(self, index_name, *index_args, **index_kwargs):
        try:
            adjustment = self._indexes.adjust(
                index_name, *index_args, **index_kwargs)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                f'Indexes don\'t support adjusting by {index_name}.') from e
        num_deleted, num_loaded = 0, 0
        if adjustment.expire_spec:
            num_deleted = await self._storage_table.delete_records(
                adjustment.expire_spec)
        if adjustment.new_spec:
            async for record in self._db_access.get_records(
                    adjustment.new_spec):
                # PERF can we save time by guaranteeing here that records don't
                # exist yet?++++++++++++++++++++
                await self._storage_table.put_record(record)
                self._indexes.observe(record)
                num_loaded += 1
        return num_deleted, num_loaded

    async def adjust(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> None:
        """
        Adjust the set of records in storage.

        Expires records from storage and loads new ones from the DB in order to
        attain the state specified via the given index' implementation-specific
        parameters.

        Calls the cache's indexes' adjust() to let them know how the set of
        records is supposed to change, which yields the adjustment that
        specifies which have to be deleted and which newly fetched. For each
        record that ends up being added to storage, calls the indexes'
        observe() to inform them that this record exists.

        Raises a ValueError if the specified index doesn't support adjusting.
        """
        num_deleted, num_loaded = await self._adjust(
            index_name, *index_args, **index_kwargs)
        if num_deleted or num_loaded:
            _logger.info(
                f'Deleted {num_deleted} records and loaded {num_loaded} ones.')

    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Get a record by primary key.

        If the key has been marked as invalid, ensures the data is fresh first.

        If the key belongs to a record that isn't cached, queries the DB and
        serves it from there. This includes the case where no record with the
        given key exists, and the Indexes aren't aware of this (i.e.
        indexes.covers('primary_key', primary_key) returns False).

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
            return await self._db_access.get_record(db_records_spec)

    async def get_records(
            self, index_name: str, *index_args: t.Any, **index_kwargs: t.Any
    ) -> tp.AsyncRecords:
        """
        Asynchronously iterate over a set of records.

        Asynchronously iterates over the set of records specified via the
        implementation-specific arguments to the given index. Records are taken
        from fast storage if the index covers the requested set of records, and
        all of them are valid.

        A record can become invalid if it is marked as such by a call to
        invalidate_record(), or if any record (no matter which one) is marked
        as invalid without providing a new score for the index that is used to
        query here. The index may also not support a coverage check at all, in
        which case a ValueError is raised.

        Otherwise, records are taken from the (relatively slower) DB. This
        implies that querying a set of records that isn't covered (even if just
        by a little bit) is expensive.
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
            records = self._db_access.get_records(db_records_spec)
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
        record was deleted from or updated in the DB, or newly added
        altogether. Data belonging to an invalidated key is guaranteed to be
        fetched from the DB again before being served to a client. Keys that
        are no longer found in the DB are deleted. Keys that aren't in cache at
        all are loaded.

        Internally, the score of the record for each of the relevant indexes is
        required to mark it invalid for queries for a set of records against
        that index. Index scores may be given via the new_scores parameter,
        which is a dictionary mapping index names to the record's new scores.
        N.B.: These must be the record's new scores, i.e. if the record was
        updated in a way that changed it's score for any index, the new updated
        score must be provided.

        The implementation will trust that these are correct, and supplying
        wrong ones will lead to to record not being properly invalidated.
        Scores needn't be specified, however when they're not for any given
        index, that entire index is marked dirty, and any query against that
        index will trigger a full refresh. One exception to this is the
        primary_key index, for which no score needs to be specified since that
        score can always be calculated and primary keys can never change.

        Implementation note: updated and deleted records aren't observed for
        the Indexes again, only records that were newly added.
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
            record = await self._db_access.get_record(db_records_spec)
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
        updated_records = self._db_access.get_records(db_records_spec)
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
