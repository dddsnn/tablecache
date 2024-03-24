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

import asyncio
import itertools as it
import logging
import typing as t

import tablecache.db as db
import tablecache.index as index
import tablecache.storage as storage
import tablecache.types as tp

_logger = logging.getLogger(__name__)


async def _async_generator(xs):
    for x in xs:
        yield x


class CachedTable[PrimaryKey: tp.PrimaryKey]:
    """
    A cached table.

    Caches a (sub-)set of records that can only be accessed relatively slowly
    (DB) in a relatively fast storage. Not thread-safe.

    Serves sets of records that can be specified as arguments to an Indexes
    instance. Transparently serves them from fast storage if available, or from
    the DB otherwise. The cache has to be loaded with load() to add the desired
    records to storage. Read access is blocked until this completes. A
    convenience function that returns a single record is available.

    Most methods for which records need to be specified can either be called
    with an IndexSpec appropriate to the cache's Indexes instance, or more
    conveniently with args and kwargs that will be used to construct an
    IndexSpec (the exception to this is invalidate_records(), which needs
    multiple IndexSpecs).

    The DB state is not reflected automatically. If one or more records in the
    DB change (or are deleted or newly added), invalidate_records() needs to be
    called for the cache to reflect that. This doesn't trigger an immediate
    refresh, but it gurantees that the updated record is loaded from the DB
    before it is served the next time.

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
            storage_table: storage.StorageTable) -> None:
        """
        :param indexes: An Indexes instance that is used to translate query
            arguments into ways of loading actual records, as well as keeping
            track of which records are in storage.
        :param db_access: The DB access.
        :param storage_table: The storage table.
        """
        self._indexes = indexes
        self._db_access = db_access
        self._storage_table = storage_table
        self._invalid_record_repo = InvalidRecordRepository(indexes)
        self._loaded_event = asyncio.Event()
        self._scratch_space_lock = asyncio.Lock()

    async def loaded(self):
        """
        Wait until the table is loaded.

        Blocks until the initial load completes. Once this returns, read access
        becomes enabled. This can be used e.g. in a readiness check.
        """
        await self._loaded_event.wait()

    async def load(self, *args: t.Any, **kwargs: t.Any) -> None:
        """
        Clear storage and load all relevant data from the DB into storage.

        Takes either a single IndexSpec instance or args and kwargs to
        construct one.

        This is very similar to adjust(), except that the storage is cleared
        first, a ValueError is raised if the cache was already loaded, and the
        whole operation doesn't take place in scratch space.

        Like adjust(), calls the cache's indexes' prepare_adjustment() to
        determine which records need to be loaded, and then commit_adjustment()
        when they have. Additionally, for each loaded record the adjustment's
        observe_loaded() is called.

        Raises a ValueError if the specified index doesn't support adjusting.
        """
        index_spec = self._make_index_spec(*args, **kwargs)
        if self._loaded_event.is_set():
            raise ValueError(
                'Already loaded. Use adjust() to change cached records.')
        _logger.info(
            f'Clearing and loading {self._indexes} of table '
            f'{self._storage_table}.')
        await self._storage_table.clear()
        num_deleted, num_loaded = await self._adjust_plain(index_spec)
        self._loaded_event.set()
        if num_deleted:
            _logger.warning(
                f'Deleted {num_deleted} records during loading, after '
                'clearing the table (this is likely a benign defect in the '
                'Indexes implementation).')
        _logger.info(f'Loaded {num_loaded} records.')

    def _make_index_spec(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], self._indexes.IndexSpec):
            return args[0]
        return self._indexes.IndexSpec(*args, **kwargs)

    async def _adjust_plain(self, index_spec):
        return await self._adjust(False, index_spec)

    async def _adjust_in_scratch(self, index_spec):
        async with self._scratch_space_lock:
            await self._refresh_invalid_locked()
            return await self._adjust(True, index_spec)

    async def _adjust(self, use_scratch, index_spec):
        try:
            adjustment = self._indexes.prepare_adjustment(index_spec)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                f'Indexes don\'t support adjusting by {index_spec.index_name}.'
            ) from e
        if use_scratch:
            put = self._storage_table.scratch_put_record
            delete = self._storage_table.scratch_discard_records
        else:
            put = self._storage_table.put_record
            delete = self._storage_table.delete_records
        num_deleted, num_loaded = await self._apply_adjustment(
            adjustment, put, delete)
        if use_scratch:
            self._storage_table.scratch_merge()
        self._indexes.commit_adjustment(adjustment)
        return num_deleted, num_loaded

    async def _apply_adjustment(self, adjustment, put, delete):
        num_deleted = num_loaded = 0
        if adjustment.expire_spec:
            async for record in delete(adjustment.expire_spec):
                adjustment.observe_expired(record)
                num_deleted += 1
        if adjustment.new_spec:
            async for record in self._db_access.get_records(
                    adjustment.new_spec):
                await put(record)
                adjustment.observe_loaded(record)
                num_loaded += 1
        return num_deleted, num_loaded

    async def adjust(self, *args: t.Any, **kwargs: t.Any) -> None:
        """
        Adjust the set of records in storage.

        Takes either a single IndexSpec instance or args and kwargs to
        construct one.

        Expires records from storage and loads new ones from the DB in order to
        attain the state specified via the index spec. Uses the storage's
        scratch space to provide a consistent view of the storage without
        blocking read operations. At all points before this method returns read
        operations reflect the state before the adjustment, and at all points
        after they reflect the state after.

        Calls the cache's indexes' prepare_adjustment() for specs on the
        records that should be expired and new ones to load. These are then
        staged in the storage's scratch space. For each record that is expired
        or loaded, the adjustment's observe_expired() or observe_loaded() is
        called. Finally, the scratch space is merged, the indexes'
        commit_adjustment() is called

        Note that, since observe() is called at the very end, a full list of
        all added records needs to be stored temporarily. This may consume a
        significant amount of extra memory with large adjustments.

        Only one adjustment or refresh (via refresh_invalid()) can be happening
        at once. Other ones are locked until previous ones complete. Before the
        adjustment, any invalid records are refreshed.

        Raises a ValueError if the specified index doesn't support adjusting.
        """
        index_spec = self._make_index_spec(*args, **kwargs)
        await self.loaded()
        num_deleted, num_loaded = await self._adjust_in_scratch(index_spec)
        if num_deleted or num_loaded:
            _logger.info(
                f'Deleted {num_deleted} records and loaded {num_loaded} ones.')

    async def get_first_record(
            self, *args: t.Any, **kwargs: t.Any) -> tp.Record:
        """
        Get a single record.

        This is a convenience function around get_records(). It returns the
        first record get_records() would have with the same arguments.

        Note that records don't have a defined order, so this should only be
        used if exactly 0 or 1 record is expected to be returned.

        Raises a KeyError if no such record exists.
        """
        records = self.get_records(*args, **kwargs)
        try:
            return await anext(records)
        except StopAsyncIteration:
            raise KeyError

    async def get_records(
            self, *args: t.Any, **kwargs: t.Any) -> tp.AsyncRecords:
        """
        Asynchronously iterate over a set of records.

        Takes either a single IndexSpec instance or args and kwargs to
        construct one.

        Asynchronously iterates over the set of records specified via the index
        spec. Records are taken from fast storage if the index covers the
        requested set of records and all of them are valid.

        A record can become invalid if it is marked as such by a call to
        invalidate_record(), or if any record (no matter which one) is marked
        as invalid without providing scores for the index that is used to query
        here. The index may also not support a coverage check at all, in which
        case a ValueError is raised.

        Otherwise, records are taken from the (relatively slower) DB. This
        implies that querying a set of records that isn't covered (even if just
        by a little bit) is expensive.
        """
        index_spec = self._make_index_spec(*args, **kwargs)
        await self.loaded()
        try:
            get_from_storage = self._indexes.covers(index_spec)
        except index.UnsupportedIndexOperation as e:
            raise ValueError(
                'Indexes don\'t support coverage check on '
                f'{index_spec.index_name}.') from e
        if get_from_storage:
            if not self._invalid_record_repo:
                records = self._storage_table.get_records(
                    self._indexes.storage_records_spec(index_spec))
            else:
                records = await self._check_and_get_records_from_storage(
                    index_spec)
        else:
            db_records_spec = self._indexes.db_records_spec(
                index_spec)
            records = self._db_access.get_records(db_records_spec)
        async for record in records:
            yield record

    async def _check_and_get_records_from_storage(self, index_spec):
        records_spec = self._indexes.storage_records_spec(index_spec)
        if not self._intervals_are_valid(records_spec):
            return await self._refresh_and_get_records_from_storage(index_spec)
        records = []
        async for record in self._storage_table.get_records(records_spec):
            records.append(record)
        return _async_generator(records)

    def _intervals_are_valid(self, records_spec):
        for interval in records_spec.score_intervals:
            if self._invalid_record_repo.interval_intersects_invalid(
                    records_spec.index_name, interval):
                return False
        return True

    async def _refresh_and_get_records_from_storage(self, index_spec):
        await self.refresh_invalid()
        return self._storage_table.get_records(
            self._indexes.storage_records_spec(index_spec))

    def invalidate_records(
            self, old_index_specs: list[index.Indexes[PrimaryKey].IndexSpec],
            new_index_specs: list[index.Indexes[PrimaryKey].IndexSpec]
    ) -> None:
        """
        Mark a single record in storage as invalid.

        The record with the given primary key is marked as not existing in
        storage with the same data as in DB. This could either be because the
        record was deleted from the DB, or because it was updated in the DB.
        Data belonging to an invalidated key is guaranteed to be fetched from
        the DB again before being served to a client. Keys that are no longer
        found in the DB are deleted.

        If the given primary key doesn't exist in storage, a KeyError is
        raised. This method can't be used to load new records, use adjust() for
        that.

        Internally, the score of the record for each of the relevant indexes is
        required to mark it invalid for queries for a set of records against
        that index. Index scores may be given via the new_scores parameter,
        which is a dictionary mapping index names to the record's new scores.
        N.B.: These must be the record's new scores, i.e. if the record was
        updated in a way that changed it's score for any index, the new updated
        score must be provided.

        The implementation will trust that these are correct, and supplying
        wrong ones will lead to the record not being properly invalidated.
        Scores needn't be specified, however when they're not for any given
        index, that entire index is marked dirty, and any query against that
        index will trigger a full refresh. One exception to this is the
        primary_key index, for which no score needs to be specified since that
        score can always be calculated and primary keys can never change.

        This method needs to wait for any ongoing adjustments or refreshes, so
        it may occasionally take a while.

        Implementation note: updated and deleted records aren't observed for
        the indexes again.
        """
        if not self._loaded_event.is_set():
            raise ValueError('Table is not yet loaded.')
        index_for_refresh = {}
        by_name = {'old': {}, 'new': {}}
        for index_spec, old_or_new in it.chain(
                zip(old_index_specs, it.repeat('old')),
                zip(new_index_specs, it.repeat('new'))):
            try:
                index_for_refresh.setdefault(old_or_new, index_spec.index_name)
                if self._indexes.covers(index_spec):
                    if index_spec.index_name in by_name[old_or_new]:
                        raise ValueError(
                            f'Index {index_spec.index_name} specified more '
                            'than once.')
                    by_name[old_or_new][index_spec.index_name] = index_spec
                    continue
            except index.UnsupportedIndexOperation:
                pass
            raise ValueError(
                f'Index spec {index_spec} is not certainly covered by the '
                'indexes.')
        if len(index_for_refresh) != 2:
            raise ValueError(
                'At least one old and one new index spec must be given.')
        self._invalid_record_repo.flag_invalid(
            by_name['old'], by_name['new'], index_for_refresh['old'],
            index_for_refresh['new'])

    async def refresh_invalid(self) -> None:
        """
        Refresh all records that have been marked as invalid.

        Ensures that all records that have been marked as invalid since the
        last refresh are loaded again from the DB.

        This operation needs to wait for any ongoing adjustments to finish. No
        refresh is triggered if all records are valid already, or if there is
        another refresh still ongoing.
        """
        if not self._invalid_record_repo:
            return
        async with self._scratch_space_lock:
            await self._refresh_invalid_locked()

    async def _refresh_invalid_locked(self):
        # Checking again avoids a second refresh in case one just happened
        # while we were waiting on the lock.
        if not self._invalid_record_repo:
            return
        _logger.info(
            f'Refreshing {len(self._invalid_record_repo)} invalid index '
            'specs.')
        for old_spec, new_spec in (
                self._invalid_record_repo.specs_for_refresh()):
            async for _ in self._storage_table.scratch_discard_records(
                    self._indexes.storage_records_spec(old_spec)):
                pass
            db_records_spec = self._indexes.db_records_spec(new_spec)
            async for record in self._db_access.get_records(db_records_spec):
                await self._storage_table.scratch_put_record(record)
        self._storage_table.scratch_merge()
        self._invalid_record_repo.clear()


class InvalidRecordRepository[PrimaryKey: tp.PrimaryKey]:
    """
    A repository of invalid records.

    Keeps track of intervals of records scores which have been marked as
    invalid, along with index specs to do the eventual refresh with.
    """

    def __init__(self, indexes: index.Indexes[PrimaryKey]) -> None:
        self._indexes = indexes
        self.clear()

    def __len__(self) -> int:
        return len(self._specs_for_refresh)

    def flag_invalid(
        self,
        old_index_specs:
            t.Mapping[str, index.Indexes[PrimaryKey].IndexSpec],
        new_index_specs:
            t.Mapping[str, index.Indexes[PrimaryKey].IndexSpec],
        old_index_for_refresh: str, new_index_for_refresh: str
    ) -> None:
        """
        Flag records as invalid.

        Takes 2 dictionaries of index specs, one specifying the invalid records
        as they exist in storage now, the other specifying the updated records.
        These may be the same. Each dictionary maps index names to
        corresponding index specs. All score intervals in the corresponding
        StorageRecordsSpecs will be considered invalid in
        interval_intersects_invalid(). Any keys in the dictionaries that aren't
        names of indexes are ignored.

        Not every index has to be present in the specs, but for the ones that
        aren't the respective index is marked as dirty. This means that future
        calls to interval_intersects_invalid() for that index will always
        return True, since there is no longer a way to know what scores are
        valid.

        One old and one new index spec is stored to be part of
        specs_for_refresh(). Which are chosen must be specified via
        {old,new}_index_for_refresh. These must be keys into the respective
        dictionaries.
        """
        if not old_index_specs or not new_index_specs:
            raise ValueError(
                'Must provide at least one old and one new index spec.')
        self._specs_for_refresh.append(
            (old_index_specs[old_index_for_refresh],
             new_index_specs[new_index_for_refresh]))
        for index_name in self._indexes.index_names:
            self._record_invalid_intervals(
                index_name, old_index_specs, new_index_specs)

    def _record_invalid_intervals(
            self, index_name, old_index_specs, new_index_specs):
        try:
            old_index_spec = old_index_specs[index_name]
            new_index_spec = new_index_specs[index_name]
        except KeyError:
            self._dirty_indexes.add(index_name)
            return
        for index_spec in (old_index_spec, new_index_spec):
            records_spec = self._indexes.storage_records_spec(index_spec)
            self._invalid_intervals[index_name].update(
                records_spec.score_intervals)

    def specs_for_refresh(self) -> t.Iterable[tuple[
            index.Indexes[PrimaryKey].IndexSpec,
            index.Indexes[PrimaryKey].IndexSpec]]:
        """
        Iterate index specs to do a refresh with.

        Generates tuples (old, new), where old is an index spec of records that
        have become invalid in storage, and new is an index spec specifying the
        records in their valid state in the DB.
        """
        yield from self._specs_for_refresh

    def interval_intersects_invalid(
            self, index_name: str, interval: storage.Interval) -> bool:
        """
        Check whether the interval contains an invalid score.

        This is the case if the interval intersects any of the intervals
        previously marked invalid for the given index, or if the entire index
        has been marked as dirty.

        If the given index_name doesn't exist, raises a KeyError.
        """
        if index_name in self._dirty_indexes:
            return True
        for invalid_interval in self._invalid_intervals[index_name]:
            if interval.intersects(invalid_interval):
                return True
        return False

    def clear(self) -> None:
        """Reset the state."""
        self._invalid_intervals = {n: set() for n in self._indexes.index_names}
        self._specs_for_refresh = []
        self._dirty_indexes = set()
