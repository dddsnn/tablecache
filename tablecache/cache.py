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
The :py:class:`CachedTable` is the main point of interaction, providing
transparent access to the records of a table (or a join of tables, or any
result set). It gets these records either from a :py:class:`.DbAccess`, or
preferrably a fast :py:class:`.StorageTable`. Access to records in both is tied
together by an :py:class:`.Indexes` instance.
"""

import asyncio
import collections.abc as ca
import logging
import typing as t

import tablecache.db as db
import tablecache.index as index
import tablecache.metrics as metrics
import tablecache.storage as storage
import tablecache.types as tp


class CachedTable[Record, PrimaryKey: tp.PrimaryKey]:
    """
    A cached table.

    Caches a (sub-)set of records that can only be accessed relatively slowly
    (DB) in a relatively fast storage. Not thread-safe.

    Serves sets of records that can be specified as arguments to an
    :py:class:`Indexes` instance. Transparently serves them from fast storage
    if available, or from the DB otherwise. The cache has to be loaded with
    :py:meth:`load` to add the desired records to storage. Read access is
    blocked until this completes. A convenience method
    :py:meth:`get_first_record` that returns a single record is available.

    Most methods for which records need to be specified can either be called
    with an :py:class:`IndexSpec <Indexes.IndexSpec>` appropriate to the
    cache's :py:class:`Indexes` instance, or more conveniently with ``args``
    and ``kwargs`` that will be passed to the :py:class:`Indexes.IndexSpec`
    inner class in order to construct one (the exception to this is
    :py:meth:`invalidate_records`, which needs multiple :py:class:`IndexSpec
    <Indexes.IndexSpec>` s).

    The DB state is not reflected automatically. If one or more records in the
    DB change (or are deleted or newly added), :py:meth:`invalidate_records`
    needs to be called for the cache to reflect that. This doesn't trigger an
    immediate refresh, but it guarantees that the updated record is loaded from
    the DB before it is served the next time.

    Which subset of the records in DB is cached can be changed by calling
    :py:meth:`adjust`. This operation can load new records and also expire ones
    no longer needed.
    """

    def __init__(
            self, indexes: index.Indexes[Record, PrimaryKey],
            db_access: db.DbAccess,
            storage_table: storage.StorageTable[Record]) -> None:
        """
        :param indexes: An :py:class:`Indexes` instance that is used to
            translate query arguments into ways of loading actual records, as
            well as keeping track of which records are in storage.
        :param db_access: The DB access used as the underlying source of truth.
        :param storage_table: The storage table used to cache records.
        """
        self._indexes = indexes
        self._db_access = db_access
        self._storage_table = storage_table
        self._invalid_record_repo = InvalidRecordRepository(indexes)
        self._loaded_event = asyncio.Event()
        self._scratch_space_lock = asyncio.Lock()
        self._logger = logging.getLogger(
            f'tablecache.CachedTable({storage_table.name})')
        self._metric_reads = metrics.get_counter(
            'tablecache_cached_table_reads_total',
            'Number of read operations on the cached table',
            ['table_name', 'type'])
        self._metric_refreshes = metrics.get_counter(
            'tablecache_cached_table_refreshes_total',
            'Number of refreshes performed on the cached table',
            ['table_name']).labels(table_name=storage_table.name)
        self._metric_adjustments = metrics.get_counter(
            'tablecache_cached_table_adjustments_total',
            'Number of adjustments performed on the cached table',
            ['table_name']).labels(table_name=storage_table.name)
        self._metric_adjustments_expired = metrics.get_counter(
            'tablecache_cached_table_adjustment_expired_total',
            'Number of records expired during adjustments on the cached table',
            ['table_name']).labels(table_name=storage_table.name)
        self._metric_adjustments_loaded = metrics.get_counter(
            'tablecache_cached_table_adjustment_loaded_total',
            'Number of records loaded during adjustments on the cached table',
            ['table_name']).labels(table_name=storage_table.name)

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

        Takes either a single :py:class:`IndexSpec <Indexes.IndexSpec>`
        instance or args and kwargs to construct one.

        This is very similar to :py:meth:`adjust`, except that the storage is
        cleared first, a :py:exc:`ValueError` is raised if the cache was
        already loaded, and the whole operation doesn't take place in scratch
        space.

        Like :py:meth:`adjust`, calls the cache's indexes'
        :py:meth:`prepare_adjustment <Indexes.prepare_adjustment>` to determine
        which records need to be loaded, and then
        :py:meth:`commit_adjustment <Indexes.commit_adjustment>` when they
        have. Additionally, for each loaded record the adjustment's
        :py:meth:`observe_loaded <Adjustment.observe_loaded>` is called.

        :raise ValueError: If the specified index doesn't support adjusting.
        """
        index_spec = self._make_index_spec(*args, **kwargs)
        if self._loaded_event.is_set():
            raise ValueError(
                'Already loaded. Use adjust() to change cached records.')
        self._logger.info(
            f'Clearing and loading {self._storage_table} with {index_spec}.')
        await self._storage_table.clear()
        num_deleted, num_loaded = await self._adjust_plain(index_spec)
        self._loaded_event.set()
        if num_deleted:
            self._logger.warning(
                f'Deleted {num_deleted} records during loading, after '
                'clearing the table (this is likely a benign defect in the '
                'Indexes implementation).')
        self._logger.info(f'Loaded {num_loaded} records.')

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
        self._logger.info(f'Applying {adjustment}.')
        self._indexes.commit_adjustment(adjustment)
        self._metric_adjustments_expired.inc(num_deleted)
        self._metric_adjustments_loaded.inc(num_loaded)
        self._metric_adjustments.inc()
        return num_deleted, num_loaded

    async def _apply_adjustment(self, adjustment, put, delete):
        num_deleted = num_loaded = 0
        if adjustment.expire_spec:
            async for record in delete(adjustment.expire_spec):
                adjustment.observe_expired(record)
                num_deleted += 1
        if adjustment.load_spec:
            async for record in self._db_access.get_records(
                    adjustment.load_spec):
                await put(record)
                adjustment.observe_loaded(record)
                num_loaded += 1
        return num_deleted, num_loaded

    async def adjust(self, *args: t.Any, **kwargs: t.Any) -> None:
        """
        Adjust the set of records in storage.

        Takes either a single :py:class:`IndexSpec <Indexes.IndexSpec>`
        instance or args and kwargs to construct one.

        Expires records from storage and loads new ones from the DB in order to
        attain the state specified via the index spec. Uses the storage's
        scratch space to provide a consistent view of the storage without
        blocking read operations. At all points before this method returns,
        read operations reflect the state before the adjustment, and at all
        points after they reflect the state after.

        Calls the cache's indexes'
        :py:meth:`prepare_adjustment <Indexes.prepare_adjustment>` for specs on
        the records that should be expired and new ones to load. These are then
        staged in the storage's scratch space. For each record that is expired
        or loaded, the adjustment's
        :py:meth:`observe_expired <Adjustment.observe_expired>` or
        :py:meth:`observe_loaded <Adjustment.observe_loaded>` is called.
        Finally, the scratch space is merged, and the indexes'
        :py:meth:`prepare_adjustment <Indexes.commit_adjustment>` is called.

        Only one adjustment or refresh (via :py:meth:`refresh_invalid`) can be
        happening at once. Other ones are locked until previous ones complete.
        Before the adjustment, any invalid records are refreshed.

        :raise ValueError: If the specified index doesn't support adjusting.
        """
        index_spec = self._make_index_spec(*args, **kwargs)
        await self.loaded()
        self._logger.info(f'Preparing adjustment to {index_spec}.')
        num_deleted, num_loaded = await self._adjust_in_scratch(index_spec)
        if num_deleted or num_loaded:
            self._logger.info(
                f'Deleted {num_deleted} records and loaded {num_loaded} ones.')

    async def get_first_record(self, *args: t.Any, **kwargs: t.Any) -> Record:
        """
        Get a single record.

        This is a convenience function around :py:meth:`get_records`. It
        returns the first record it would have with the same arguments.

        Note that records don't have a defined order, so this should only be
        used if exactly 0 or 1 record is expected to be returned.

        :raise KeyError: If no such record exists.
        """
        records = self.get_records(*args, **kwargs)
        try:
            return await anext(records)
        except StopAsyncIteration:
            raise KeyError
        finally:
            await records.aclose()

    async def get_records(
            self, *args: t.Any, **kwargs: t.Any) -> ca.AsyncIterable[Record]:
        """
        Asynchronously iterate over a set of records.

        Takes either a single :py:class:`IndexSpec <Indexes.IndexSpec>`
        instance or args and kwargs to construct one.

        Asynchronously iterates over the set of records specified via ``spec``.
        Records are taken from fast storage if the index covers the requested
        set of records and all of them are valid.

        A record can become invalid if it is marked as such by a call to
        :py:meth:`invalidate_record`, or if any record (no matter which one) is
        marked as invalid without providing scores for the index that is used
        to query here.

        Otherwise, records are taken from the (relatively slower) DB. This
        implies that querying a set of records that isn't covered (even if just
        by a little bit) is expensive.

        :return: The requested records as an asynchronous iterator.
        :raise ValueError: If the requested index doesn't support
            :py:meth:`covers <Indexes.covers>`.
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
            records, read_type = (
                await self._check_and_get_records_from_storage(index_spec))
        else:
            read_type = 'cache_miss'
            db_records_spec = self._indexes.db_records_spec(
                index_spec)
            records = self._db_access.get_records(db_records_spec)
        self._metric_reads.labels(
            table_name=self._storage_table.name, type=read_type).inc()
        try:
            async for record in records:
                yield record
        finally:
            await records.aclose()

    async def _check_and_get_records_from_storage(self, index_spec):
        read_type = 'cache_hit'
        records_spec = self._indexes.storage_records_spec(index_spec)
        if not self._intervals_are_valid(records_spec):
            read_type = 'cache_hit_with_refresh'
            await self.refresh_invalid()
        return self._storage_table.get_records(records_spec), read_type

    def _intervals_are_valid(self, records_spec):
        for interval in records_spec.score_intervals:
            if self._invalid_record_repo.interval_intersects_invalid(
                    records_spec.index_name, interval):
                return False
        return True

    def invalidate_records(
            self,
            old_index_specs: list[index.Indexes[Record, PrimaryKey].IndexSpec],
            new_index_specs: list[index.Indexes[Record, PrimaryKey].IndexSpec],
            *, force_refresh_on_next_read: bool = True
    ) -> None:
        """
        Mark records in storage as invalid.

        All records that are currently in storage and match any index spec in
        ``old_index_specs`` or ``new_index_specs`` are marked as invalid. This
        stores the information necessary to do a refresh (i.e. fetch from the
        DB) of these records. If force_refresh_on_next_read is True, any future
        request for any of these records is guaranteed to trigger a refresh
        first. This guarantee holds for read operations that start after this
        method returns. Reads that have already started (in a different task)
        may respect invalidations that happen here, but probably won't. It is
        valid to specify records that haven't actually changed (they will be
        refreshed as well, though).

        During a refresh, all records matching the first index spec in
        ``old_index_specs`` are deleted, then records are loaded again using
        the first index spec in ``new_index_specs``. It is valid (and perfectly
        reasonable for many setups) if old_index_specs == new_index_specs.

        All index specs in both lists should specify the same set of records,
        only for different indexes. Having the first element in
        ``new_index_specs`` specify a proper superset of records to that in
        ``old_index_specs`` is possible. Some of the new records will simply be
        loaded unnecessarily (but records can't exist twice, since they'e
        unique by their primary key). However, specifying fewer records in
        ``new_index_specs`` will cause records to be lost.

        Each index must only be specified once in each list. All indexes for
        which an index spec is given must support coverage checks and certainly
        cover that index spec (i.e. :py:meth:`covers <Indexes.covers>` returns
        ``True``).

        Not all indexes must be represented in the index spec lists, but those
        that aren't are marked as dirty. Any reads against a dirty index will
        unconditionally cause a refresh (as opposed to indexes that aren't
        dirty, which will only be refreshed if the records queried for have
        been marked as invalid). This is necessary since, without information
        on which records are invalid, we must assume that all of them are.

        This method can be used to load new records, as long as they are
        covered by all given indexes.

        Note: records that are updated or deleted during a refresh are not
        observed in an adjustment (i.e. :py:meth:`Adjustment.observe_expired`,
        :py:meth:`Adjustment.observe_loaded`). If this is needed,
        :py:meth:`adjust` must be used instead.

        :param old_index_specs: Specifications of the sets of records that
            should be invalidated, using their old (i.e. now possibly invalid)
            scores. Must not be empty, and may contain a specification for any
            available index.
        :param new_index_specs: Like ``old_index_specs``, but specifying the
            same records by their new (possibly updated) scores.
        :param force_refresh_on_next_read: Whether to do an automatic refresh
            before the next read for any of the invalidated records. The
            refresh is executed lazily when the read arrives, not immediately.
            If False, the invalid records will continue to be served from
            storage. A manual refresh must be performed (using
            :py:meth:`refresh_invalid`).
        :raise ValueError: If the table is not yet loaded, an index is
            specified more than once, or one of the index specs lists is empty.
        """
        if not self._loaded_event.is_set():
            raise ValueError('Table is not yet loaded.')
        old_index_for_refresh, old_specs_by_name = (
            self._parse_index_specs_for_invalidation(old_index_specs))
        new_index_for_refresh, new_specs_by_name = (
            self._parse_index_specs_for_invalidation(new_index_specs))
        if not old_index_for_refresh or not new_index_for_refresh:
            raise ValueError(
                'At least one old and one new index spec must be given.')
        self._invalid_record_repo.flag_invalid(
            old_specs_by_name, new_specs_by_name, old_index_for_refresh,
            new_index_for_refresh,
            consider_in_intersects_check=force_refresh_on_next_read)

    def _parse_index_specs_for_invalidation(self, index_specs):
        first_index_name = None
        specs_by_name = {}
        for index_spec in index_specs:
            if index_spec.index_name in specs_by_name:
                raise ValueError(
                    f'Index {index_spec.index_name} specified more than once.')
            try:
                if not self._indexes.covers(index_spec):
                    raise ValueError(
                        f'Index spec {index_spec} is not covered by the '
                        'indexes.')
            except index.UnsupportedIndexOperation as e:
                raise ValueError(
                    f'Index spec {index_spec} is not covered by the '
                    'indexes (doesn\'t support coverage check).') from e
            first_index_name = first_index_name or index_spec.index_name
            specs_by_name[index_spec.index_name] = index_spec
        return first_index_name, specs_by_name

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
        self._logger.info(
            f'Refreshing {len(self._invalid_record_repo)} invalid index '
            'specs.')
        num_deleted = num_loaded = 0
        old_and_new_specs = self._invalid_record_repo.specs_for_refresh()
        for old_spec, new_spec in old_and_new_specs:
            async for _ in self._storage_table.scratch_discard_records(
                    self._indexes.storage_records_spec(old_spec)):
                num_deleted += 1
            db_records_spec = self._indexes.db_records_spec(new_spec)
            async for record in self._db_access.get_records(db_records_spec):
                await self._storage_table.scratch_put_record(record)
                num_loaded += 1
        self._storage_table.scratch_merge()
        self._invalid_record_repo.clear()
        self._logger.info(
            f'Refresh done. Deleted {num_deleted} and loaded {num_loaded} '
            f'records across {len(old_and_new_specs)} index spec pairs.')
        self._metric_refreshes.inc()


class InvalidRecordRepository[Record, PrimaryKey: tp.PrimaryKey]:
    """
    A repository of invalid records.

    Keeps track of intervals of records scores which have been marked as
    invalid, along with index specs to do the eventual refresh with.
    """

    def __init__(self, indexes: index.Indexes[Record, PrimaryKey]) -> None:
        self._indexes = indexes
        self.clear()

    def __len__(self) -> int:
        return len(self._specs_for_refresh)

    def flag_invalid(
        self,
        old_index_specs:
            t.Mapping[str, index.Indexes[Record, PrimaryKey].IndexSpec],
        new_index_specs:
            t.Mapping[str, index.Indexes[Record, PrimaryKey].IndexSpec],
        old_index_for_refresh: str, new_index_for_refresh: str,
        consider_in_intersects_check: bool
    ) -> None:
        """
        Flag records as invalid.

        Takes 2 dictionaries of index specs, one specifying the invalid records
        as they exist in storage now, the other specifying the updated records.
        These may be the same. Each dictionary maps index names to
        corresponding index specs. Not every index has to be present in the
        specs, but missing ones may be marked as dirty. Any keys in the
        dictionaries that aren't names of indexes are ignored.

        One old and one new index spec is stored to be part of
        specs_for_refresh(). Which are chosen must be specified via
        {old,new}_index_for_refresh. These must be keys into the respective
        dictionaries.

        If consider_in_intersects_check is True, all score intervals in the
        corresponding StorageRecordsSpecs will be considered invalid in
        interval_intersects_invalid(). Additionally, any indexes that are
        missing are marked as dirty, i.e. future calls to
        interval_intersects_invalid() for that index will always return True,
        since there is no longer a way to know what scores are valid.

        :param consider_in_intersects_check: Whether to consider the records
            invalid in interval_intersects_invalid().
        """
        if not old_index_specs or not new_index_specs:
            raise ValueError(
                'Must provide at least one old and one new index spec.')
        self._specs_for_refresh.append(
            (old_index_specs[old_index_for_refresh],
             new_index_specs[new_index_for_refresh]))
        if consider_in_intersects_check:
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

    def specs_for_refresh(self) -> list[tuple[
            index.Indexes[Record, PrimaryKey].IndexSpec,
            index.Indexes[Record, PrimaryKey].IndexSpec]]:
        """
        Get index specs to do a refresh with.

        Returns a list of tuples (old, new), where old is an index spec of
        records that have become invalid in storage, and new is an index spec
        specifying the records in their valid state in the DB.
        """
        return list(self._specs_for_refresh)

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
