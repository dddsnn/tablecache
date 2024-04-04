# Copyright 2024 Marc Lehmann

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
import collections.abc as ca
import itertools as it
import operator as op
import typing as t
import uuid

import aiorwlock
import sortedcontainers

import tablecache.index as index
import tablecache.metrics as metrics
import tablecache.storage as storage
import tablecache.types as tp


def _always_true(*args, **kwargs): return True


class LocalStorageTable[Record, PrimaryKey: tp.PrimaryKey](
        storage.StorageTable[Record]):
    """
    A StorageTable that stores its data in native Python data structures.

    This implementation of :py:class:`.StorageTable` uses
    :external:py:class:`sortedcontainers.SortedKeyList` s to enable fast access
    to records via their scores. Using native data structures has the advantage
    that each index can store direct references to all records, so there is no
    additional redirection necessary when getting records via indexes.

    Records inserted into the table are stored as-is, without any explicit
    validation. As long as it's possible to calculate their scores and extract
    a primary key using the record scorer, they are accepted. It is up to the
    user to ensure that records are complete.

    Read operations return the exact same record instances that were inserted.
    In case they are mutable, they must not be modified while they reside in
    storage. Make a copy. More specifically, if a record is modified in a way
    that changes its score for any index, that index becomes inconsistent and
    the record may not be returned in a read operation when it should be.

    Regular write operations (:py:meth:`put_record`, :py:meth:`delete_records`)
    are blocked while scratch space is active (i.e. between the first call to
    :py:meth:`scratch_put_record` or :py:meth:`scratch_discard_records` and the
    subsequent call to :py:meth:`scratch_merge`). They will resume once the
    merge completes. The merge is done in a background task, which shuffles
    some data around. While this task runs, scratch operations are blocked.

    Read operations are generally prioritized over write operations.
    :py:meth:`get_records` is never blocked entirely, although it may need to
    wait momentarily to take away a lock from an ongoing merge operation.
    Asynchronous write operations that can take a while (the scratch merge task
    in particular) regularly yield back to the event loop when it is safe, to
    allow read operations to jump in.
    """

    def __init__(
            self, record_scorer: index.RecordScorer[Record, PrimaryKey], *,
            table_name: str = None) -> None:
        """
        :param record_scorer: A RecordScorer used to calculate a record's
            scores for all the indexes that need to be represented in storage.
            The score function must not raise exceptions, or the storage may be
            left in an undefined state.
        :param table_name: Name of the table. Only informational. If not given,
            a random UUID string is generated.
        """
        self._record_scorer = record_scorer
        self._table_name = table_name
        if self._table_name is None:
            self._table_name = str(uuid.uuid4())
        self._scratch_condition = asyncio.Condition()
        self._scratch_merge_task = None
        self._scratch_merge_read_lock = aiorwlock.RWLock()
        self._metric_records = metrics.get_gauge(
            'tablecache_local_table_records_total',
            'Number of records currently in the table',
            ['table_name', 'type'])
        self._reset_record_storage()

    def __repr__(self) -> str:
        return f'Local table {self.name} ({len(self._records)} records)'

    def _reset_record_storage(self):
        self._records = {}
        self._scratch_records = {}
        self._indexes = self._make_index_dict()
        self._scratch_indexes = self._make_index_dict()
        self._scratch_records_to_delete = {}
        self._set_records_metrics()

    def _set_records_metrics(self):
        self._metric_records.labels(
            table_name=self.name, type='regular').set(len(self._records))
        self._metric_records.labels(
            table_name=self.name, type='scratch').set(
                len(self._scratch_records))
        self._metric_records.labels(
            table_name=self.name, type='scratch_delete').set(
                len(self._scratch_records_to_delete))

    def _make_index_dict(self):
        return {
            index_name: sortedcontainers.SortedKeyList(key=op.itemgetter(0))
            for index_name in self._record_scorer.index_names}

    @t.override
    @property
    def name(self) -> str:
        return self._table_name

    @t.override
    async def clear(self) -> None:
        self._reset_record_storage()

    @t.override
    async def put_record(self, record: Record) -> None:
        """
        Store a record.

        This operation will block while scratch space is active and resume
        after the scratch merge finishes.

        :param record: The record to add.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(self._scratch_is_clear)
            self._put_record_to_dicts(record, self._records, self._indexes)
        self._set_records_metrics()

    def _put_record_to_dicts(self, record, records, indexes):
        primary_key = self._record_scorer.primary_key(record)
        try:
            existing_record = records.pop(primary_key)
            self._discard_record_from_dicts(
                existing_record, records, indexes, primary_key)
        except KeyError:
            pass
        records[primary_key] = record
        for index_name in self._record_scorer.index_names:
            score = self._record_scorer.score(index_name, record)
            indexes[index_name].add((score, record))
        return primary_key

    def _discard_record_from_dicts(
            self, record, records, indexes, primary_key=None):
        if primary_key is None:
            primary_key = self._record_scorer.primary_key(record)
        records.pop(primary_key, None)
        for index_name in self._record_scorer.index_names:
            score = self._record_scorer.score(index_name, record)
            indexes[index_name].discard((score, record))

    @t.override
    async def get_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        async with self._scratch_merge_read_lock.reader_lock:
            for record in self._get_records_locked(records_spec):
                yield record

    def _get_records_locked(self, records_spec):
        if self._include_scratch_records:
            records = it.chain(
                *[self._get_records_from_indexes(records_spec, indexes)
                  for indexes in [self._scratch_indexes, self._indexes]])
            record_is_ok = (
                self._record_is_not_deleted_and_not_previously_returned())
        else:
            records = self._get_records_from_indexes(
                records_spec, self._indexes)
            record_is_ok = _always_true
        for record in records:
            if record_is_ok(record):
                yield record

    def _get_records_from_indexes(self, records_spec, indexes):
        for interval in records_spec.score_intervals:
            for _, record in indexes[records_spec.index_name].irange_key(
                    interval.ge, interval.lt, inclusive=(True, False)):
                if records_spec.recheck_predicate(record):
                    yield record

    def _record_is_not_deleted_and_not_previously_returned(self):
        already_returned = set()

        def checker(record):
            primary_key = self._record_scorer.primary_key(record)
            is_ok = (primary_key not in self._scratch_records_to_delete and
                     primary_key not in already_returned)
            already_returned.add(primary_key)
            return is_ok
        return checker

    @t.override
    async def delete_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Delete multiple records.

        Asynchronously iterates over the records that are deleted as they exist
        in storage. Must be fully consumed to finish deletion.

        This operation will block while scratch space is active and resume
        after the scratch merge finishes.

        Internally, first finds all records matching ``records_spec``, then
        deletes them. If another task adds a record after that first step, this
        record will not be deleted by this operation. Similarly, if another
        task deletes one of the records after that first step, this operation
        will attempt to delete it again. This won't fail, but it will inflate
        the number of records that is returned.

        :param records_spec: A specification of the records to delete.
        :return: The records as they are deleted as an asynchronous iterator,
            in no particular order.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(self._scratch_is_clear)
            records_to_delete = [
                r async for r in self.get_records(records_spec)]
            for record in records_to_delete:
                self._discard_record_from_dicts(
                    record, self._records, self._indexes)
                yield record
                await asyncio.sleep(0)  # Yield to event loop to remain lively.
        self._set_records_metrics()

    @property
    def _include_scratch_records(self):
        return self._scratch_merge_task is not None

    def _scratch_is_not_merging(self):
        return self._scratch_merge_task is None

    def _scratch_is_clear(self):
        return (self._scratch_is_not_merging() and
                not any(i for i in self._scratch_indexes.values()) and
                not self._scratch_records_to_delete)

    @t.override
    async def scratch_put_record(self, record: Record) -> None:
        """
        Add a record to scratch space.

        This operation will block while a merge background task is running.

        :param record: The record to add to scratch space.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_is_not_merging)
            primary_key = self._put_record_to_dicts(
                record, self._scratch_records, self._scratch_indexes)
            self._scratch_records_to_delete.pop(primary_key, None)
        self._set_records_metrics()

    @t.override
    async def scratch_discard_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Mark a set of records to be deleted in scratch space.

        Asynchronously iterates over the records that are marked for discarding
        as they exist in storage. This may include records that have already
        been marked for discarding. These records will continue to be available
        until scratch space is merged. Must be fully consumed to finish the
        operation.

        This operation will block while a merge background task is running.

        :param records_spec: A specification of the records to mark for
            discarding.
        :return: The records marked for discarding as an asynchronous iterator,
            in no particular order.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_is_not_merging)
            async for record in self._scratch_discard_records_locked(
                    records_spec):
                yield record

    async def _scratch_discard_records_locked(self, records_spec):
        records_to_discard = [
            r for r in self._get_records_from_indexes(
                records_spec, self._scratch_indexes)]
        for record in records_to_discard:
            for index_name in self._record_scorer.index_names:
                score = self._record_scorer.score(index_name, record)
                self._scratch_indexes[index_name].discard((score, record))
            await asyncio.sleep(0)  # Yield to event loop to remain lively.
        async for record in self.get_records(records_spec):
            primary_key = self._record_scorer.primary_key(record)
            self._scratch_records_to_delete[primary_key] = record
            yield record
        self._set_records_metrics()

    @t.override
    def scratch_merge(self) -> None:
        """
        Merge scratch space.

        This immediately causes read operations to reflect the state of the
        table that includes modifications in scratch space.

        Spawns a background task that shuffles some data around. Until this
        completes, all write operations are locked.
        """
        self._scratch_merge_task = asyncio.create_task(self._scratch_merge())

    async def _scratch_merge(self):
        while self._scratch_records_to_delete:
            async with self._scratch_merge_read_lock.writer_lock:
                primary_key, record = self._scratch_records_to_delete.popitem()
                self._discard_record_from_dicts(
                    record, self._records, self._indexes, primary_key)
            await asyncio.sleep(0)  # Yield to event loop to remain lively.
        while self._scratch_records:
            async with self._scratch_merge_read_lock.writer_lock:
                primary_key, record = self._scratch_records.popitem()
                self._put_record_to_dicts(record, self._records, self._indexes)
                self._discard_record_from_dicts(
                    record, self._scratch_records, self._scratch_indexes,
                    primary_key)
            await asyncio.sleep(0)  # Yield to event loop to remain lively.
        assert not any(self._scratch_indexes.values())
        assert not self._scratch_records_to_delete
        self._scratch_merge_task = None
        async with self._scratch_condition:
            self._scratch_condition.notify_all()
        self._set_records_metrics()
