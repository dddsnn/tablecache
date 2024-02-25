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
import itertools as it
import operator as op
import typing as t
import uuid

import aiorwlock
import sortedcontainers

import tablecache.index as index
import tablecache.storage as storage
import tablecache.types as tp


def _always_true(*args, **kwargs): return True


class LocalStorageTable[PrimaryKey: tp.PrimaryKey](
        storage.StorageTable[PrimaryKey]):
    """
    A StorageTable that stores its data in native Python data structures.

    This implementation of StorageTable uses SortedKeyLists from the
    sortedcontainers library to enable fast access to records via their scores.
    Using native data structures has the advantage that each index can store
    direct references to all records, so there is no additional redirection
    necessary when getting records via non-primary-key indexes.

    Records inserted into the table are stored as-is, without any explicit
    validation. As long as it's possible to calculate their scores using the
    provided function, they are accepted. It is up to the user to ensure that
    records are complete.

    Read operations return the exact same record instances that were inserted.
    In case they are mutable, they must not be modified. Make a copy. More
    specifically, if a record is modified in a way that changes its score for
    any index, that index becomes inconsistent and the record may not be
    returned in a read operation when it should be.

    Regular write operations (put_record(), delete_record{,s}()) are blocked
    while scratch space is active (i.e. between the first call to
    scratch_put_record() or scratch_discard_records() and the subsequent call
    to scratch_merge()). They will resume once the merge completes. The merge
    is done in a background task, which shuffles around some data. While this
    task runs, scratch operations are blocked. Read operations
    (get_record{,s}()) are never blocked entirely, although get_records() may
    need to wait a bit if a merge operation is in progress.
    """

    def __init__(
            self, *, record_scorer: index.RecordScorer[PrimaryKey],
            table_name: str = None) -> None:
        """
        :param record_scorer: A RecordScorer used to calculate a record's
            scores for all the indexes that need to be represented in storage.
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
        self._reset_record_storage()

    def __repr__(self) -> str:
        return f'Local table {self._table_name}'

    def _reset_record_storage(self):
        self._indexes = self._make_index_dict()
        self._scratch_indexes = self._make_index_dict()
        self._scratch_records_to_delete = {}

    def _make_index_dict(self):
        return {
            index_name: sortedcontainers.SortedKeyList(key=op.itemgetter(0))
            for index_name in self._record_scorer.index_names}

    @t.override
    async def clear(self) -> None:
        self._reset_record_storage()

    @t.override
    async def put_record(self, record: tp.Record) -> None:
        """
        Store a record.

        This operation will block while scratch space is active and resume
        after the scratch merge finishes.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(self._scratch_is_clear)
            self._put_record(record)

    def _put_record(self, record):
        try:
            self._delete_record_by_primary_key(
                self._record_scorer.primary_key(record))
        except KeyError:
            pass
        self._for_each_index(
            record, lambda index_name, score:
            self._indexes[index_name].add((score, record)))

    def _for_each_index(self, record, function):
        for index_name in self._record_scorer.index_names:
            score = self._record_scorer.score(index_name, record)
            function(index_name, score)

    @t.override
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        return self._get_record(primary_key)

    def _get_record(self, primary_key):
        if self._include_scratch_records:
            if primary_key in self._scratch_records_to_delete:
                raise KeyError
            indexes_to_try = [self._scratch_indexes, self._indexes]
        else:
            indexes_to_try = [self._indexes]
        records_spec = self._records_spec_for_only_primary_key(primary_key)
        records = it.chain(
            *[self._get_records_from_indexes(records_spec, indexes)
              for indexes in indexes_to_try])
        try:
            return next(records)
        except StopIteration:
            raise KeyError

    def _records_spec_for_only_primary_key(self, primary_key):
        primary_key_score = self._record_scorer.primary_key_score(primary_key)
        return storage.StorageRecordsSpec(
            'primary_key',
            [storage.Interval.only_containing(primary_key_score)],
            recheck_predicate=self._record_primary_key_equals(primary_key))

    def _record_primary_key_equals(self, primary_key):
        def checker(record):
            return self._record_scorer.primary_key(record) == primary_key
        return checker

    @t.override
    async def get_records(
            self, records_spec: storage.StorageRecordsSpec) -> tp.AsyncRecords:
        async with self._scratch_merge_read_lock.reader_lock:
            async for record in self._get_records_locked(records_spec):
                yield record

    async def _get_records_locked(self, records_spec):
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

    def _record_is_not_deleted_and_not_previously_returned(self):
        already_returned = set()

        def checker(record):
            primary_key = self._record_scorer.primary_key(record)
            is_ok = (primary_key not in self._scratch_records_to_delete and
                     primary_key not in already_returned)
            already_returned.add(primary_key)
            return is_ok
        return checker

    def _get_records_from_indexes(self, records_spec, indexes):
        for interval in records_spec.score_intervals:
            for _, record in indexes[records_spec.index_name].irange_key(
                    interval.ge, interval.lt, inclusive=(True, False)):
                if records_spec.recheck_predicate(record):
                    yield record

    @t.override
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        """
        Delete a record by primary key.

        This operation will block while scratch space is active and resume
        after the scratch merge finishes.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(self._scratch_is_clear)
            self._delete_record_by_primary_key(primary_key)

    def _delete_record_by_primary_key(self, primary_key):
        record = self._get_record(primary_key)
        self._delete_record_from_indexes(record, self._indexes)

    def _delete_record_from_indexes(self, record, indexes):
        self._for_each_index(
            record, lambda index_name, score:
            indexes[index_name].discard((score, record)))

    @t.override
    async def delete_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        """
        Delete multiple records.

        This operation will block while scratch space is active and resume
        after the scratch merge finishes.

        Returns the number of records deleted.

        Internally, first finds all records matching the records spec, then
        deletes them. If another task adds a record after that first step, this
        record will not be deleted by this operation. Similarly, if another
        task deletes one of the records after that first step, this operation
        will attempt to delete it again. This won't fail, but it will inflate
        the number of records that is returned.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(self._scratch_is_clear)
            records_to_delete = [
                r async for r in self.get_records(records_spec)]
            for record in records_to_delete:
                self._delete_record_from_indexes(record, self._indexes)
                await asyncio.sleep(0)  # Yield to event loop to remain lively.
            return len(records_to_delete)

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
    async def scratch_put_record(self, record: tp.Record) -> None:
        """
        Add a record to scratch space.

        This operation will block while a merge background task is running.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_is_not_merging)
            await self._scratch_put_record_locked(record)

    async def _scratch_put_record_locked(self, record):
        self._scratch_records_to_delete.pop(
            self._record_scorer.primary_key(record), None)
        self._for_each_index(
            record, lambda index_name, score:
            self._scratch_indexes[index_name].add((score, record)))

    @t.override
    async def scratch_discard_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        """
        Mark a set of records to be deleted in scratch space.

        This operation will block while a merge background task is running.

        Returns the number of records marked for deletion. This may include
        records that have already been thusly marked before.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_is_not_merging)
            return await self._scratch_discard_records_locked(records_spec)

    async def _scratch_discard_records_locked(self, records_spec):
        num_discarded = 0
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
            num_discarded += 1
        return num_discarded

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
        async with self._scratch_merge_read_lock.writer_lock:
            while self._scratch_records_to_delete:
                _, record = self._scratch_records_to_delete.popitem()
                self._delete_record_from_indexes(record, self._indexes)
                await asyncio.sleep(0)  # Yield to event loop to remain lively.
            while self._scratch_indexes['primary_key']:
                _, record = self._scratch_indexes['primary_key'].pop()
                self._put_record(record)
                self._delete_record_from_indexes(record, self._scratch_indexes)
                await asyncio.sleep(0)  # Yield to event loop to remain lively.
            assert not any(self._scratch_indexes.values())
            assert not self._scratch_records_to_delete
            self._scratch_merge_task = None
            async with self._scratch_condition:
                self._scratch_condition.notify_all()
