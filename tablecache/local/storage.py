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

import operator as op
import typing as t

import sortedcontainers

import tablecache.index as index
import tablecache.storage as storage
import tablecache.types as tp


class LocalStorageTable[PrimaryKey](storage.StorageTable[PrimaryKey]):
    def __init__(
            self, *, table_name: str, primary_key_name: str,
            indexes: index.Indexes[PrimaryKey]) -> None:
        self._table_name = table_name
        self._primary_key_name = primary_key_name
        self._index_names = indexes.index_names
        self._score_function = indexes.score
        self._primary_key_score_function = indexes.primary_key_score
        self._indexes = self._make_index_dict()
        self._scratch_indexes = self._make_index_dict()
        self._scratch_records_to_delete = {}

    def __repr__(self) -> str:
        return f'Local table {self._table_name}'

    def _make_index_dict(self):
        return {index_name: sortedcontainers.SortedKeyList(
            key=op.itemgetter(0)) for index_name in self._index_names}

    @t.override
    async def clear(self) -> None:
        self._indexes = self._make_index_dict()
        self._scratch_indexes = self._make_index_dict()
        self._scratch_records_to_delete = {}

    @t.override
    async def put_record(self, record: tp.Record) -> None:
        self._put_record(record)

    def _put_record(self, record):
        if self._primary_key_name not in record:
            raise ValueError('Missing primary key.')
        primary_key = record[self._primary_key_name]
        try:
            self._delete_record(primary_key)
        except KeyError:
            pass
        for index_name in self._index_names:
            score = self._score_function(index_name, record)
            self._indexes[index_name].add((score, record))

    @t.override
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        return self._get_record(primary_key)

    def _get_record(self, primary_key):
        primary_key_score = self._primary_key_score_function(primary_key)
        records = self._get_records(
            storage.StorageRecordsSpec(
                'primary_key',
                [storage.Interval.only_containing(primary_key_score)],
                recheck_predicate=lambda r: r[self._primary_key_name] == primary_key), self._indexes)
        try:
            return next(records)
        except StopIteration:
            raise KeyError

    @t.override
    async def get_records(
            self, records_spec: storage.StorageRecordsSpec) -> tp.AsyncRecords:
        for record in self._get_records(records_spec, self._indexes):
            yield record

    def _get_records(self, records_spec, indexes):
        for interval in records_spec.score_intervals:
            for _, record in indexes[records_spec.index_name].irange_key(
                    interval.ge, interval.lt, inclusive=(True, False)):
                if records_spec.recheck_predicate(record):
                    yield record

    @t.override
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        self._delete_record(primary_key)

    def _delete_record(self, primary_key):
        record = self._get_record(primary_key)
        for index_name in self._index_names:
            score = self._score_function(index_name, record)
            self._indexes[index_name].discard((score, record))

    @t.override
    async def delete_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        records_to_delete = [r async for r in self.get_records(records_spec)]
        for record in records_to_delete:
            self._delete_record(record[self._primary_key_name])
        return len(records_to_delete)

    @t.override
    async def scratch_put_record(self, record: tp.Record) -> None:
        if self._primary_key_name not in record:
            raise ValueError('Missing primary key.')
        primary_key = record[self._primary_key_name]
        self._scratch_records_to_delete.pop(primary_key, None)
        for index_name in self._index_names:
            score = self._score_function(index_name, record)
            self._scratch_indexes[index_name].add((score, record))

    @t.override
    async def scratch_discard_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        num_discarded = 0
        records_to_delete = [r for r in self._get_records(
            records_spec, self._scratch_indexes)]
        for record in records_to_delete:
            for index_name in self._index_names:
                score = self._score_function(index_name, record)
                self._scratch_indexes[index_name].discard((score, record))
        async for record in self.get_records(records_spec):
            primary_key = record[self._primary_key_name]
            self._scratch_records_to_delete[primary_key] = record
            num_discarded += 1
        return num_discarded

    @t.override
    def scratch_merge(self) -> None:
        for record in self._scratch_records_to_delete.values():
            for index_name in self._index_names:
                score = self._score_function(index_name, record)
                self._indexes[index_name].discard((score, record))
        for _, record in self._scratch_indexes['primary_key']:
            self._put_record(record)
        self._scratch_indexes = self._make_index_dict()
        self._scratch_records_to_delete = {}
