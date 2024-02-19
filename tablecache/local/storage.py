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

import typing as t

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
        self._records = {}
        self._indexes = {}
        self._scratch_records = {}
        self._scratch_pks_delete = set()

    def __repr__(self) -> str:
        return f'Local table {self._table_name}'

    @t.override
    async def clear(self) -> None:
        self._records = {}
        self._indexes = {}

    @t.override
    async def put_record(self, record: tp.Record) -> None:
        if self._primary_key_name not in record:
            raise ValueError('Missing primary key.')
        primary_key = record[self._primary_key_name]
        self._records[primary_key] = record
        for index_name in self._index_names:
            score = self._score_function(index_name, record)
            self._indexes.setdefault(index_name, {}).setdefault(
                score, set()).add(primary_key)

    @t.override
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        return self._make_record(self._records[primary_key])

    @t.override
    async def get_records(
            self, records_spec: storage.StorageRecordsSpec) -> tp.AsyncRecords:
        for record in self._records.values():
            if not records_spec.recheck_predicate(record):
                continue
            for interval in records_spec.score_intervals:
                score = self._score_function(records_spec.index_name, record)
                if score in interval:
                    yield self._make_record(record)
                    break

    def _make_record(self, record):
        return record | {'source': 'storage'}

    @t.override
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        del self._records[primary_key]
        self._delete_index_entries(primary_key)

    def _delete_index_entries(self, primary_key):
        for index in self._indexes.values():
            for score, primary_keys in list(index.items()):
                primary_keys.discard(primary_key)
                if not primary_keys:
                    del index[score]

    @t.override
    async def delete_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        delete = [r async for r in self.get_records(records_spec)]
        for record in delete:
            await self.delete_record(record[self._primary_key_name])
        return len(delete)

    @t.override
    async def scratch_put_record(self, record: tp.Record) -> None:
        primary_key = record[self._primary_key_name]
        self._scratch_records[primary_key] = self._make_record(record)
        self._scratch_pks_delete.discard(primary_key)

    @t.override
    async def scratch_discard_records(
            self, records_spec: storage.StorageRecordsSpec) -> int:
        num_discarded = 0
        async for record in self.get_records(records_spec):
            primary_key = record[self._primary_key_name]
            self._scratch_records.pop(primary_key, None)
            self._scratch_pks_delete.add(primary_key)
            num_discarded += 1
        return num_discarded

    @t.override
    def scratch_merge(self) -> None:
        for primary_key in self._scratch_pks_delete:
            self._records.pop(primary_key, None)
        self._records.update(self._scratch_records)
        self._scratch_pks_delete.clear()
        self._scratch_records.clear()
