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

import abc

import tablecache.index as index
import tablecache.types as tp


class StorageTable[PrimaryKey](abc.ABC):
    @property
    @abc.abstractmethod
    def table_name(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def clear(self) -> None:
        """Delete all data belonging to this table."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_record(self, record: tp.Record) -> None:
        """
        Store a record.

        May raise an exception if the record is invalid in some way.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Retrieve a previously stored record by primary key.

        Raises a KeyError if no record with that primary key exists.

        May raise other exceptions if there is a problem in retrieving the
        record.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_records(
            self, records_spec: index.StorageRecordsSpec) -> tp.AsyncRecords:
        """
        Get multiple records.

        Asynchronously iterates over all records that match the recods spec.
        That's all records that have a score in the specified index that is
        contained in one of the specified intervals, and additionally match the
        recheck predicate.

        Records are guaranteed to be unique as long as the record_spec's
        intervals don't overlap (as per their contract).

        No particular order is guaranteed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        """Delete a record by primary key."""
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_records(
            self, records_spec: index.StorageRecordsSpec) -> int:
        """
        Delete multiple records.

        Deletes exactly those records that would have been returned by
        get_records() when called with the same argument.

        Returns the number of records deleted.
        """
        raise NotImplementedError
