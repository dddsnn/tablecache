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

import abc
import collections.abc as ca
import dataclasses as dc
import itertools as it
import numbers
import operator as op

import tablecache.types as tp


@dc.dataclass(frozen=True)
class Interval:
    """
    A number interval.

    Represents an interval of the shape [ge,lt[, i.e. with a closed lower and
    open upper bound.
    """
    ge: numbers.Real
    lt: numbers.Real

    def __contains__(self, x):
        return self.ge <= x < self.lt


@dc.dataclass(frozen=True)
class StorageRecordsSpec:
    """
    A specification of records in storage.

    Represents a (possibly empty) set of records in a storage table. These are
    all those which have an index score in the index with the given name which
    is contained in any of the given intervals.

    Additionally, the record must satifsy the recheck predicate, i.e. it must
    return True when called with the record. The default recheck predicate
    accepts any record (i.e. only the index score is important). This predicate
    can be used to query the storage for a range of records that may contain
    some undesirable ones, and then filtering those out.

    The score intervals must not overlap.
    """
    @staticmethod
    def _always_use_record(_):
        return True

    def __post_init__(self):
        for left, right in it.pairwise(
                sorted(self.score_intervals, key=op.attrgetter('ge'))):
            if left.lt > right.ge:
                raise ValueError('Intervals overlap.')

    index_name: str
    score_intervals: list[Interval]
    recheck_predicate: ca.Callable[[tp.Record], bool] = _always_use_record


class StorageTable[PrimaryKey](abc.ABC):
    """
    Fast storage table.

    Abstract interface for fast record storage. Offers methods to put records,
    get and delete records by primary key, as well as to get and delete
    multiple records that match score ranges. Each record is associated with
    one or more scores by which it can be queried. Implementations are expected
    to use a sorted data structure that enables fast access via those scores.

    Also offers a scratch space, where records can marked to be added or
    deleted without affecting reads on the table until they are explicitly
    merged. This is meant to provide a consitent view of the data while
    (potentially slow) updates of the data are going on in the background. The
    implementation of the merge operation is expected to be relatively fast so
    that updates provide little disruption.
    """
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
            self, records_spec: StorageRecordsSpec) -> tp.AsyncRecords:
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
    async def delete_records(self, records_spec: StorageRecordsSpec) -> int:
        """
        Delete multiple records.

        Deletes exactly those records that would have been returned by
        get_records() when called with the same argument.

        Returns the number of records deleted.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def scratch_put_record(self, record: tp.Record) -> None:
        """
        Add a record to scratch space.

        Records in scratch space have no effect on get operations until they
        are merged via scratch_merge().
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def scratch_discard_record(self, primary_key: PrimaryKey) -> None:
        """
        Mark a record to be deleted in scratch space.

        Records marked for deletion have no effect on get operations until they
        are merged via scratch_merge().

        This can be undone by adding the record again via scratch_put_record().

        In contrast to delete_record(), does not raise an exception if no
        record with the given primary key exists.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def scratch_merge(self) -> None:
        """
        Merge scratch space.

        Merge records added to scratch space via scratch_put_record() or marked
        for deletion via scratch_discard_record() so that these changes are
        reflected in get_record() and get_records().

        This method is not async, as the switchover is meant to be fast.
        However, implementations may start background tasks to handle some
        cleanup during which further scratch operations are blocked.
        """
        raise NotImplementedError
