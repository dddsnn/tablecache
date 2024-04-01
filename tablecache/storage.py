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
The :py:class:`StorageTable` is the abstract base for access to a records
storage. Storage always means a place to put cached records from which they can
be fetched again quickly.

Performance is achieved by associating each record with one or more scores, and
having the :py:class:`StorageTable` be able to quickly fetch records by a range
of scores. The :py:class:`Interval` defines such a range, and the
:py:class:`StorageRecordsSpec` specifies a set of records in storage via any
number of them. Additionally, it has a :py:attr:`recheck_predicate`, which can
be used to filter out records that aren't wanted.

Each :py:class:`StorageTable` provides a scratch space, which is a place to
stage write operations that shouldn't take effect immediately. This is used by
the :py:class:`.CachedTable` during adjustments (i.e. when expiring old and
loading new records). The :py:class:`.CachedTable` is meant to provide a
consistent view of the records, and locking everything isn't an option since
adjustments may take a long time. The scratch space allows it to get all
changes ready without affecting reads. Then, the prepared scratch space can be
merged, which should be implemented to be very fast.
"""

import abc
import collections.abc as ca
import dataclasses as dc
import itertools as it
import math
import operator as op
import typing as t

import tablecache.types as tp


@dc.dataclass(frozen=True)
class Interval:
    """
    A number interval.

    Represents an interval of the shape ``[ge,lt)``, i.e. with a closed lower
    and open upper bound.
    """
    ge: tp.Score
    lt: tp.Score

    def __post_init__(self):
        if self.ge > self.lt:
            raise ValueError('Bounds are not in order.')

    def __repr__(self) -> str:
        return f'Interval [{self.ge}, {self.lt})'

    @staticmethod
    def everything() -> t.Self:
        """
        The interval from negative to positive infinity, covering everything.
        """
        return Interval(float('-inf'), float('inf'))

    @staticmethod
    def only_containing(value) -> t.Self:
        """
        The smallest interval containing the given value.
        """
        return Interval(value, math.nextafter(value, float('inf')))

    def __contains__(self, x: tp.Score) -> bool:
        return self.ge <= x < self.lt

    def intersects(self, other: t.Self) -> bool:
        """Check whether the intervals have any element in common."""
        if not isinstance(other, Interval):
            raise TypeError(
                'Can only check intersection with other intervals.')
        return self.lt > other.ge and self.ge < other.lt

    def covers(self, other: t.Self) -> bool:
        """Check whether this interval contains everything in other."""
        if not isinstance(other, Interval):
            raise TypeError(
                'Can only check covers with other intervals.')
        other_is_empty = other.ge == other.lt
        return other_is_empty or (self.ge <= other.ge and other.lt <= self.lt)


@dc.dataclass(frozen=True)
class StorageRecordsSpec[Record]:
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
    def always_use_record(_):
        return True

    def __post_init__(self):
        for left, right in it.pairwise(
                sorted(self.score_intervals, key=op.attrgetter('ge'))):
            if left.lt > right.ge:
                raise ValueError('Intervals overlap.')

    def __repr__(self) -> str:
        return (
            f'records with {self.index_name} scores in {self.score_intervals} '
            f'matching {self.recheck_predicate.__name__}')

    index_name: str
    score_intervals: list[Interval]
    recheck_predicate: tp.RecheckPredicate[Record] = always_use_record


class StorageTable[Record](abc.ABC):
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

    The behavior of the regular write operations (:py:meth:`put_record` and
    :py:meth:`delete_records`) is not necessarily well-defined when they occur
    concurrently (i.e. from separate tasks). When in doubt, locking should be
    used, or the scratch space, which is guaranteed to behave in the presence
    of multiple tasks.
    """
    @property
    @abc.abstractmethod
    def name(self) -> str:
        """A name for the table."""

    @abc.abstractmethod
    async def clear(self) -> None:
        """Delete all data belonging to this table."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_record(self, record: Record) -> None:
        """
        Store a record.

        If a record with the same primary key already exists, it is replaced.

        :param record: The record to add.
        :raise: If the record is invalid in some way.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_records(
            self, records_spec: StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Get multiple records.

        Asynchronously iterates over all records that match the records spec.
        That's all records that have a score in the specified index that is
        contained in one of the specified intervals, and additionally match the
        recheck predicate.

        Records are guaranteed to be unique as long as the record spec's
        intervals don't overlap (as per their contract).

        :param records_spec: A specification of the records to get.
        :return: The requested records as an asynchronous iterator, in no
            particular order.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_records(
            self, records_spec: StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Delete multiple records.

        Deletes exactly those records that would have been returned by
        :py:meth:`get_records` when called with the same argument.

        Asynchronously iterates over the records that are deleted as they exist
        in storage. Must be fully consumed to finish deletion.

        :param records_spec: A specification of the records to delete.
        :return: The records as they are deleted as an asynchronous iterator,
            in no particular order.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def scratch_put_record(self, record: Record) -> None:
        """
        Add a record to scratch space.

        Records in scratch space have no effect on get operations until they
        are merged via :py:meth:`scratch_merge`.

        :param record: The record to add to scratch space.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def scratch_discard_records(
            self, records_spec: StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Mark a set of records to be deleted in scratch space.

        Records marked for deletion have no effect on get operations until they
        are merged via :py:meth:`scratch_merge`.

        This can be undone by adding the record again via
        :py:meth:`scratch_put_record`.

        Asynchronously iterates over the records that are marked for discarding
        as they exist in storage. These records will continue to be available
        until scratch space is merged. Must be fully consumed to finish the
        operation.

        :param records_spec: A specification of the records to mark for
            discarding.
        :return: The records marked for discarding as an asynchronous iterator,
            in no particular order.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def scratch_merge(self) -> None:
        """
        Merge scratch space.

        Merge records added to scratch space via :py:meth:`scratch_put_record`
        or marked for deletion via :py:meth:`scratch_discard_records` so that
        these changes are reflected in :py:meth:`get_record` and
        :py:meth:`get_records`.

        This method is not async, as the switchover is meant to be fast.
        However, implementations may start background tasks to handle some
        cleanup during which further scratch operations are blocked.
        """
        raise NotImplementedError
