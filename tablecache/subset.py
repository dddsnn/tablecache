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
import collections.abc as ca
import dataclasses as dc
import numbers
import typing as t

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
class Adjustment[Subset]:
    expire_intervals: ca.Iterable[Interval]
    new_subset: Subset


class Subset(abc.ABC):
    """
    Specification of a subset of records in a table.

    Specifies a subset of a table both for storage and DB tables, but doesn't
    store any values. It is used as an argument in querying either storage or
    DB table to transparently specify the same set of records in either.

    For storage tables, which are ordered by a score, the score_intervals
    property is an iterator over Intervals of scores that together contain all
    the relevant records. More precisely, a record with a score s belongs to
    this subset if there is an interval i in score_intervals such that s in i.

    For DB tables, the db_args property defines a tuple of query parameters
    that must match the subset query of the DB table and parameterize it in a
    way such that only those records are returned that match this subset.

    It is up to the implementor to ensure that the score properties and db_args
    match the same records in their respective databases.
    """
    @property
    @abc.abstractmethod
    def score_intervals(self) -> ca.Iterable[Interval]:
        """
        All score intervals representing this subset in the storage table.

        Intervals must not overlap.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def db_args(self) -> tuple:
        """
        Tuple of DB query arguments.

        Must match the query which the DB table uses to fetch a subset of
        records.
        """
        raise NotImplementedError

    def __contains__(self, score: numbers.Real) -> bool:
        """Return whether a score is contained in this subset."""
        return any(score in i for i in self.score_intervals)


class CachedSubset(Subset):
    """
    A subset keeping track of records that have actually been loaded.

    Extends the interface of Subset to be used by a Cache. Provides a method to
    calculate the score of a record and an observe() callback the cache will
    call whenever it adds a record to storage. The subset can be changed via
    adjust() in an implementation-specific way (e.g. expire old values, get new
    ones).
    """
    @classmethod
    @abc.abstractmethod
    def record_score(cls, record: tp.Record) -> numbers.Real:
        """Calculate a record's score."""
        raise NotImplementedError

    @abc.abstractmethod
    def covers(self, other: Subset) -> bool:
        """
        Check whether this subset fully covers a different one.

        This is the case if all records matching other also match self. In
        particular, it's true if the subsets are equal.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def observe(self, record: tp.Record) -> None:
        """
        Observe a record being inserted into storage.

        This can be used by the implementation to maintain information on which
        score intervals actually exist in cache.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def adjust(self, **kwargs: dict[str, t.Any]) -> Adjustment[t.Self]:
        """
        Adjust which records are specified by this subset.

        The implementation-specific kwargs define how exactly this subset is
        changed. Returns a tuple (old, new), where old is an iterable of score
        intervals that are no longer represented by this subset, and new is a
        Subset representing records that may not have been represented
        previously, but should be now.

        The observations of old records are expired.
        """
        raise NotImplementedError


class CachedSubsetWithPrimaryKey(CachedSubset):
    """Convenience subclass providing a primary key name class member."""
    _primary_key_name = 'primary_key_placeholder'

    @classmethod
    def with_primary_key(cls, primary_key_name: str) -> type[t.Self]:
        """Return a subclass with given primary key name."""
        class WithPrimaryKey(cls):
            _primary_key_name = primary_key_name

        return WithPrimaryKey


class All(CachedSubsetWithPrimaryKey):
    """
    A subset that actually represents the whole, matching everything.

    Scores are the hash of the primary key. Defines no DB query arguments and
    is not adjustable.
    """
    def __repr__(self):
        return f'all (with primary key named {self._primary_key_name})'

    @property
    def score_intervals(self) -> ca.Iterable[Interval]:
        return [Interval(float('-inf'), float('inf'))]

    @property
    def db_args(self) -> tuple:
        return ()

    @classmethod
    def record_score(cls, record: tp.Record) -> numbers.Real:
        return hash(record[cls._primary_key_name])

    def covers(self, other: Subset) -> bool:
        return True

    def observe(self, record: tp.Record) -> None:
        pass

    def adjust(self) -> Adjustment[t.Self]:
        raise NotImplementedError


class NumberRangeSubset(CachedSubsetWithPrimaryKey):
    """
    A simple subset matching records by their score directly.

    Scores are the primary key. Can be adjusted by pruning values from the low
    end up to a certain value, and adding new ones from the high end to a new
    upper limit.

    Useful e.g. for the simple case where scores are equal to the numerical
    primary key. The DB args are a 2-tuple specifying the lower and upper
    bound.
    """
    def __init__(self, ge: numbers.Real, lt: numbers.Real) -> None:
        self._ge = ge
        self._lt = lt

    def __repr__(self):
        return (
            f'{type(self).__name__} in the interval [{self._ge}, {self._lt}[')

    @property
    def score_intervals(self) -> ca.Iterable[Interval]:
        return [Interval(self._ge, self._lt)]

    @property
    def db_args(self) -> tuple[numbers.Real, numbers.Real]:
        return (self._ge, self._lt)

    @classmethod
    def record_score(cls, record: tp.Record) -> numbers.Real:
        return record[cls._primary_key_name]

    def covers(self, other: t.Self) -> bool:
        return self._ge <= other._ge and self._lt >= other._lt

    def observe(self, record: tp.Record) -> None:
        pass

    def adjust(self, *, prune_until: numbers.Real,
               extend_until: numbers.Real) -> Adjustment[t.Self]:
        if prune_until < self._ge:
            raise ValueError(
                'New lower bound must not be lower than the current one.')
        if extend_until < self._lt:
            raise ValueError(
                'New upper bound must not be lower than the current one.')
        self._ge = prune_until
        old_lt, self._lt = self._lt, extend_until
        return Adjustment(
            Interval(float('-inf'), self._ge),
            type(self)(self._primary_key_name, old_lt, self._lt))
