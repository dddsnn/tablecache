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
import numbers
import typing as t


class Range(abc.ABC):
    """
    Specification of a range of records in a table.

    Specifies a subset of a table in which values are contiguous in some way.

    For storage tables, which are ordered by a score, has the property score_ge
    for the lower bound and score_lt for the upper bound of the score. All
    records with a score greater or equal than score_ge and less than score_lt
    are considered to be in the range.

    For DB tables, the db_args property defines a tuple of query parameters
    that must match the range query of the DB table and parameterize it in a
    way such that only those records are returned that match the range.

    It is up to the implementor to ensure that the score properties and db_args
    match the same records in their respective databases.
    """
    @property
    @abc.abstractmethod
    def score_ge(self) -> numbers.Real:
        """
        Lower bound for storage scores.

        Records matching this range must have a score greater or equal than
        this.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def score_lt(self) -> numbers.Real:
        """
        Upper bound for storage scores.

        Records matching this range must have a score less than this.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def db_args(self) -> tuple[t.Any]:
        """
        Tuple of DB query arguments.

        Must match the query which the DB table uses to fetch a range of
        records.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def covers(self, other: t.Self) -> bool:
        """
        Check whether this range fully covers a different range.

        This is the case if all records matching other also match self. In
        particular, it's true if the ranges are equal.
        """
        raise NotImplementedError


class AllRange(Range):
    """
    A range matching everything.

    Defines no DB query arguments.
    """
    def __repr__(self):
        return type(self).__name__

    @property
    def score_ge(self) -> numbers.Real:
        return float('-inf')

    @property
    def score_lt(self) -> numbers.Real:
        return float('inf')

    @property
    def db_args(self) -> tuple:
        return ()

    def covers(self, other: t.Self) -> bool:
        return True


class NumberRange(Range):
    """
    A simple range matching records by their score directly.

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
    def score_ge(self) -> numbers.Real:
        return self._ge

    @property
    def score_lt(self) -> numbers.Real:
        return self._lt

    @property
    def db_args(self) -> tuple[numbers.Real]:
        return (self._ge, self._lt)

    def covers(self, other: t.Self) -> bool:
        return self._ge <= other._ge and self._lt >= other._lt
