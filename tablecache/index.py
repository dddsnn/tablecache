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
import math
import numbers
import typing as t

import tablecache.types as tp


class UnsupportedIndexOperation(Exception):
    pass


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
    @staticmethod
    def _always_use_record(_):
        return True
    index_name: str
    score_intervals: list[Interval]
    recheck_predicate: ca.Callable[[tp.Record], bool] = _always_use_record


@dc.dataclass(frozen=True)
class DbRecordsSpec:
    query: str
    args: tuple


@dc.dataclass(frozen=True)
class Adjustment:
    expire_spec: t.Optional[StorageRecordsSpec]
    new_spec: t.Optional[DbRecordsSpec]


class Indexes[PrimaryKey](abc.ABC):
    @property
    def index_names(self) -> t.Iterable[str]:
        """Return names of all indexes."""
        return self.score_functions.keys()

    @property
    @abc.abstractmethod
    def score_functions(self) -> t.Mapping[str, tp.ScoreFunction]:
        raise NotImplementedError

    @abc.abstractmethod
    def primary_key_score(self, primary_key: PrimaryKey) -> numbers.Real:
        raise NotImplementedError

    @abc.abstractmethod
    def storage_records_spec(
        self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> StorageRecordsSpec:
        raise NotImplementedError

    @abc.abstractmethod
    def db_records_spec(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> DbRecordsSpec:
        raise NotImplementedError

    @abc.abstractmethod
    def adjust(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> Adjustment:
        raise NotImplementedError

    @abc.abstractmethod
    def covers(
            self, index_name: str, *args: t.Any, **kwargs: t.Any) -> bool:

        raise NotImplementedError

    def observe(self, record: tp.Record) -> None:
        """
        Observe a record being inserted into storage.

        This can be used by the implementation to maintain information on which
        score intervals actually exist in cache.
        """


class PrimaryKeyIndexes(Indexes[numbers.Real]):

    def __init__(
            self, primary_key_name: str, query_all_string: str,
            query_some_string: str) -> None:
        self._primary_key_name = primary_key_name
        self._query_all_string = query_all_string
        self._query_some_string = query_some_string
        self._covers_all = False
        self._primary_keys = set()

    @property
    def score_functions(self) -> t.Mapping[str, tp.ScoreFunction]:
        return {'primary_key': self._extract_primary_key}

    def _extract_primary_key(self, **kwargs):
        return kwargs[self._primary_key_name]

    def primary_key_score(self, primary_key: numbers.Real) -> numbers.Real:
        return self.score_functions['primary_key'](
            **{self._primary_key_name: primary_key})

    def storage_records_spec(
            self, index_name: str, *primary_keys: numbers.Real
    ) -> StorageRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        intervals = [Interval(
            primary_key, math.nextafter(primary_key, float('inf')))
            for primary_key in primary_keys]
        return StorageRecordsSpec(index_name, intervals)

    def db_records_spec(
            self, index_name: str, *primary_keys: numbers.Real
    ) -> DbRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        if not primary_keys:
            return DbRecordsSpec(self._query_all_string, ())
        return DbRecordsSpec(self._query_some_string, (primary_keys,))

    def adjust(
            self, index_name: str, *primary_keys: numbers.Real) -> Adjustment:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        if self._covers_all:
            if not primary_keys:
                return Adjustment(None, None)
            self._covers_all = False
            self._primary_keys = set(primary_keys)
            return Adjustment(
                StorageRecordsSpec(
                    'primary_key', [Interval(float('-inf'), float('inf'))]),
                self.db_records_spec('primary_key', *primary_keys))
        if not primary_keys:
            self._covers_all = True
            return Adjustment(None, self.db_records_spec('primary_key'))
        self._primary_keys = set(primary_keys)
        return Adjustment(
            StorageRecordsSpec(
                'primary_key', [Interval(float('-inf'), float('inf'))]),
            self.db_records_spec('primary_key', *primary_keys))

    def covers(self, index_name: str, *primary_keys: numbers.Real) -> bool:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        if self._covers_all:
            return True
        if not primary_keys:
            return False
        return all(pk in self._primary_keys for pk in primary_keys)

    def observe(self, record: tp.Record) -> None:
        self._primary_keys.add(self._extract_primary_key(**record))
