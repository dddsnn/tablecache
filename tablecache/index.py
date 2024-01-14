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
class Adjustment:
    expire_intervals: ca.Iterable[Interval]
    index_name: t.Optional[str]
    index_args: tuple[t.Any]
    index_kwargs: dict[str, t.Any]


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
    def storage_intervals(
        self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> ca.Iterable[Interval]:
        raise NotImplementedError

    @abc.abstractmethod
    def db_query_range(
        self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> tuple[str, tuple]:
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

    # @abc.abstractmethod
    # def covers_subset(
    #         self, index_name: str, *args: t.Any,
    #         **kwargs:t.Any) -> bool:
    #     raise NotImplementedError

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
        return primary_key

    def storage_intervals(
            self, index_name: str, *primary_keys: numbers.Real
    ) -> ca.Iterable[Interval]:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        for primary_key in primary_keys:
            yield Interval(
                primary_key, math.nextafter(primary_key, float('inf')))

    def db_query_range(
            self, index_name: str, *primary_keys: numbers.Real
    ) -> tuple[str, tuple]:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        if not primary_keys:
            return self._query_all_string, ()
        return self._query_some_string, (primary_keys,)

    def adjust(
            self, index_name: str, *primary_keys: numbers.Real) -> Adjustment:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index is supported.')
        if self._covers_all:
            if not primary_keys:
                return Adjustment([], None, (), {})
            self._covers_all = False
            self._primary_keys = set()
            return Adjustment(
                [Interval(float('-inf'), float('inf'))], 'primary_key',
                primary_keys, {})
        if not primary_keys:
            self._covers_all = True
            return Adjustment([], 'primary_key', (), {})
        self._primary_keys = set()
        return Adjustment(
            [Interval(float('-inf'), float('inf'))], 'primary_key',
            primary_keys, {})

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