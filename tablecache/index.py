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
import itertools as it
import math
import numbers
import operator as op
import typing as t

import tablecache.types as tp


class UnsupportedIndexOperation(Exception):
    """
    Raised to signal that a certain operation is not supported on an index.
    """


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


@dc.dataclass(frozen=True)
class DbRecordsSpec:
    """A specification of records in the DB."""
    query: str
    args: tuple


@dc.dataclass(frozen=True)
class Adjustment:
    """
    A specification of an adjustment to be made to the cache.

    Specifies records that should be expired from the cache's storage, as well
    as ones that should be loaded from the DB and put into storage.

    The records specified via the expire_spec need not necessarily exist in
    storage. Likewise, ones specified via new_spec may already exist. Setting
    either to None signals that no records should be expired or loaded,
    respectively.
    """
    expire_spec: t.Optional[StorageRecordsSpec]
    new_spec: t.Optional[DbRecordsSpec]


class Indexes[PrimaryKey](abc.ABC):
    """
    A set of indexes used to access storage and DB tables.

    Provides a uniform way to specify a set of records to be queried from
    either storage or DB tables. This is done with storage_records_spec() and
    db_records_spec(), respectively.

    Also keeps track of the set of records that are in storage, as opposed to
    those that are only available via the DB. To this end, the observe()
    callback is expected to be called by the cache whenever a record is
    inserted into storage. Calls to covers() then return whether the set of
    records specified there is present in storage. Finally, adjust() can be
    used to change which records are in storage by expressing which ones should
    be. The implementation then returns which records need to be removed from
    storage and which ones fetched from the DB and inserted in order to attain
    that state.

    An Indexes contains one or more indexes that can be used to specify sets of
    records. An index is defined via a score function, which takes a record's
    attributes as kwargs and returns a numerical score. Records can then be
    queried as ranges of these scores quickly. An additional recheck predicate
    can be defined by the index to filter out some potential bycatch. At the
    very least, an index named primary_key must exist, which maps a record's
    primary key to a score.

    The access methods {storage,db}_records_spec() and covers() take an index
    name along with arbitrary args and kwargs, which the specific
    implementation needs to interpret in a useful way. The same parameters
    should always represent the same set of records. E.g., a call to
    storage_records_spec() yields a StorageRecordsSpec to get a set of records
    from storage, while a call to db_recods_spec should yield a DbRecordsSpec
    to get the same set of records from the DB.

    The methods involving index state, covers() and adjust(), may not be
    supported for every index. E.g., an index may only be meant for querying
    (i.e. support covers()), but not for adjusting the indexes. In that case,
    these methods raise an UnsupportedIndexOperation.
    """
    @property
    def index_names(self) -> t.Iterable[str]:
        """Return names of all indexes."""
        return self.score_functions.keys()

    @property
    @abc.abstractmethod
    def score_functions(self) -> t.Mapping[str, tp.ScoreFunction]:
        """
        A mapping of index names to their respective score functions.

        Always contains an entry primary_key.

        Generally, score functions are expected to be called with all
        attributes of a record as kwargs, but at least for the primary_key
        index, just the primary (as a kwarg) will do.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def primary_key_score(self, primary_key: PrimaryKey) -> numbers.Real:
        """
        Calculate the primary key score.

        This functionn takes a primary key itself as an argument, as opposed to
        the primary_key entry in score_functions, which takes record attributes
        as kwargs.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def storage_records_spec(
        self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> StorageRecordsSpec:
        """
        Specify records in storage based on an index.

        Returns a StorageRecordsSpec that specifies the set of records in
        storage that matches the index parameters. args and kwargs are
        interpreted in a way specific to the index.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def db_records_spec(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> DbRecordsSpec:
        """
        Specify records in the DB based on an index.

        Like storage_records_spec(), but specifies the same set of records in
        the DB.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def adjust(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> Adjustment:
        """
        Adjust which records are covered by the indexes.

        Takes implementation-specific args and kwargs for one of the indexes
        that specify the set of records that should be in storage after the
        adjustment. Returns an Adjustment, which contains a StorageRecordsSpec
        of records to delete from storage and a DbRecordsSpec of ones to load
        from the DB in order to attain that state.

        After the call, the indexes assume that the records that were specified
        to be deleted from storage are no longer covered, and likewise that
        those specified to be loaded are. Future calls to covers() will reflect
        that.

        Raises an UnsupportedIndexOperation if adjusting by the given index is
        not supported.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def covers(
            self, index_name: str, *args: t.Any, **kwargs: t.Any) -> bool:
        """
        Check whether the specified records are covered by storage.

        Takes implementation-specific args and kwargs that specify a set of
        records, and returns whether all of them are in storage. This
        determination is based on previous calls to adjust() and observe().

        Note that a record may also be considered covered if doesn't exist.
        E.g., say records with primary keys between 0 and 10 were loaded into
        storage, but none even exists with primary key 5. Then that record is
        still covered by storage, and the cache doesn't need to go to the DB to
        check if that record exists.

        Raises an UnsupportedIndexOperation if the given index doesn't support
        checking coverage. However, the primary_key index always does.
        """
        raise NotImplementedError

    def observe(self, record: tp.Record) -> None:
        """
        Observe a record being inserted into storage.

        This can be used by the implementation to maintain information on which
        records exist and update statistics.
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
