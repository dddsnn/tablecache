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
import math
import numbers
import typing as t

import tablecache.db as db
import tablecache.storage as storage
import tablecache.types as tp


class UnsupportedIndexOperation(Exception):
    """
    Raised to signal that a certain operation is not supported on an index.
    """


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
    expire_spec: t.Optional[storage.StorageRecordsSpec]
    new_spec: t.Optional[db.DbRecordsSpec]


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
    primary key to a score. The purpose of the Indexes is to tie its different
    indexes together and potentially share information between them.

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
    @abc.abstractmethod
    def index_names(self) -> frozenset[str]:
        """
        Return names of all indexes.

        These are the names of all the indexes in this instance. Always
        contains at least primary_key.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, index_name: str, record: tp.Record) -> numbers.Real:
        """
        Calculate a record's score for an index.

        Raises a ValueError if the given index doesn't exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def primary_key_score(self, primary_key: PrimaryKey) -> numbers.Real:
        """Calculate the primary key score."""
        raise NotImplementedError

    @abc.abstractmethod
    def storage_records_spec(
        self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> storage.StorageRecordsSpec:
        """
        Specify records in storage based on an index.

        Returns a StorageRecordsSpec that specifies the set of records in
        storage that matches the index parameters. args and kwargs are
        interpreted in a way specific to the index.

        When index_name is primary_key, this method can always be called with
        a single primary key and no other arguments to specify that record.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def db_records_spec(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> db.DbRecordsSpec:
        """
        Specify records in the DB based on an index.

        Like storage_records_spec(), but specifies the same set of records in
        the DB.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_adjustment(
            self, index_name: str, *args: t.Any, **kwargs: t.Any
    ) -> Adjustment:
        """
        Prepare an adjustment of which records are covered by the indexes.

        Takes implementation-specific args and kwargs for one of the indexes
        that specify the set of records that should be in storage after the
        adjustment. Returns an Adjustment, which contains a StorageRecordsSpec
        of records to delete from storage and a DbRecordsSpec of ones to load
        from the DB in order to attain that state.

        This method only specifies what would need to change in order to adjust
        the indexes, but does not modify the internal state of the Indexes

        May return a subclass of Adjustment that contains additional
        information needed in commit_adjustment().

        Raises an UnsupportedIndexOperation if adjusting by the given index is
        not supported.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        """
        Commits a prepared adjustment.

        Takes an Adjustment previously returned from prepare_adjustment() and
        modifies internal state to reflect it. After the call, the indexes
        assume that the records that were specified to be deleted from storage
        are no longer covered, and likewise that those specified to be loaded
        are. Future calls to covers() will reflect that.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def covers(
            self, index_name: str, *args: t.Any, **kwargs: t.Any) -> bool:
        """
        Check whether the specified records are covered by storage.

        Takes implementation-specific args and kwargs that specify a set of
        records, and returns whether all of them are in storage. This
        determination is based on previous calls to commit_adjustment() and
        observe().

        May also return False if the records may be covered, but there isn't
        enough information to be certain. This could happen when the Indexes
        are adjusted by a different index than this covers check is done. E.g.,
        if an adjustment containing a specific set of primary keys is committed
        and then a covers check is done for a range of primary keys, there may
        not be enough information to determine whether the set that was loaded
        contained all primary keys in the range.

        A record may also be considered covered if doesn't exist. E.g., say
        records with primary keys between 0 and 10 were loaded into storage,
        but none even exists with primary key 5. Then that record is still
        covered by storage, and the cache doesn't need to go to the DB to check
        if that record exists.

        Raises an UnsupportedIndexOperation if the given index doesn't support
        checking coverage. However, the primary_key index always does. Also,
        like in *_records_spec(), when index_name is primary_key, this method
        can always be called with just a single primary key and no other
        arguments.
        """
        raise NotImplementedError

    def observe(self, record: tp.Record) -> None:
        """
        Observe a record being inserted into storage.

        This can be used by the implementation to maintain information on which
        records exist and update statistics.
        """


class AllIndexes(Indexes[t.Any]):
    """
    Very simple indexes loading everything.

    Only the primary_key index is supported, but it essentially doesn't do
    anything. All operations load everything (even if a single primary key is
    specified). The only control there is is to specify a recheck_predicate in
    storage_records_spec() as a filter.
    """

    def __init__(self, query_all_string: str) -> None:
        """
        :param query_all_string: A string to query all records from the DB.
        """
        self._query_all_string = query_all_string

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['primary_key'])

    @t.override
    def score(self, index_name: str, record: tp.Record) -> numbers.Real:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return 0

    @t.override
    def primary_key_score(self, primary_key: ca.Hashable) -> numbers.Real:
        return 0

    @t.override
    def storage_records_spec(
        self, index_name: str, *index_args: t.Any,
            recheck_predicate: tp.RecheckPredicate =
            storage.StorageRecordsSpec.always_use_record
    ) -> storage.StorageRecordsSpec:
        """
        :param recheck_prediate: Optional predicate to filter records.
        """
        return storage.StorageRecordsSpec(
            'primary_key', [storage.Interval.everything()], recheck_predicate)

    @t.override
    def db_records_spec(
        self, index_name: str, *index_args: t.Any
    ) -> db.QueryArgsDbRecordsSpec:
        return db.QueryArgsDbRecordsSpec(self._query_all_string, ())

    @t.override
    def prepare_adjustment(
            self, index_name: str, *index_args: t.Any) -> Adjustment:
        return Adjustment(None, self.db_records_spec('primary_key'))

    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        pass

    @t.override
    def covers(self, index_name: str, *index_args: t.Any) -> bool:
        return True


class PrimaryKeyIndexes(Indexes[ca.Hashable]):
    """
    Simple indexes for only selected primary keys.

    An index capable of loading either everything, or a select set of primary
    keys. Only the primary_key index is supported. Scores are the primary key's
    hash, so anything hashable works as keys.

    The implementation is very basic and likely only useful for testing and
    demonstration. Issues in practice could be:

    - In storage_records_spec(), one interval is included for every primary
      key, which makes no use of fast access to storage an is likely slow.
    - When loading select keys, all of them are stored in a set, which can get
      big.
    - When adjusting to a different, disjoint set of primary keys, everything
      is expired and loaded fresh, instead of only loading the difference.
    """
    @dc.dataclass(frozen=True)
    class Adjustment(Adjustment):
        primary_keys: set[ca.Hashable]
        cover_all: bool

    def __init__(
            self, primary_key_name: str, query_all_string: str,
            query_some_string: str) -> None:
        """
        :param primary_key_name: Name of the primary key.
        :query_all_string: A query string used to query all records in the DB.
            Will be used without parameters.
        :query_some_string: A query string used to query only a selection of
            primary keys. Will be used with a single parameter, which is a
            tuple of the primary key. Essentially, the query will have to
            include something like `WHERE primary_key = ANY($1)`.
        """
        self._primary_key_name = primary_key_name
        self._query_all_string = query_all_string
        self._query_some_string = query_some_string
        self._covers_all = False
        self._primary_keys = set()

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['primary_key'])

    @t.override
    def score(self, index_name: str, record: tp.Record) -> numbers.Real:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return hash(record[self._primary_key_name])

    @t.override
    def primary_key_score(self, primary_key: ca.Hashable) -> numbers.Real:
        return hash(primary_key)

    @t.override
    def storage_records_spec(
            self, index_name: str, *primary_keys: ca.Hashable,
            all_primary_keys: bool = False) -> storage.StorageRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        if all_primary_keys:
            intervals = [storage.Interval.everything()]
            recheck_predicate = storage.StorageRecordsSpec.always_use_record
        else:
            primary_keys = frozenset(primary_keys)
            intervals = []
            for primary_key in primary_keys:
                score = self.primary_key_score(primary_key)
                score_plus_epsilon = math.nextafter(score, float('inf'))
                intervals.append(storage.Interval(score, score_plus_epsilon))

            def recheck_predicate(record):
                return record[self._primary_key_name] in primary_keys
        return storage.StorageRecordsSpec(
            index_name, intervals, recheck_predicate)

    @t.override
    def db_records_spec(
        self, index_name: str, *primary_keys: ca.Hashable,
            all_primary_keys: bool = False) -> db.QueryArgsDbRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        if all_primary_keys:
            return db.QueryArgsDbRecordsSpec(self._query_all_string, ())
        return db.QueryArgsDbRecordsSpec(
            self._query_some_string, (primary_keys,))

    @t.override
    def prepare_adjustment(
            self, index_name: str, *primary_keys: ca.Hashable,
            all_primary_keys: bool = False) -> 'PrimaryKeyIndexes.Adjustment':
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        new_primary_keys = set() if all_primary_keys else set(primary_keys)
        if all_primary_keys:
            expire_spec = None
        else:
            expire_spec = storage.StorageRecordsSpec(
                'primary_key', [storage.Interval.everything()])
        if self._covers_all and all_primary_keys:
            new_spec = None
        elif not all_primary_keys:
            new_spec = self.db_records_spec('primary_key', *primary_keys)
        else:
            new_spec = self.db_records_spec(
                'primary_key', all_primary_keys=True)
        return self.Adjustment(
            expire_spec, new_spec, new_primary_keys, all_primary_keys)

    @t.override
    def commit_adjustment(
            self, adjustment: 'PrimaryKeyIndexes.Adjustment') -> None:
        self._primary_keys = adjustment.primary_keys
        self._covers_all = adjustment.cover_all

    @t.override
    def covers(
            self, index_name: str, *primary_keys: ca.Hashable,
            all_primary_keys: bool = False) -> bool:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        if self._covers_all:
            return True
        if all_primary_keys:
            return False
        return all(pk in self._primary_keys for pk in primary_keys)


class PrimaryKeyRangeIndexes(Indexes[numbers.Real]):
    """
    Simple indexes for a range of primary keys.

    An index capable of loading a range of primary keys. Only the primary_key
    index is supported. Primary keys must be numbers, and are equal to their
    scores. No recheck predicate is needed.

    Methods taking index args take (in addition to the index_name, which must
    be 'primary_key'), either a single primary_key, or a range of keys via ge
    and lt (greater-equal and less-than).

    The implementation is quite simple, and adjusts will always expire all
    current data and load the entire requested data set, even if they overlap
    substantially.
    """
    @dc.dataclass(frozen=True)
    class Adjustment(Adjustment):
        interval: storage.Interval

    def __init__(self, primary_key_name: str, query_range_string: str) -> None:
        """
        :param primary_key_name: Name of the primary key.
        :query_range_string: A query string used to query a range of records in
            the DB. Will be used with 2 parameters, the lower inclusive bound
            and the upper exclusive bound. That means the query will likely
            have to contain something like `WHERE primary_key >= $1 AND
            primary_key < $2`.
        """
        self._primary_key_name = primary_key_name
        self._query_range_string = query_range_string
        self._interval = storage.Interval(0, 0)

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['primary_key'])

    @t.override
    def score(self, index_name: str, record: tp.Record) -> numbers.Real:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return record[self._primary_key_name]

    @t.override
    def primary_key_score(self, primary_key: numbers.Real) -> numbers.Real:
        return primary_key

    @t.override
    def storage_records_spec(
            self, index_name: str,
            primary_key: t.Optional[numbers.Real] = None,
            ge: t.Optional[numbers.Real] = None,
            lt: t.Optional[numbers.Real] = None) -> storage.StorageRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return storage.StorageRecordsSpec(
            index_name, [self._interval_from_args(primary_key, ge, lt)])

    def _interval_from_args(self, primary_key, ge, lt):
        if primary_key is not None:
            if ({ge, lt} != {None}):
                raise ValueError(
                    'Specify either a single primary key or an interval.')
            return storage.Interval.only_containing(primary_key)
        else:
            if primary_key is not None:
                raise ValueError(
                    'Specify either a single primary key or an interval.')
            return storage.Interval(ge, lt)

    @t.override
    def db_records_spec(
        self, index_name: str,
            primary_key: t.Optional[numbers.Real] = None,
            ge: t.Optional[numbers.Real] = None,
            lt: t.Optional[numbers.Real] = None) -> db.QueryArgsDbRecordsSpec:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        interval = self._interval_from_args(primary_key, ge, lt)
        return db.QueryArgsDbRecordsSpec(
            self._query_range_string, (interval.ge, interval.lt))

    @t.override
    def prepare_adjustment(
            self, index_name: str,
            primary_key: t.Optional[numbers.Real] = None,
            ge: t.Optional[numbers.Real] = None,
            lt: t.Optional[numbers.Real] = None
    ) -> 'PrimaryKeyRangeIndexes.Adjustment':
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        new_interval = self._interval_from_args(primary_key, ge, lt)
        expire_intervals = [self._interval]
        expire_spec = storage.StorageRecordsSpec(
            'primary_key', expire_intervals)
        new_spec = self.db_records_spec('primary_key', primary_key, ge, lt)
        return self.Adjustment(expire_spec, new_spec, new_interval)

    @t.override
    def commit_adjustment(
            self, adjustment: 'PrimaryKeyRangeIndexes.Adjustment') -> None:
        self._interval = adjustment.interval

    @t.override
    def covers(
            self, index_name: str,
            primary_key: t.Optional[numbers.Real] = None,
            ge: t.Optional[numbers.Real] = None,
            lt: t.Optional[numbers.Real] = None) -> bool:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        interval = self._interval_from_args(primary_key, ge, lt)
        if primary_key is not None:
            return primary_key in self._interval
        return (self._interval.ge <= interval.ge and
                interval.lt <= self._interval.lt)
