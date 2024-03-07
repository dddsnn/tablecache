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
import dataclasses as dc
import math
import numbers
import operator as op
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


class RecordScorer[PrimaryKey: tp.PrimaryKey](abc.ABC):
    """
    Score calculator for a set of indexes.

    Provides a way to calculate the scores of records in a number of indexes.
    Scores are orderable (most likely some kind of number) that give records a
    place in an index and make it possible to query many records quickly using
    a range of scores. Scores need not be unique (although it's better to avoid
    too many collisions).

    Every record always has a primary key which uniquely identifies it, which
    can be extracted from a record using primary_key().

    This is the limited interface required by implementations of StorageTable,
    but it's probably best implemented as part of an Indexes.
    """
    @property
    @abc.abstractmethod
    def index_names(self) -> frozenset[str]:
        """
        Return names of all indexes.

        These are the names of all the indexes for which scores can be
        calculated. Always contains at least primary_key.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def score(self, index_name: str, record: tp.Record) -> tp.Score:
        """
        Calculate a record's score for an index.

        Raises a ValueError if the given index doesn't exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def primary_key(self, record: tp.Record) -> PrimaryKey:
        """
        Extract the primary key from a record.

        Raises a ValueError if the primary key is missing or otherwise invalid.
        """
        raise NotImplementedError


class Indexes[PrimaryKey: tp.PrimaryKey](RecordScorer[PrimaryKey]):
    """
    A set of indexes used to access storage and DB tables.

    This adds storage state information and ways to query a storage table and
    the DB to the RecordScorer interface. The purpose of Indexes is to tie its
    different indexes, their respective scoring and record access, together and
    potentially share information between them.

    Provides a uniform way to specify a set of records to be queried from
    either storage or DB tables. This is done with storage_records_spec() and
    db_records_spec(), respectively.

    Also keeps track of the set of records available from storage, as opposed
    to those that are only available via the DB. To this end,
    prepare_adjustment() is expected to be called before loading records into
    storage, and commit_adjustment() when the load is complete. From that point
    on, the state considers the records that it specified to load to be in
    storage. Further adjustments can be made later in order to change the
    records in storage.

    The covers() method can be used to check whether a set of records is
    available from storage.

    Methods for which a set of records needs to be specified
    ({storage,db}_records_spec(), covers(), and prepare_adjustment()) take an
    instance of the IndexSpec inner class. This encapsulates the way to specify
    a particular set of records for the particular Indexes implementation.
    Subclasses may define their own IndexSpecs, but these must be inner classes
    and subclasses of IndexSpec (i.e. issubclass(
    MyIndexesImplementation.IndexSpec, Indexes,IndexSpec)).

    The methods involving index state, covers() and prepare_adjustment(), may
    not be supported for every index. E.g., an index may only be meant for
    querying (i.e. support covers()), but not for adjusting the indexes. In
    that case, these methods raise an UnsupportedIndexOperation. If any method
    is called with the name of an index that doesn't exist, a ValueError is
    raised.
    """
    class IndexSpec:
        """Specification of a set of records in an index."""

        def __init__(self, index_name: str) -> None:
            self.index_name = index_name

    @abc.abstractmethod
    def storage_records_spec(
            self, spec: IndexSpec) -> storage.StorageRecordsSpec:
        """
        Specify records in storage based on an index.

        Returns a StorageRecordsSpec that specifies the set of records in
        storage that matches the index spec.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def db_records_spec(self, spec: IndexSpec) -> db.DbRecordsSpec:
        """
        Specify records in the DB based on an index.

        Like storage_records_spec(), but specifies the same set of records in
        the DB.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_adjustment(self, spec: IndexSpec) -> Adjustment:
        """
        Prepare an adjustment of which records are covered by the indexes.

        Returns an Adjustment, which contains a StorageRecordsSpec of records
        to delete from storage and a DbRecordsSpec of ones to load from the DB
        in order to attain the state in which exactly the records specified via
        the index spec are loaded.

        This method only specifies what would need to change in order to adjust
        the indexes, but does not modify the internal state of the Indexes.

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
    def covers(self, spec: IndexSpec) -> bool:
        """
        Check whether the specified records are covered by storage.

        Returns whether all of the records specified via the index spec are in
        storage. This determination is based on previous calls to
        commit_adjustment() and observe().

        May also return False if the records may be covered, but there isn't
        enough information to be certain. This could happen when the Indexes
        are adjusted by a different index than this covers check is done. E.g.,
        if an adjustment containing a specific set of primary keys is committed
        and then a covers check is done for a range of primary keys, there may
        not be enough information to determine whether the set that was loaded
        contained all primary keys in the range.

        A record may also be considered covered if it doesn't exist. E.g., say
        records with primary keys between 0 and 10 were loaded into storage,
        but none even exists with primary key 5. Then that record is still
        covered by storage, and the cache doesn't need to go to the DB to check
        if that record exists.

        Raises an UnsupportedIndexOperation if the given index doesn't support
        checking coverage.
        """
        raise NotImplementedError

    def observe(self, record: tp.Record) -> None:
        """
        Observe a record being inserted into storage.

        This can be used by the implementation to maintain information on which
        records exist and update statistics.
        """


class AllIndexes(Indexes[tp.PrimaryKey]):
    """
    Very simple indexes loading everything.

    Only a single index named all, but it essentially doesn't do anything. All
    operations load everything. The only control there is is to specify a
    recheck_predicate as a filter, but it is only used in
    storage_records_spec().
    """
    class IndexSpec(Indexes[tp.PrimaryKey].IndexSpec):
        def __init__(
                self, index_name: str,
                recheck_predicate: tp.RecheckPredicate =
                storage.StorageRecordsSpec.always_use_record) -> None:
            self.index_name = index_name
            self.recheck_predicate = recheck_predicate

        def __repr__(self):
            return (
                'IndexSpec specifying all records matching '
                f'{self.recheck_predicate}')

    def __init__(
            self, primary_key_attrs: tuple[str, ...], query_all_string: str
    ) -> None:
        """
        :param primary_key_attrs: Tuple of attribute name that make up the
            primary key.
        :param query_all_string: A string to query all records from the DB.
        """
        self._primary_key_extractor = op.itemgetter(*primary_key_attrs)
        self._query_all_string = query_all_string

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['all'])

    @t.override
    def score(self, index_name: str, record: tp.Record) -> tp.Score:
        return 0

    @t.override
    def primary_key(self, record: tp.Record) -> t.Any:
        try:
            return self._primary_key_extractor(record)
        except KeyError:
            raise ValueError('Missing primary key.')

    @t.override
    def storage_records_spec(
            self, spec: IndexSpec) -> storage.StorageRecordsSpec:
        return storage.StorageRecordsSpec(
            'all', [storage.Interval.everything()], spec.recheck_predicate)

    @t.override
    def db_records_spec(self, spec: IndexSpec) -> db.QueryArgsDbRecordsSpec:
        return db.QueryArgsDbRecordsSpec(self._query_all_string, ())

    @t.override
    def prepare_adjustment(
            self, spec: IndexSpec) -> Adjustment:
        return Adjustment(None, self.db_records_spec(self.IndexSpec('all')))

    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        pass

    @t.override
    def covers(self, spec: IndexSpec) -> bool:
        return True


class PrimaryKeyIndexes(Indexes[tp.PrimaryKey]):
    """
    Simple indexes for only selected primary keys.

    An index capable of loading either everything, or a select set of primary
    keys. Only the primary_key index is supported. Scores are the primary key's
    hash, so anything hashable works as keys. Only a single primary key
    attribute is supported.

    The implementation is very basic and likely only useful for testing and
    demonstration. Issues in practice could be:

    - In storage_records_spec(), one interval is included for every primary
      key, which makes no use of fast access to storage an is likely slow.
    - When loading select keys, all of them are stored in a set, which can get
      big.
    - When adjusting to a different, disjoint set of primary keys, everything
      is expired and loaded fresh, instead of only loading the difference.
    """
    class IndexSpec(Indexes[tp.PrimaryKey].IndexSpec):
        def __init__(
                self, index_name: str, *primary_keys: tp.PrimaryKey,
                all_primary_keys: bool = False):
            """
            :param index_name: Must be primary_key.
            :param primary_keys: Individual primary keys to specify. Mutually
                exclusive with all_primary_keys.
            :param all_primary_keys: Whether to specify all primary keys.
                Mutually exclusive with primary_keys.
            """
            if index_name != 'primary_key':
                raise ValueError('Only the primary_key index exists.')
            if primary_keys and all_primary_keys:
                raise ValueError(
                    'Must specify either to use all primary keys or specific '
                    'ones, not both.')
            super().__init__(index_name)
            self.primary_keys = primary_keys
            self.all_primary_keys = all_primary_keys

        def __repr__(self):
            if self.all_primary_keys:
                return 'IndexSpec specifying all records'
            return (
                'IndexSpec specifying records with primary keys '
                f'{self.primary_keys}')

    @dc.dataclass(frozen=True)
    class Adjustment(Adjustment):
        primary_keys: set[tp.PrimaryKey]
        cover_all: bool

    def __init__(
            self, primary_key_name: str, query_all_string: str,
            query_some_string: str) -> None:
        """
        :param primary_key_name: Name of the primary key attribute.
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
    def score(self, index_name: str, record: tp.Record) -> tp.Score:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return hash(self.primary_key(record))

    @t.override
    def primary_key(self, record: tp.Record) -> t.Any:
        try:
            return record[self._primary_key_name]
        except KeyError:
            raise ValueError('Missing primary key.')

    @t.override
    def storage_records_spec(
            self, spec: IndexSpec) -> storage.StorageRecordsSpec:
        if spec.all_primary_keys:
            intervals = [storage.Interval.everything()]
            recheck_predicate = storage.StorageRecordsSpec.always_use_record
        else:
            primary_keys = frozenset(spec.primary_keys)
            intervals = []
            for primary_key in primary_keys:
                score = hash(primary_key)
                score_plus_epsilon = math.nextafter(score, float('inf'))
                intervals.append(storage.Interval(score, score_plus_epsilon))

            def recheck_predicate(record):
                return self.primary_key(record) in primary_keys
        return storage.StorageRecordsSpec(
            spec.index_name, intervals, recheck_predicate)

    @t.override
    def db_records_spec(self, spec: IndexSpec) -> db.QueryArgsDbRecordsSpec:
        if spec.all_primary_keys:
            return db.QueryArgsDbRecordsSpec(self._query_all_string, ())
        return db.QueryArgsDbRecordsSpec(
            self._query_some_string, (spec.primary_keys,))

    @t.override
    def prepare_adjustment(self, spec: IndexSpec) -> Adjustment:
        if spec.all_primary_keys:
            expire_spec = None
        else:
            expire_spec = storage.StorageRecordsSpec(
                'primary_key', [storage.Interval.everything()])
        if self._covers_all and spec.all_primary_keys:
            new_spec = None
        else:
            new_spec = self.db_records_spec(spec)
        return self.Adjustment(
            expire_spec, new_spec, set(spec.primary_keys),
            spec.all_primary_keys)

    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        self._primary_keys = adjustment.primary_keys
        self._covers_all = adjustment.cover_all

    @t.override
    def covers(self, spec: IndexSpec) -> bool:
        if self._covers_all:
            return True
        if spec.all_primary_keys:
            return False
        return all(pk in self._primary_keys for pk in spec.primary_keys)


class PrimaryKeyRangeIndexes(Indexes[numbers.Real]):
    """
    Simple indexes for a range of primary keys.

    An index capable of loading a range of primary keys. Only the primary_key
    index is supported. Primary keys must be numbers.

    Ranges of primary keys are specified as an inclusive lower bound (ge) and
    an exclusive upper bound (lt) (greater-equal and less-than).

    The implementation is quite simple, and adjusts will always expire all
    current data and load the entire requested data set, even if they overlap
    substantially.
    """
    class IndexSpec(Indexes[tp.PrimaryKey].IndexSpec):
        def __init__(
                self, index_name: str, *, ge: numbers.Real, lt: numbers.Real):
            """
            :param index_name: Must be primary_key.
            :param ge: Lower (inclusive) bound.
            :param lt: Upper (exclusive) bound.
            """
            if index_name != 'primary_key':
                raise ValueError('Only the primary_key index exists.')
            super().__init__(index_name)
            self.interval = storage.Interval(ge, lt)

        def __repr__(self):
            if self.all_primary_keys:
                return 'IndexSpec specifying all records'
            return (
                'IndexSpec specifying records with primary keys in '
                f'{self.interval}')

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
    def score(self, index_name: str, record: tp.Record) -> tp.Score:
        if index_name != 'primary_key':
            raise ValueError('Only the primary_key index exists.')
        return self.primary_key(record)

    @t.override
    def primary_key(self, record: tp.Record) -> numbers.Real:
        try:
            return record[self._primary_key_name]
        except KeyError:
            raise ValueError('Missing primary key.')

    @t.override
    def storage_records_spec(
            self, spec: IndexSpec) -> storage.StorageRecordsSpec:
        return storage.StorageRecordsSpec('primary_key', [spec.interval])

    @t.override
    def db_records_spec(
            self, spec: IndexSpec) -> db.QueryArgsDbRecordsSpec:
        return db.QueryArgsDbRecordsSpec(
            self._query_range_string, (spec.interval.ge, spec.interval.lt))

    @t.override
    def prepare_adjustment(self, spec: IndexSpec) -> Adjustment:
        expire_spec = storage.StorageRecordsSpec(
            'primary_key', [self._interval])
        new_spec = self.db_records_spec(spec)
        return self.Adjustment(expire_spec, new_spec, spec.interval)

    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        self._interval = adjustment.interval

    @t.override
    def covers(self, spec: IndexSpec) -> bool:
        return self._interval.covers(spec.interval)
