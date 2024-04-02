# Copyright 2024 Marc Lehmann

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

import asyncio
import collections.abc as ca
import datetime
import logging
import numbers
import typing as t

import asyncpg

import tablecache as tc
import tablecache.postgres as tcpg
import tablecache.local as tcl

POSTGRES_DSN = 'postgres://postgres:@localhost:5432/postgres'

# In this example, we have a table of timestamped device data, and we want to
# be able to quickly query data from a time range (there is no joining going on
# here to simplify the demo, but a join of multiple relations would work just
# as well). Additionally, we only want to load the most recent data into the
# cache. Older data can still be queried, but will hit Postgres rather than
# the cache. We define a LocalStorageTable subclass that helps us distinguish
# between records fetched from storage and ones from the DB by converting them
# to plain dicts as they're inserted into storage, while the latter remain as
# asyncpg.Records.

type Record = ca.Mapping[str, t.Any]


# This is our Indexes implementation. We want to use it to load, get, and
# adjust records as whole ranges of a timestamp attribute.
class TimeRangeIndexes(tc.Indexes[Record, numbers.Real]):
    # Our IndexSpec specifies a range of records by a lower (inclusive) and
    # upper (exclusive) timestamp bound. We only have one index named time.
    # This class must be an inner class of our Indexes and inherit
    # Indexes.IndexSpec.
    class IndexSpec(tc.Indexes[Record, tc.PrimaryKey].IndexSpec):
        def __init__(
                self, index_name: str, ge_isoformat: str, lt_isoformat: str):
            if index_name != 'time':
                raise ValueError('Only the time index exists.')
            super().__init__(index_name)
            self.ge = datetime.datetime.fromisoformat(ge_isoformat)
            self.lt = datetime.datetime.fromisoformat(lt_isoformat)
            if self.ge > self.lt:
                raise ValueError('Bounds are not in order.')

        # A __repr__ can be useful, as this will be part of the log output.
        def __repr__(self) -> str:
            return f'all records between {self.ge} and {self.lt}'

    # In our Adjustment we store the desired target interval in addition to the
    # specs for expiring and loading.
    class Adjustment(tc.Adjustment[dict]):
        def __init__(
                self, expire_spec: t.Optional[tc.StorageRecordsSpec[dict]],
                load_spec: t.Optional[tc.DbRecordsSpec],
                ge: datetime.datetime, lt: datetime.datetime) -> None:
            super().__init__(expire_spec, load_spec)
            self.ge = ge
            self.lt = lt

        def __repr__(self) -> str:
            return (
                f'adjustment expiring {self.expire_spec} and loading records '
                f'with time between {self.ge.isoformat()} and '
                f'{self.lt.isoformat()}')

    # Our implementation needs the name of the primary key attribute (it can
    # only handle one), the name of the timestamp attribute, and a query string
    # that takes a lower and upper bound as parameters. It maintains the
    # currently loaded lower and upper timestamp bounds.
    def __init__(
            self, primary_key_name: str, ts_attr_name: str,
            query_range_string: str) -> None:
        self._primary_key_name = primary_key_name
        self._ts_attr_name = ts_attr_name
        self._query_range_string = query_range_string
        self._ge = datetime.datetime.fromtimestamp(0)
        self._lt = datetime.datetime.fromtimestamp(0)

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['time'])

    # The score for all records is the epoch timestamp of the timstamp
    # attribute. This is what gets indexed in a sorted list.
    @t.override
    def score(self, index_name: str, record: Record) -> tc.Score:
        if index_name != 'time':
            raise ValueError('Only the time index exists.')
        try:
            return record[self._ts_attr_name].timestamp()
        except KeyError:
            raise ValueError('Missing time attribute.')

    @t.override
    def primary_key(self, record: Record) -> numbers.Real:
        try:
            return record[self._primary_key_name]
        except KeyError:
            raise ValueError('Missing primary key.')

    # To get the records from storage, we specify a single score interval from
    # the requested lower and upper bound.
    @t.override
    def storage_records_spec(
            self, spec: IndexSpec) -> tc.StorageRecordsSpec[dict]:
        return tc.StorageRecordsSpec(
            'time', [tc.Interval(spec.ge.timestamp(), spec.lt.timestamp())])

    # Similarly for the DB, where we specify the bounds as args to our query.
    @t.override
    def db_records_spec(
            self, spec: IndexSpec) -> tc.QueryArgsDbRecordsSpec:
        return tc.QueryArgsDbRecordsSpec(
            self._query_range_string, (spec.ge, spec.lt))

    # To prepare an adjustment, we check whether there are any timestamp ranges
    # covered right now that aren't supposed to be anymore after the
    # adjustment, and we add these as intervals in our expire_spec. Then we
    # specify to load all records in the requested time range. We could invest
    # a little more effort and only load those we don't already have, but
    # loading them again is easier and doesn't harm correctness (only
    # performance). We store the requested time range in the adjustment.
    @t.override
    def prepare_adjustment(self, spec: IndexSpec) -> Adjustment:
        expire_intervals = []
        if self._ge < spec.ge:
            expire_intervals.append(
                tc.Interval(self._ge.timestamp(), spec.ge.timestamp()))
        if self._lt > spec.lt:
            expire_intervals.append(
                tc.Interval(spec.lt.timestamp(), self._lt.timestamp()))
        if expire_intervals:
            expire_spec = tc.StorageRecordsSpec('time', expire_intervals)
        else:
            expire_spec = None
        load_spec = self.db_records_spec(spec)
        return self.Adjustment(expire_spec, load_spec, spec.ge, spec.lt)

    # When the CachedTable has deleted and loaded what we specified when we
    # prepared the adjustment, it asks us to commit the adjustment. At this
    # point we should update our state to reflect the new state of the storage,
    # so we update our timestamp bounds to those in the adjustment.
    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        self._ge = adjustment.ge
        self._lt = adjustment.lt

    # A range of records is covered by the storage if the requested interval is
    # contained in the currently available one.
    @t.override
    def covers(self, spec: IndexSpec) -> bool:
        return self._ge <= spec.ge and self._lt >= spec.lt


async def main():
    # We instantiate our custom Indexes with a suitable query string (note the
    # >= and < for the bounds).
    query_string = 'SELECT * FROM device_data WHERE ts >= $1 AND ts < $2'
    indexes = TimeRangeIndexes('data_id', 'ts', query_string)
    db_access = tcpg.PostgresAccess(dsn=POSTGRES_DSN)
    storage_table = LocalStorageTable(indexes, table_name='device_data')
    table = tc.CachedTable(indexes, db_access, storage_table)
    async with db_access, asyncpg.create_pool(POSTGRES_DSN) as postgres_pool:
        await setup_example_db(postgres_pool)
        # We're loading the table and specifying that records in the range from
        # 2023-01-01T12:00:00 to 2023-01-03T12:00:00 should be loaded (there
        # are records before and after this range that aren't going into the
        # cache).
        await table.load('time', '2023-01-01T12:00:00', '2023-01-03T12:00:00')
        # Depending on whether the range we query for is contained in the
        # cache, we'll either get all records from cache, or all from the DB.
        # There is no middle ground, if the range is not fully cached, the DB
        # will be hit for everything.
        print('All records in cache:')
        await print_records(table, 'time', '2023-01-02', '2023-01-03')
        print('Not all records in cache (will be loaded from DB):')
        await print_records(table, 'time', '2023-01-01', '2023-01-05')
        # We can adjust the range of records in storage. This will cause old
        # records to be discarded and new ones to be loaded.
        await table.adjust('time', '2023-01-03', '2023-01-06')
        print('Records we have expired from cache are loaded from the DB:')
        await print_records(table, 'time', '2023-01-02', '2023-01-03')
        print('Newer records were added to the cache:')
        await print_records(table, 'time', '2023-01-04', '2023-01-06')
        # When a record changes in a way that changes its scores in any of the
        # indexes, we must invalidate it with the appropriate old and new
        # scores. Here, we're updating the record from 2023-01-04 to
        # 2023-01-05, so we invalidate with an old spec around midnight of
        # 2023-01-04, and a new spec around midnight of 2023-01-05. We could
        # have also reloaded the whole range from 2023-01-04 to 2023-01-06.
        # Note though, that the new index spec must still be covered, so we
        # couldn't have gone up to 2023-01-07 (storage only covers up to
        # 2023-01-06).
        await postgres_pool.execute('''
            UPDATE device_data SET ts = '2023-01-05' WHERE data_id = 4''')
        table.invalidate_records(
            [indexes.IndexSpec(
                'time', '2023-01-03T23:50:00', '2023-01-04T00:10:00')],
            [indexes.IndexSpec(
                'time', '2023-01-04T23:50:00', '2023-01-05T00:10:00')])
        print('Record 4 with updated ts inside of the covered range:')
        await print_records(table, 'time', '2023-01-04', '2023-01-06')
        # If we update the record to outside of the covered range, it will be
        # fetched from the DB (with the correct data). Note that it is still in
        # storage with the wrong data, but everything is fetched from the DB
        # due to the cache miss.
        await postgres_pool.execute('''
            UPDATE device_data SET ts = '2023-01-07' WHERE data_id = 4''')
        print(
            'Record 4 with updated ts outside of the covered range (fetched '
            'from DB):')
        await print_records(table, 'time', '2023-01-04', '2023-01-08')
        # To get the record into storage again, we invalidate using the covered
        # range, then adjust to include the record's new timestamp.
        table.invalidate_records(
            [indexes.IndexSpec('time', '2023-01-04', '2023-01-06')],
            [indexes.IndexSpec('time', '2023-01-04', '2023-01-06')])
        await table.adjust('time', '2023-01-03', '2023-01-08')
        print(
            'Record 4 with updated ts now inside the covered range after the '
            'adjustment:')
        await print_records(table, 'time', '2023-01-04', '2023-01-08')


async def setup_example_db(postgres_pool):
    await postgres_pool.execute(
        '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE device_data (
            data_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            data integer NOT NULL,
            ts timestamp without time zone NOT NULL
        );

        INSERT INTO device_data(data, ts) VALUES
            (1, '2023-01-01T00:00:00'),
            (2, '2023-01-02T00:00:00'),
            (3, '2023-01-03T00:00:00'),
            (4, '2023-01-04T00:00:00');''')


async def print_records(table, *args, **kwargs):
    async for record in table.get_records(*args, **kwargs):
        print(record)


# This subclass of LocalStorageTable is only here to distinguish records loaded
# from storage (simple dicts) from ones loaded from the DB (asyncpg.Records)
# for the demonstration.
class LocalStorageTable(tcl.LocalStorageTable):
    async def put_record(self, record: asyncpg.Record) -> None:
        await super().put_record(dict(record))

    async def scratch_put_record(self, record: asyncpg.Record) -> None:
        await super().scratch_put_record(dict(record))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
