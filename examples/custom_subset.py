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

import asyncio
import collections.abc as ca
import contextlib
import datetime
import numbers
import pathlib
import sys
import typing as t

import asyncpg
import redis.asyncio as redis

import tablecache as tc
import tablecache.types as tp

sys.path.append(str(pathlib.Path(__file__).parent.parent))


# In this example, we have a table of timestamped device data, and we want to
# be able to quickly query data from a time range (there is no joining going on
# here to simplify the demo, but a join of multiple relations would work just
# as well). Additionally, we only want to load the most recent data into the
# cache (as older data is rarely interesting). Older data can still be queried,
# but will hit Postgres rather than Redis.
#
# The Redis backend requires that each record has exactly one (float) score
# that defines its order by which it can be efficiently found. We will create a
# custom CachedSubset subclass that uses the timestamp as the score.
async def main():
    base_query_string = 'SELECT * FROM device_data'
    # The query_subset_string needs to have parameters $1 etc. that match the
    # parameters of our custom subset. In our case, we want to filter by
    # timestamp lower and upper bounds. Note that lower bounds are always
    # inclusive and upper bounds exclusive.
    query_subset_string = f'{base_query_string} WHERE ts >= $1 AND ts < $2'
    query_pks_string = f'{base_query_string} WHERE device_id = ANY ($1)'
    postgres_pool = asyncpg.create_pool(
        min_size=0, max_size=1,
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_conn = redis.Redis()
    db_table = tc.PostgresTable(
        postgres_pool, query_subset_string, query_pks_string)
    table = tc.CachedTable(
        TsSubset,
        db_table,
        primary_key_name='data_id',
        attribute_codecs={
            'data_id': tc.IntAsStringCodec(),
            'data': tc.IntAsStringCodec(),
            'ts': tc.UtcDatetimeCodec(), },
        redis_conn=redis_conn,
        redis_table_name='device_data',
    )
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(postgres_pool)
        await setup_example_dbs(redis_conn, postgres_pool)
        # We're loading the table and specifying that records in the range from
        # 2023-01-01T12:00:00 to 2023-01-03T12:00:00 should be loaded (so there
        # are records before and after this range that aren't going into the
        # cache).
        await table.load('2023-01-01T12:00:00', '2023-01-03T12:00:00')
        # Now that the cache is loaded, we can query it by subset. Depending on
        # whether the subset we query for is contained in the cache, we'll
        # either get all records from cache, or all from the DB. There is
        # currently no middle ground. If the subset is not fully cached, the DB
        # will be hit for everything.
        print('All records in cache:')
        async for record in table.get_record_subset('2023-01-02',
                                                    '2023-01-03'):
            print(record)
        print('Not all records in cache (will be loaded from DB):')
        async for record in table.get_record_subset('2023-01-01',
                                                    '2023-01-05'):
            print(record)
        # We can adjust the subset of records that should be cached. This will
        # cause old records to be discarded and new ones to be loaded.
        # Arguments are passed through to our subset's adjust() method. It is
        # up to the subset to decide which records are discarded and added.
        await table.adjust_cached_subset(
            prune_until_isoformat='2023-01-03',
            extend_until_isoformat='2023-01-05')
        print('Records we have expired from cache are loaded from the DB:')
        async for record in table.get_record_subset('2023-01-02',
                                                    '2023-01-03'):
            print(record)
        print('Newer records were added to the cache:')
        async for record in table.get_record_subset('2023-01-04',
                                                    '2023-01-05'):
            print(record)
        await redis_conn.close()


async def setup_example_dbs(redis_conn, postgres_pool):
    await redis_conn.flushall()
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
            (1, '2023-01-01T00:00:00Z'),
            (2, '2023-01-02T00:00:00Z'),
            (3, '2023-01-03T00:00:00Z'),
            (4, '2023-01-04T00:00:00Z');''')


# Our custom subset takes lower and upper timestamp bounds, and provides their
# respective epoch timestamps as lower and upper score bounds. It also defines
# a set of Postgres parameters that will yield the same records. It is up to
# the implementor to ensure these parameters and the score bounds match.
class TsSubset(tc.CachedSubset):
    def __init__(self, ge_isoformat: str, lt_isoformat: str) -> None:
        self._ge = datetime.datetime.fromisoformat(ge_isoformat)
        self._lt = datetime.datetime.fromisoformat(lt_isoformat)
        if self._ge > self._lt:
            raise ValueError('Bounds are not in order.')

    def __repr__(self):
        return (
            f'timestamp range from {self._ge.isoformat()} to '
            f'{self._lt.isoformat()}')

    # There is only one interval of scores for the values in Redis represented
    # by this subset.
    @property
    @t.override
    def score_intervals(self) -> ca.Iterable[tc.Interval]:
        return [tc.Interval(self._ge.timestamp(), self._lt.timestamp())]

    @property
    @t.override
    def db_args(self) -> tuple[numbers.Real, numbers.Real]:
        return (self._ge, self._lt)

    # This is the score function that extracts a (float) timestamp from a
    # record.
    @classmethod
    @t.override
    def record_score(cls, record: tp.Record) -> numbers.Real:
        return record['ts'].timestamp()

    @t.override
    def covers(self, other: t.Self) -> bool:
        return self._ge <= other._ge <= other._lt <= self._lt

    # The observe() method is a callback the CachedTable will call every time
    # it inserts a record into storage. This would allows us to keep track of
    # which records (and which scores) actually exist. We don't need this here,
    # but it will become useful in more advanced examples.
    @t.override
    def observe(self, record: tp.Record) -> None:
        pass

    # We're defining an adjust() method that allows us to discard outdated
    # records (up to some timestap) and load new ones. We have to adjust our
    # bounds, and return a tuple of score intervals to be deleted (just one in
    # our case) and a Subset instance of new records to be added.
    @t.override
    def adjust(
            self, *, prune_until_isoformat: str,
            extend_until_isoformat: str) -> tc.Adjustment[t.Self]:
        prune_until = datetime.datetime.fromisoformat(prune_until_isoformat)
        extend_until = datetime.datetime.fromisoformat(extend_until_isoformat)
        if prune_until < self._ge:
            raise ValueError(
                'New lower bound must not be lower than the current one.')
        if extend_until < self._lt:
            raise ValueError(
                'New upper bound must not be lower than the current one.')
        self._ge = prune_until
        old_lt, self._lt = self._lt, extend_until
        return tc.Adjustment(
            [tc.Interval(float('-inf'), self._ge.timestamp())],
            TsSubset(old_lt.isoformat(), self._lt.isoformat()))


if __name__ == '__main__':
    asyncio.run(main())
