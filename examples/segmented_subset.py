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
import math
import numbers
import pathlib
import sys
import typing as t

sys.path.append(str(pathlib.Path(__file__).parent.parent))

import asyncpg
import redis.asyncio as redis

import tablecache as tc
import tablecache.types as tp


# This example builds on custom_subset.py, but here each row of data has a
# device ID, and we want to be able to quickly query data from a time range,
# but only for a specific device. Since the Redis backend requires that each
# record have exactly one (float) score, our CachedSubset implementation has to
# munge both device ID and timestamp into one number.
async def main():
    base_query_string = 'SELECT * FROM device_data'
    # The query_subset_string needs to have parameters $1 etc. that match the
    # parameters of our custom subset. In our case, we want to filter by
    # timestamp, and optionally by device_id (we want all devices when loading
    # the cache, but only a specific one when we have a cache miss). We achieve
    # the optional filtering by setting $3 to true or false to get all or only
    # one specific device, respectively.
    query_subset_string = f'''{base_query_string} WHERE ts >= $1 AND ts < $2
        AND ($3 OR device_id = $4)'''
    query_pks_string = f'{base_query_string} WHERE device_id = ANY ($1)'
    postgres_pool = asyncpg.create_pool(
        min_size=0, max_size=1,
        dsn='postgres://postgres:@localhost:5432/postgres')
    redis_conn = redis.Redis()
    db_table = tc.PostgresTable(
        postgres_pool, query_subset_string, query_pks_string)
    table = tc.CachedTable(
        DeviceTsSubset,
        db_table,
        primary_key_name='data_id',
        attribute_codecs={
            'data_id': tc.IntAsStringCodec(),
            'device_id': tc.IntAsStringCodec(),
            'data': tc.IntAsStringCodec(),
            'ts': tc.UtcDatetimeCodec(),},
        redis_conn=redis_conn,
        redis_table_name='device_data',
    )
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(postgres_pool)
        await setup_example_dbs(redis_conn, postgres_pool)
        # When loading, we set the device ID to None, which causes our subset
        # implementation to load data from all devices in the time range.
        await table.load(None, '2023-01-01T12:00:00', '2023-01-03T12:00:00')
        # Now that the cache is loaded, we can query it by subset. Depending on
        # whether the subset we query for is contained in the cache, we'll
        # either get all records from cache, or all from the DB. There is
        # currently no middle ground. If the subset is not fully cached, the DB
        # will be hit for everything.
        print('Device 1, all records in cache:')
        async for record in table.get_record_subset(1, '2023-01-02',
                                                    '2023-01-03'):
            print(record)
        print('Device 1, not all records in cache (will be loaded from DB):')
        async for record in table.get_record_subset(1, '2023-01-01',
                                                    '2023-01-05'):
            print(record)
        print('Device 2, all records in cache:')
        async for record in table.get_record_subset(2, '2023-01-02',
                                                    '2023-01-03'):
            print(record)
        print('Device 2, not all records in cache (will be loaded from DB):')
        async for record in table.get_record_subset(2, '2023-01-01',
                                                    '2023-01-05'):
            print(record)
        # The adjust() method in our subset implementation can only prune away
        # old records, and only for all devices at once.
        await table.adjust_cached_subset(prune_until_isoformat='2023-01-03')
        print('Records we have expired from cache are loaded from the DB:')
        async for record in table.get_record_subset(1, '2023-01-02',
                                                    '2023-01-03'):
            print(record)
        await redis_conn.close()


async def setup_example_dbs(redis_conn, postgres_pool):
    await redis_conn.flushall()
    await postgres_pool.execute(
        '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE devices (
            device_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY);
        CREATE TABLE device_data (
            data_id integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            device_id integer NOT NULL REFERENCES devices(device_id),
            data integer NOT NULL,
            ts timestamp without time zone NOT NULL
        );

        INSERT INTO devices(device_id) VALUES (1), (2);
        INSERT INTO device_data(device_id, data, ts) VALUES
            (1,  11, '2023-01-01T00:00:00Z'),
            (1,  12, '2023-01-02T00:00:00Z'),
            (1,  13, '2023-01-03T00:00:00Z'),
            (1,  14, '2023-01-04T00:00:00Z'),
            (2,  22, '2023-01-02T00:00:00Z'),
            (2,  23, '2023-01-03T00:00:00Z'),
            (2,  24, '2023-01-04T00:00:00Z');''')


# Our custom subset takes a device ID as well as lower and upper timestamp
# bounds, and provides lower and upper score bounds for it using its
# _device_ts_score (see below). When Redis is queried with this subset (via the
# CachedTable), it will yield all the records for the specified device in the
# specified time frame.
#
# The subset also defines a set of Postgres parameters that will yield the same
# records. It is up to the implementor to ensure these parameters and the score
# bounds match.
#
# The subset accepts a device ID of None, but this only works when querying
# Postgres where we can specify more than one filter in the query (useful when
# loading the cache, where we want all devices). In Redis, we have only the
# score to filter by, and our score function has locked us into the
# segmentation of the data by device ID.
class DeviceTsSubset(tc.CachedSubset):
    def __init__(
            self, device_id: t.Optional[int], ge_isoformat: str,
            lt_isoformat: str) -> None:
        self._device_id = device_id
        self._ge = datetime.datetime.fromisoformat(ge_isoformat)
        self._lt = datetime.datetime.fromisoformat(lt_isoformat)
        if self._ge > self._lt:
            raise ValueError('Bounds are not in order.')
        self._observed_scores = None

    def __repr__(self):
        if self._device_id is None:
            device = 'all devices'
        else:
            device = f'device with ID {self._device_id}'
        return (
            f'timestamp range for {device} from {self._ge.isoformat()} to '
            f'{self._lt.isoformat()}')

    # This function combines device ID and timestamp into a single float score.
    # It takes the device ID as the integer part, and adds the epoch timestamp
    # divided by 10**10. It essentially creates a namespace for each device in
    # the interval [device_id, device_id+1[ (i.e. the upper bound is
    # exclusive). Since double-precision floats give us 15 decimal places of
    # precision, we can have seconds-precision timestamps as long as our device
    # IDs are less than 10**5.
    @staticmethod
    def _device_ts_score(
            device_id: int, ts: datetime.datetime) -> numbers.Real:
        return device_id + (ts.timestamp() / 10_000_000_000)

    @property
    @t.override
    def score_intervals(self) -> ca.Iterable[tc.Interval]:
        if self._device_id is None:
            raise ValueError('Can\'t represent scores for all devices.')
        if self._observed_scores is None:
            return [
                tc.Interval(
                    self._device_ts_score(self._device_id, self._ge),
                    self._device_ts_score(self._device_id, self._lt))]
        yield from self._observed_scores.values()

    @property
    @t.override
    def db_args(
            self) -> tuple[numbers.Real, numbers.Real, bool, t.Optional[int]]:
        return (self._ge, self._lt, self._device_id is None, self._device_id)

    @classmethod
    @t.override
    def record_score(cls, record: tp.Record) -> numbers.Real:
        device_id = record['device_id']
        ts = record['ts']
        return cls._device_ts_score(device_id, ts)

    @t.override
    def covers(self, other: t.Self) -> bool:
        covers_device_id = (
            self._device_id is None or self._device_id == other._device_id)
        covers_time = self._ge <= other._ge <= other._lt <= self._lt
        return covers_device_id and covers_time

    # The subset is used by the CachedTable to keep track of which records
    # actually exist in Redis. To do this, it calls the observe() callback
    # everytime it inserts a record. We're using this here to basically keep
    # track of which devices even exist, so when we have to generate the
    # score_intervals property, we don't have to go through intervals for all
    # possible device IDs, but only those that actually exist.
    @t.override
    def observe(self, record: tp.Record) -> None:
        self._observed_scores = self._observed_scores or {}
        device_id = record['device_id']
        score = self.record_score(record)
        try:
            current = self._observed_scores[device_id]
            self._observed_scores[device_id] = tc.Interval(
                min(current.ge, score), max(current.lt, score))
        except KeyError:
            # This is the first record we're observing for this device. We're
            # adding the smallest possible bit to the upper bound, so the
            # interval isn't empty.
            self._observed_scores[device_id] = tc.Interval(
                score, math.nextafter(score, math.inf))

    # Adjusting the interval in this example only supports pruning from the
    # left side (extending to the right would be possible though). There is
    # some handling for special cases, but the main part is that we're going
    # through all the observed score intervals by device ID, pruning it from
    # the left, and add the interval that needs to be deleted in Redis to a
    # list that we return in the end.
    @t.override
    def adjust(
            self, *,
            prune_until_isoformat: datetime.datetime) -> tc.Adjustment[t.Self]:
        prune_until = datetime.datetime.fromisoformat(prune_until_isoformat)
        if prune_until < self._ge:
            raise ValueError(
                'New lower bound must not be lower than the current one.')
        if self._device_id is not None:
            raise ValueError('Can only adjust a subset covering all devices.')
        if self._observed_scores is None:
            # No record has ever been observed, no need to expire anything.
            # We're not supporting loading new records here, so use an empty
            # interval for the new records.
            return ([], DeviceTsSubset(None, '1970-01-01', '1970-01-01'))
        if prune_until >= self._lt:
            # We're pruning everything, so expire all observed intervals and
            # load nothing new.
            intervals = self._observed_scores.values()
            self._observed_scores.clear()
            self._ge = self._lt
            return (
                intervals, DeviceTsSubset(None, '1970-01-01', '1970-01-01'))
        intersection_intervals = []
        for device_id, current in list(self._observed_scores.items()):
            new_ge = self._device_ts_score(device_id, prune_until)
            intersection_intervals.append(
                tc.Interval(
                    self._device_ts_score(
                        device_id, datetime.datetime.fromtimestamp(0)),
                    new_ge))
            if new_ge >= current.lt:
                del self._observed_scores[device_id]
            else:
                self._observed_scores[device_id] = tc.Interval(
                    new_ge, current.lt)
        self._ge = prune_until
        # Since we're not supporting extending the subset here, we just return
        # an empty subset as the subset of new records to load.
        return tc.Adjustment(
            intersection_intervals,
            DeviceTsSubset(None, '1970-01-01', '1970-01-01'))


if __name__ == '__main__':
    asyncio.run(main())
