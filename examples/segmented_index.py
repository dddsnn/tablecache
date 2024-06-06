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
import itertools as it
import logging
import numbers
import typing as t

import asyncpg

import tablecache as tc
import tablecache.postgres as tcpg
import tablecache.local as tcl

POSTGRES_DSN = 'postgres://postgres:@localhost:5432/postgres'

# This example builds on time_range.py. We have timestamped device data again,
# but this time we want to query data for a specific set of devices only. To
# make this fast, we need to get both the device IDs and the timestamps into
# our index.

type Record = ca.Mapping[str, t.Any]


# With this Indexes implementation, we want to be able to address either all
# records in a time range using the time index (like in the time_range.py
# example), or only records by a select set of devices in a time range, using
# the device_and_time index. We'll only use (and support) the time index for
# loading and adjusting, and the device_and_time index for fetching.
class DeviceTimeRangeIndexes(tc.Indexes[Record, numbers.Real]):
    class IndexSpec(tc.Indexes[Record, tc.PrimaryKey].IndexSpec):
        def __init__(
                self, index_name: str, ge: datetime.datetime,
                lt: datetime.datetime, device_ids: frozenset[int] = None):
            super().__init__(index_name)
            self.ge = ge
            self.lt = lt
            if self.ge > self.lt:
                raise ValueError('Bounds are not in order.')
            self.device_ids = device_ids
            if index_name == 'time':
                if self.device_ids is not None:
                    raise ValueError(
                        'Can only specify device IDs with device_and_time '
                        'index.')
            elif index_name == 'device_and_time':
                if self.device_ids is None:
                    raise ValueError(
                        'Must specify device IDs with device_and_time index.')
            else:
                raise ValueError(f'Unknown index {index_name}.')

        def __repr__(self) -> str:
            all_records = f'all records between {self.ge} and {self.lt}'
            if self.index_name == 'time':
                return all_records
            return all_records + f' by devices {self.device_ids}'

    class Adjustment(tc.Adjustment[dict]):
        def __init__(
                self, expire_spec: t.Optional[tc.StorageRecordsSpec[dict]],
                load_spec: t.Optional[tc.DbRecordsSpec],
                ge: datetime.datetime, lt: datetime.datetime) -> None:
            super().__init__(expire_spec, load_spec)
            self.ge = ge
            self.lt = lt
            self.device_ids = set()

        def __repr__(self) -> str:
            all_records = (
                f'adjustment expiring {self.expire_spec} and loading records '
                f'with time between {self.ge.isoformat()} and '
                f'{self.lt.isoformat()}')
            if self.device_ids is None:
                return all_records
            return all_records + f' by devices {self.device_ids}'

        # This callback will be called with every loaded record while the
        # adjustment is being applied. We need it to find out which device IDs
        # exist.
        def observe_loaded(self, record: asyncpg.Record) -> None:
            self.device_ids.add(record['device_id'])

        # This callback will be called with every deleted record. We don't need
        # it here because we're always deleting everything and can just start
        # from scratch.
        def observe_expired(self, record: dict) -> None:
            pass

    # We're not taking configuration arguments this time and just hardcoding
    # all the attribute names for brevity.
    def __init__(self) -> None:
        self._ge = datetime.datetime.fromtimestamp(0)
        self._lt = datetime.datetime.fromtimestamp(0)
        self._device_ids = frozenset()

    @t.override
    @property
    def index_names(self) -> frozenset[str]:
        return frozenset(['time', 'device_and_time'])

    # The score for the time index is the epoch timestamp like in
    # time_range.py, but for the device_and_time index we create a 64-bit int
    # by shifting the 32 bits of the device ID left and adding the timestamp.
    # This essentially creates a namespace for every device in the number
    # domain, grouping the device's scores together.
    @t.override
    def score(self, index_name: str, record: Record) -> int:
        if index_name == 'time':
            return record['ts'].timestamp()
        elif index_name == 'device_and_time':
            return self._device_ts_score(record['device_id'], record['ts'])
        else:
            raise ValueError(f'Unknown index {index_name}.')

    @staticmethod
    def _device_ts_score(device_id: int, ts: datetime.datetime) -> int:
        return (device_id << 32) | int(ts.timestamp())

    # The primary key is actually made up of 2 attributes, the device ID and
    # sequence number. We can support this by returning a tuple.
    @t.override
    def primary_key(self, record: Record) -> tuple[int, int]:
        return (record['device_id'], record['data_seq'])

    # To get the records from storage, we specify a score for the requested
    # lower and upper bound for each of the devices that are requested and also
    # actually exist. We don't want to support accessing cache for all devices
    # (i.e. via the plain time index), so we raise an exception in that case.
    @t.override
    def storage_records_spec(
            self, spec: IndexSpec) -> tc.StorageRecordsSpec[dict]:
        if spec.index_name != 'device_and_time':
            raise tc.UnsupportedIndexOperation(
                'Can only access storage by device_and_time.')
        score_intervals = [
            tc.Interval(
                self._device_ts_score(device_id, spec.ge),
                self._device_ts_score(device_id, spec.lt))
            for device_id in spec.device_ids & self._device_ids]
        return tc.StorageRecordsSpec('device_and_time', score_intervals)

    @t.override
    def db_records_spec(
            self, spec: IndexSpec) -> tc.QueryArgsDbRecordsSpec:
        if spec.index_name == 'time':
            return tc.QueryArgsDbRecordsSpec(
                'SELECT * FROM device_data WHERE ts >= $1 AND ts < $2',
                (spec.ge, spec.lt))
        return tc.QueryArgsDbRecordsSpec(
            '''SELECT * FROM device_data
            WHERE ts >= $1 AND ts < $2 AND device_id = ANY($3)''',
            (spec.ge, spec.lt, spec.device_ids))

    # For simplicity, we delete everything and load everything again when we're
    # adjusting.
    @t.override
    def prepare_adjustment(self, spec: IndexSpec) -> Adjustment:
        expire_spec = tc.StorageRecordsSpec('time', [tc.Interval.everything()])
        load_spec = self.db_records_spec(spec)
        return self.Adjustment(expire_spec, load_spec, spec.ge, spec.lt)

    # When we're asked to commit an adjustment, we update our timestamp bounds
    # as usual, but we also have to copy over the device IDs the adjustment has
    # seen while data was loaded.
    @t.override
    def commit_adjustment(self, adjustment: Adjustment) -> None:
        self._ge = adjustment.ge
        self._lt = adjustment.lt
        self._device_ids = frozenset(adjustment.device_ids)

    # The coverage check is a little different from the one in time_range.py.
    # We always have all devices, so we still only need to check the timestamp
    # bounds. But we only want to support coverage checks by device_and_time,
    # to match the behavior of storage_records_spec() (it's no use knowing that
    # data is in storage if you can't get to it). Additionally, we're lying a
    # bit: We're claiming to cover 20 seconds more than we actually do. We're
    # doing this for fewer cache misses. When a request comes in with an upper
    # bound of right now, we'll still be able to serve it from cache, even if
    # our latest adjustment was up to 20 seconds ago. If we adjust regularly,
    # we can avoid cache misses altogether. Of course, this comes at the cost
    # of not returning records that have been added in the past 20 seconds.
    @t.override
    def covers(self, spec: IndexSpec) -> bool:
        if spec.index_name != 'device_and_time':
            raise tc.UnsupportedIndexOperation(
                'Can only check coverage by device_and_time.')
        fudged_lt = self._lt + datetime.timedelta(seconds=20)
        return self._ge <= spec.ge and fudged_lt >= spec.lt


async def main():
    indexes = DeviceTimeRangeIndexes()
    db_access = tcpg.PostgresAccess(dsn=POSTGRES_DSN)
    storage_table = LocalStorageTable(indexes, table_name='device_data')
    table = tc.CachedTable(indexes, db_access, storage_table)
    async with db_access, asyncpg.create_pool(POSTGRES_DSN) as postgres_pool:
        await setup_example_db(postgres_pool)
        await insert_data(postgres_pool, 1, 0, 11, minutes_ago(3))
        await insert_data(postgres_pool, 1, 1, 12, minutes_ago(2))
        await insert_data(postgres_pool, 1, 2, 13, minutes_ago(1))
        await insert_data(postgres_pool, 1, 3, 14, minutes_ago(0))
        await insert_data(postgres_pool, 2, 0, 22, minutes_ago(2))
        await insert_data(postgres_pool, 2, 1, 23, minutes_ago(1))
        await insert_data(postgres_pool, 2, 2, 24, minutes_ago(0))
        covers_ge = minutes_ago(2.1)
        await table.load('time', covers_ge, minutes_ago(0))
        print('Device 1, all records in cache:')
        await print_records(
            table, 'device_and_time', minutes_ago(2), minutes_ago(0), {1})
        print('Device 1, records from DB:')
        await print_records(
            table, 'device_and_time', minutes_ago(4), minutes_ago(0), {1})
        print('Both devices, all in cache:')
        await print_records(
            table, 'device_and_time', covers_ge, minutes_ago(0), {1, 2})
        # Now lets start a task that inserts a new data point for both devices
        # every 7 seconds, and one that adjusts the table to the past 2 minutes
        # every 15 seconds.
        asyncio.create_task(loop_add_data(postgres_pool))
        asyncio.create_task(loop_adjust(table))
        # Now that we adjust every 15 seconds, along with our lying covers()
        # implementation that serves records that are up to 20 seconds out of
        # date, we never miss the cache.
        while True:
            print('Both devices, past 2 minutes, all in cache:')
            await print_records(
                table, 'device_and_time', minutes_ago(2), minutes_ago(0),
                {1, 2})
            await asyncio.sleep(10)


async def setup_example_db(postgres_pool):
    await postgres_pool.execute(
        '''
        DROP SCHEMA public CASCADE;
        CREATE SCHEMA public;
        CREATE TABLE devices (device_id integer PRIMARY KEY);
        CREATE TABLE device_data (
            device_id integer NOT NULL REFERENCES devices(device_id),
            data_seq integer NOT NULL,
            data integer NOT NULL,
            ts timestamp without time zone NOT NULL,
            PRIMARY KEY(device_id, data_seq)
        );''')


async def insert_data(postgres_pool, device_id, data_seq, data, ts):
    await postgres_pool.execute(
        'INSERT INTO devices(device_id) VALUES ($1) ON CONFLICT DO NOTHING',
        device_id)
    await postgres_pool.execute('''
        INSERT INTO device_data(device_id, data_seq, data, ts)
        VALUES ($1, $2, $3, $4)''', device_id, data_seq, data, ts)


def minutes_ago(minutes):
    return datetime.datetime.now() - datetime.timedelta(minutes=minutes)


async def print_records(table, *args, **kwargs):
    async for record in table.get_records(*args, **kwargs):
        print(record)


async def loop_add_data(postgres_pool):
    for i in it.count(20):
        await insert_data(postgres_pool, 1, i, i, minutes_ago(0))
        await insert_data(postgres_pool, 2, i, i + 10, minutes_ago(0))
        await asyncio.sleep(7)


async def loop_adjust(table):
    while True:
        await table.adjust('time', minutes_ago(2), minutes_ago(0))
        await asyncio.sleep(15)


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
    import prometheus_client as pc
    pc.start_http_server(9050)
    asyncio.run(main())
