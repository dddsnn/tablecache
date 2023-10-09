# tablecache

Simple cache for unwieldily joined relations.

## Copyright and license

Copyright 2023 Marc Lehmann

This file is part of tablecache.

tablecache is free software: you can redistribute it and/or modify it under the
terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version.

tablecache is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License along
with tablecache. If not, see <https://www.gnu.org/licenses/>.

## Purpose

tablecache is a small library that caches tables in a slow database (or, more
likely, big joins of many tables) in a faster storage.

Suppose you have a relational database that's nice and normalized (many
tables), but you also need fast access to data resulting from joining a lot of
these tables to display somewhere.

tablecache can take your big query, and put the denormalized results in faster
storage. When data is updated in the DB, the corresponding key in cache can be
invalidated to be refreshed on the next request. The cache supports getting
individual records by primary key, as well as getting an entire subset of all
records defined via a custom `Subset` class (see below).

## Limitations

Currently, only Postgres is supported as DB, and only Redis as the fast
storage.

The library assumes that the query to be cached has a (single) column acting as
primary key, i.e. one which uniquely identifies a row in the result set of the
query.

## Usage

The main components when using the library are a DB table abstraction
(`PostgresTable`), a storage table abstraction (`RedisTable`), and a
`CachedTable` tying the 2 ends together.

The storage needs to encode and decode the data (to/from bytes). This is done
via codecs. Some basic ones are provided (`tablecache.*Codec`).

Records in the `CachedTable` can be accessed individually by primary key using
`get_record()`, or as part of a subset via `get_record_subset()` (see below for
`Subset`). If a record doesn't exist in fast storage, it is transparently
fetched from the DB. Note though that, if only some records in a subset are in
storage, all are queried from the DB.

If records change in the DB, the `CachedTable` has to be informed via
`invalidate_record()`. Invalidated records are lazily refreshed when they're
requested the next time.

Check out [examples/basic.py](examples/basic.py) for a quick start, which
should be pretty self-explanatory. There are more examples showing off advanced
functionality. There is a `docker-compose.yml` in the directory which starts
the Postgres and Redis instance needed for the examples.

### Redis

The Redis instance backing the cache must be configured to not expire keys
(this is the default), or data will be lost.

To enable fast access of subsets of records, data is stored in a sorted set
with a (float) score that allows fast range queries. The score is calculated by
the `CachedSubset` implementation. When primary keys are numbers, it's
perfectly legal to use the primary key itself as the score
(`tablecache.NumberRangeSubset.with_primary_key('name_of_the_primary_key')`
returns a `CachedSubset` subclass that can be used). If the primary key is not
a numerical, using its hash may be a good option.

The classical example for a more meaningful use of the score is timeseries
data, where each record has a timestamp field (separate from the primary key)
and you need to quickly get all records in a time range. The score could be the
epoch timestamp in this case.

It's fine for different records to have the same score, the cache will tell
them apart by their unique primary key. Some care should be taken when defining
the score to avoid unnecessary collisions though. When looking up records by
primary key, all those with the same score are searched linearly for the one
with the matching key.

### Subsets

Subsets are the way to interact with more than one record at a time.
`tablecache.Subset` is an abstract base that allows the cache to get a subset
of all existing records either from the DB via a tuple of query parameters, or
from Redis via a list of score intervals. It is up to the implementor of the
subset to ensure that these actually match (i.e. that querying the DB with the
subset's DB parameters yields the same records as querying Redis with the score
intervals).

`tablecache.CachedSubset` extends it to allow a `CachedTable` to keep track of
which records it's supposed to keep in fast storage, and which score ranges
actually exist. `CachedSubset` implementations can offer a way adjust which
records should be cached via `adjust()`, which tells the `CachedTable` which
records to expire and which new ones to load. This can be used to e.g. expire
old timeseries data while loading the latest.
[examples/custom_subset.py](examples/custom_subset.py) and
[examples/segemented_subset.py](examples/segemented_subset.py) show how this is
done in practice.

Note that, while `Subset` and `CachedSubset` are separated for clarity, the
former doesn't need a separate implementation as it's just a superclass.

## Logging

The library logs messages with logger names in the `tablecache` namespace.
