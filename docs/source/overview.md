# Overview

## Installation

```shell
pip install tablecache[extras,of,your,choice]
```

### Extras

`tablecache` comes with a few optional extras that pull in additional
dependencies. Without any, there are no concrete implementations of DB and
storage access, so you probably want at least 2 of these.

- `postgres`: Adds submodule {py:mod}`.postgres` which provides a
  {py:class}`.DbAccess` implementation for Postgres using
  {external:py:mod}`asyncpg`
- `local`: Adds submodule {py:mod}`.local` which provides a
  {py:class}`.StorageTable` implementation storing records in local data
  structures (i.e. in Python)
- `redis`: Adds submodule {py:mod}`.redis` which provides a
  {py:class}`.StorageTable` implementation storing records in Redis
- `test`: extra dependencies for testing `tablecache`
- `dev`: extra dependencies for developing `tablecache`
- `docs`: extra dependencies to build this documentation

## Purpose

Suppose you have a relational database that's nice and normalized (many
tables), and you need to access the result of a large join. The DB can't
combine its indexes on the individual tables in a way that makes querying the
join fast. But there would be a performant way to access ranges of records, if
only you could easily materialize the join and then index that directly.

`tablecache` can take your big query and put the denormalized results, or a
subset of them, in faster
storage. You can define one or more ways to index these records. The classic
example is timestamped data, where you want to keep the past n days in cache,
and be able to quickly get records from a time range.

The contents of the cache can be adjusted, i.e. old ones expired and new ones
loaded. Any records not cached will be transparently fetched from the DB, so
manual queries against the DB after a cache miss are not necessary.

Cached records can also be invalidated. This needs to be done when the data in
the DB changes in order for the cache to continue reflecting the DB state. But
the cache only needs to be told which records have changed, the actual refresh
is done automatically.
