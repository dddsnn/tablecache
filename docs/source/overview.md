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
subset of them, in faster storage. You can define one or more ways to index
these records for fast access to whole ranges of them. The classic example is
timestamped data, where you want to keep the past few days in cache, and be
able to quickly get records from a time range.

The contents of the cache can be adjusted, i.e. old ones expired and new ones
loaded. Any records not cached will be transparently fetched from the DB, so
manual queries against the DB after a cache miss are not necessary.

Cached records can also be invalidated. This needs to be done when the data in
the DB changes in order for the cache to continue reflecting the DB state. But
the cache only needs to be told which records have changed, the actual refresh
is done automatically.

## Non-goals

`tablecache` is not a database, only a performance layer for
difficult-to-index result sets. It reflects the underlying DB which is the
single source of truth. The storage used for caching is meant to be disposable.
You're supposed to be able to delete it all and deploy a new instance which
loads the cache fresh from the DB.

The DB is not actively watched for changes. Once loaded, without any further
interaction the cache will continue to return the same records, even if they
change in the DB. It will load new records and refresh changed ones, but it
needs to be told which ones (not _how_ they've changed, it will go to the DB
for that).

`tablecache` doesn't implement caching strategies, i.e. it doesn't decide which
records should be in cache, this needs to be done externally. But again, once
that decision has been made, records are evicted and loaded automatically.

`tablecache` is not a high-level tool that figures out how to index and query
your data based on a declarative description (like a relational DB), and it's
not a low-level tool that gives you full control over how data is managed (like
a use-case-specific custom cache). It is intended to sit in the middle of the
abstraction spectrum, requiring the user to decide how data is accessed and
indexed (by providing an {py:class}`.Indexes` implementation), and which
records should be kept in cache in the first place, while taking over some of
the tedious tasks like transparently handling cache misses by querying the DB
and keeping track of invalid records.

## Limitations

Each record must be uniquely identifiable by a primary key. These can be
anything hashable though and are calculated in the user-supplied implementation
of {py:meth}`Indexes.primary_key() <.RecordScorer.primary_key>`, so multicolumn
primary keys are possible by e.g. returning tuples.

Indexing is done by associating each record with one or more scores (one per
index) and storing them in a sorted data structure that makes it fast
to access ranges of scores. Sets of records can be accessed quickly if they
have similar scores, e.g. it's possible to index a timestamp and then get all
records in a time range. Indexing more than one attribute is also possible by
interleaving scores, but requires some tweaking. However, only one index can be
used per read operation, you cannot combine multiple ones.

Currently, everything `tablecache` does is single-threaded and not thread-safe.
This implies that each instance of {py:class}`.CachedTable` owns its storage
exclusively, and multiple instances will each need their own copy of the data.
A feature where one instance manages the storage while others access it
read-only as read replicas is feasible, but not supported.

`tablecache` is designed with `asyncio` in mind. Using traditional blocking IO
libraries may not work well.

## Usage

In order to use `tablecache`, you need to

- create a {py:class}`.DbAccess`, e.g. a {py:class}`.PostgresAccess`
- create a {py:class}`.StorageTable`, e.g. a {py:class}`.LocalStorageTable`
- implement {py:class}`.Indexes`
- put them all together in a {py:class}`.CachedTable`

Then you can {py:meth}`.CachedTable.load` your table,
{py:meth}`.CachedTable.get_records` from it using the indexes you defined,
{py:meth}`.CachedTable.adjust` it to change which records are cached, and
{py:meth}`.CachedTable.invalidate_records` to inform the cache of changes in
the underlying data.

See also the {doc}`examples <examples/basic>` for a guide on how to put this all
together.

### Indexes

Your {py:class}`.Indexes` implementation is where you define your indexes and
tie them all together. An instance of the class is also used to keep track of
the records that are currently in cache.

You need to define

- {py:class}`.Indexes.IndexSpec`: A specification of how to query a particular
  one of your indexes. Must be a subclass of {py:class}`.Indexes.IndexSpec` and
  an inner class of your {py:class}`.Indexes`. You need to add all the data
  required for a query.
- {py:attr}`Indexes.index_names <.RecordScorer.index_names>`: A property
  returning a set of available index names.
- {py:meth}`Indexes.score() <.RecordScorer.score>`: A method that calculates
  the score of a record for a given index.
- {py:meth}`Indexes.primary_key() <.RecordScorer.primary_key>`: A method that
  extracts the primary key from a record.
- {py:meth}`.Indexes.storage_records_spec`: A method that takes an
  {py:class}`IndexSpec <.Indexes.IndexSpec>` and returns a
  {py:class}`.StorageRecordsSpec`, a way to specify a set of records in
  storage.
- {py:meth}`.Indexes.db_records_spec`: Like
  {py:meth}`.Indexes.storage_records_spec`, but returning a
  {py:class}`.DbRecordsSpec`, a way to specify the same set of records in the
  DB.
- {py:meth}`.Indexes.prepare_adjustment`: A method that takes an
  {py:class}`IndexSpec <.Indexes.IndexSpec>` and returns an
  {py:class}`Adjustment <tablecache.Adjustment>`, which contains information on
  which records to expire from and load into the cache in order to attain the
  state specified in the {py:class}`IndexSpec <.Indexes.IndexSpec>`. You can
  return your own {py:class}`Adjustment <tablecache.Adjustment>` subclass to
  include extra data.
- {py:meth}`.Indexes.commit_adjustment`: A method that takes an
  {py:class}`Adjustment <tablecache.Adjustment>` previously returned by
  {py:meth}`prepare_adjustment <.Indexes.prepare_adjustment>` and commits the
  changes. The {py:class}`Adjustment <tablecache.Adjustment>` will have had
  callbacks called for expired and new records, through which you can track
  which records are now cached.
- {py:meth}`.Indexes.covers`: A method that takes an
  {py:class}`IndexSpec <.Indexes.IndexSpec>` and returns whether all the
  records specified are currently available in cache.

### Accessing and updating data

Before anything can happen, you need to {py:meth}`load <.CachedTable.load>`
your {py:class}`.CachedTable`. This method, along with {py:meth}`get_records
<.CachedTable.get_records>`, {py:meth}`get_first_record
<.CachedTable.get_first_record>` and {py:meth}`adjust <.CachedTable.adjust>`,
takes one of your {py:class}`IndexSpec <.Indexes.IndexSpec>`s as an argument.
As a convenience, these methods also take arbitrary args and kwargs, which will
be passed to the {py:class}`IndexSpec <.Indexes.IndexSpec>` constructor to
create one.

Records can be fetched with {py:meth}`.CachedTable.get_records` or
{py:meth}`.CachedTable.get_first_record` (which is just a convenience wrapper
around the former). Whenever the specified records are available in cache
(according to {py:meth}`.Indexes.covers`) and haven't been invalidated, they
are fetched from cache. Otherwise, they are fetched from the DB.

{py:meth}`.CachedTable.adjust` can be used to change the set of records that
are kept in storage. This will internally call
{py:meth}`.Indexes.prepare_adjustment` for specs on the records to expire and
load, perform the changes while calling the
{py:class}`Adjustment <tablecache.Adjustment>`'s
{py:meth}`observe_expired <tablecache.Adjustment.observe_expired>` and
{py:meth}`observe_loaded <tablecache.Adjustment.observe_loaded>` callbacks, and
then commit them using {py:meth}`.Indexes.commit_adjustment`. Afterwards,
{py:meth}`.Indexes.covers` should reflect the new state. Adjustments are done
without blocking read operations, while providing a consistent view of the
data.

```{note}
{py:meth}`.CachedTable.load` also uses the adjustment mechanism
({py:meth}`.Indexes.prepare_adjustment` etc.), so that the {py:class}`.Indexes`
can observe all the records that are initially loaded.
```

{py:meth}`.CachedTable.invalidate_records` can be used to inform the cache that
the data in the underlying DB has changed. By default, records that are
invalidated are guaranteed to be fetched from the DB before they are returned
the next time. This refresh is done lazily, i.e. only when a request comes in
for a record that has been invalidated (requests for records that are still
valid are served without refresh). This default behavior can be overridden by
passing `refresh_automatically=False`, in which case fetches will continue to
serve the invalid records until a manual refresh is triggered. This can be
useful when immediate correctness isn't crucial, but avoiding refreshes
blocking read operations is. Invalid records can be refreshed manually using
{py:meth}`.CachedTable.refresh_invalid`. This is also possible when
`refresh_automatically=True`.

{py:meth}`.CachedTable.invalidate_records` is a bit more complex since it takes
more than one {py:class}`IndexSpec <.Indexes.IndexSpec>`. That's because
updates to records may change them in a way that also changes their scores in a
particular index. So you have to actually specify how to find the old records
currently in the cache, and how to find the new records in the DB (these
{py:class}`IndexSpec <.Indexes.IndexSpec>`s may be the same, but don't have to
be). Additionally, you may specify multiple
{py:class}`IndexSpec <.Indexes.IndexSpec>`s each for old and new records, one
for each of your indexes. This gives the cache the information whether a record
that is requested is invalid and a refresh is necessary before serving a read.
Without this, everything still works correctly, but indexes without information
are marked as dirty and will unconditionally trigger a refresh on the next read
against them.

```{note}
All records specified in {py:meth}`.CachedTable.invalidate_records` must be
present in cache (according to {py:meth}`.Indexes.covers`), or a
{py:exc}`ValueError` is raised.
{py:meth}`invalidate_records <.CachedTable.invalidate_records>` is only meant
to update records that were within the range of the indexes and have changed,
not add completely new ones. While it will work when the new records' scores
are all in the covered range, the adjustment mechanism (i.e.
{py:meth}`.Indexes.prepare_adjustment` etc.) is not used and the
{py:class}`.Indexes` will not be informed of the new records. This may or may
not be ok, depending on your implementation.
```

## Available implementations

These implementations of {py:class}`.DbAccess` and {py:class}`.StorageTable`
are available as submodules when selecting the appropriate extras:

### {py:class}`.DbAccess`: {py:mod}`tablecache.postgres`

Simple Postgres access available with the `postgres` extra. Uses
{external:py:mod}`asyncpg` and specifies records via a query string and an args
tuple.

### {py:class}`.StorageTable`: {py:mod}`tablecache.local`

A {py:class}`.StorageTable` implementation storing records in local Python data
structures, available with the `local` extra. Uses
{external:py:mod}`sortedcontainers <sortedcontainers.sortedlist>` for indexes.

This implementation is probably the better choice over the Redis
implementation. Having the data in Python makes it possible to just put
references into the index lists, meaning fewer indirections. This
implementation also supports any kind of number as scores, including
arbitrarily large integers.

### {py:class}`.StorageTable`: {py:mod}`tablecache.redis`

A {py:class}`.StorageTable` implementation storing records in a Redis instance,
available with the `redis` extra. Uses
{external:py:class}`redis.asyncio <redis.asyncio.connection.Connection>`.

Scores must be representible as 64-bit floats (Redis' sorted set is used).
Records are stored in Redis as byte strings, which means they must be encoded
using {py:class}`.Codec`s that come with the module. The Redis instance backing
the cache must be configured to not expire keys (this is the default), or data
will be lost.

## Logging

The library logs messages with logger names in the `tablecache` namespace.
