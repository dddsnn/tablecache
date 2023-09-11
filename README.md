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
invalidated to be refreshed on the next request.

## Usage

The main components when using the library are a DB table abstraction
(`PostgresTable`), a storage table abstraction (`RedisTable`), and a
`CachedTable` tying the 2 ends together.

The storage needs to encode and decode the data (to/from bytes). This is done
via codecs. Some basic ones are provided (`tablecache.*Codec`).

Check out examples/users_cities.py for a quick start, which should be pretty
self-explanatory.

## Limitations

Currently, only Postgres is supported as DB, and only Redis as the fast
storage.

The library assumes that the query to be cached has a (single) column acting as
primary key, i.e. one which uniquely identifies a row in the result set of the
query.

At the moment, the Redis storage supports only one table, which takes up the
entire keyspace of the connected Redis instance.

## Logging

The library logs messages with logger names in the `tablecache` namespace.
