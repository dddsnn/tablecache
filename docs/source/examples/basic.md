# Basic

This is an example that demonstrates the basic setup of a cache. It
demonstrates:

- how to create an instance of an existing {py:class}`.Indexes` implementation
- how to create a {py:class}`.CachedTable` backed by a
  {py:class}`tablecache.postgres.PostgresAccess` and a
  {py:class}`tablecache.local.LocalStorageTable`
- how to load records into it
- how to fetch records
- how to invalidate records that have changed in the underlying DB
- how to manually trigger a refresh of invalid records

The source file can be found in [the repository's `examples` directory](
    https://github.com/dddsnn/tablecache/blob/main/examples/). To run
it, you need a running Postgres, which can be started by `up`ping the
`docker-compose.yml` found in the same directory.

```{literalinclude} ../../../examples/basic.py
---
start-at: import
```
