# Redis storage

This is the same example as {doc}`basic`, but with a {py:class}`.RedisTable`
instead of a {py:class}`.LocalStorageTable`. It demonstrates the peculiarities
of using Redis, which mostly revolve around defining the {py:class}`.Codec` s
that allow the table to store records.

The source file can be found in [the repository's `examples` directory](
    https://github.com/dddsnn/tablecache/blob/main/examples/). To run
it, you need a running Postgres and Redis, which can be started by `up`ping the
`docker-compose.yml` found in the same directory.

```{literalinclude} ../../../examples/redis_storage.py
---
start-at: import
```
