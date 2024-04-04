# Time ranges

This is an example that demonstrates how to index timestamped data for fast
access to time ranges with a custom {py:class}`.Indexes` implementation. It
demonstrates:

- how to write a custom {py:class}`.Indexes` implementation
- how to load only a subset of records into storage
- how records that aren't in storage will be fetched from the DB
- how to adjust the data covered by storage
- how to invalidate records whose scores have changed
- how to disable automatic refreshes to avoid slowing down reads

The source file can be found in [the repository's `examples` directory](
    https://github.com/dddsnn/tablecache/blob/main/examples/). To run
it, you need a running Postgres, which can be started by `up`ping the
`docker-compose.yml` found in the same directory.

```{literalinclude} ../../../examples/time_range.py
---
start-at: import
```
