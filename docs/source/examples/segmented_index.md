# Segmented index

This is an example building on {doc}`time_range` in which we index 2 attributes
in order to access records for specific devices in a specific time range. It
demonstrates:

- how a score function can index more than one attribute
- how to use a composite primary key
- how to observe new records during an adjustment
- how to define more than one index
- how to be creative with the truth in {py:meth}`.Indexes.covers` to avoid
  cache misses
- how to continuously adjust the storage to always have the latest data in
  cache

The source file can be found in [the repository's `examples` directory](
    https://github.com/dddsnn/tablecache/blob/main/examples/). To run
it, you need a running Postgres, which can be started by `up`ping the
`docker-compose.yml` found in the same directory.

```{literalinclude} ../../../examples/segmented_index.py
---
start-at: import
```
