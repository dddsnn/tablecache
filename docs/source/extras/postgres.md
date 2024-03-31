# Postgres DB access

```{eval-rst}
.. automodule:: tablecache.postgres
```

```{py:data} RecordParser
---
type: collections.abc.Callable[[asyncpg.Record], typing.Any]
---
A function parsing an {external:py:class}`asyncpg.Record` into a custom data
structure.
```

```{eval-rst}
.. autoclass:: tablecache.postgres.PostgresAccess
    :members:
    :show-inheritance:
```
