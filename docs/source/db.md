# DB access

```{eval-rst}
.. automodule:: tablecache.db
```

```{py:data} RecordParser[DbRecord, Record]
---
type: collections.abc.Callable[[DbRecord], Record]
---
A function parsing a record from the DB into a custom data structure.
```

```{eval-rst}
.. autoclass:: tablecache.DbRecordsSpec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.QueryArgsDbRecordsSpec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.DbAccess
    :members:
```
