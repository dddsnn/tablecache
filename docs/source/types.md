# Types

These are some globally useful type aliases.

```{py:data} PrimaryKey
---
type: collections.abc.Hashable
---
Base type of possible primary keys.
```

```{py:data} Score
---
type: numbers.Real
---
Base type of possible scores.
```

```{py:data} Record
---
type: collections.abc.Mapping[str, t.Any]
---
Base record type.
```

```{py:data} AsyncRecords
---
type: collections.abc.AsyncIterator[Record]
---
Asynchronous iterator over Records.
```

```{py:data} RecheckPredicate
---
type: collections.abc.Callable[[Record], bool]
---
Predicate operating on a record.
```
