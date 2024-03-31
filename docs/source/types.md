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

```{py:data} RecheckPredicate[Record]
---
type: collections.abc.Callable[[Record], bool]
---
Predicate operating on a record.
```
