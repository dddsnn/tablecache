# Redis storage

```{eval-rst}
.. automodule:: tablecache.redis
```

## Storage

```{py:data} tablecache.redis.AttributeCodecs
---
type: collections.abc.Mapping[str, tablecache.redis.Codec]
---
A mapping of attribute names to {py:class}`.Codec`s used to encode and decode
them.
```

```{eval-rst}
.. autoclass:: tablecache.redis.RedisCodingError
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.RedisTable
    :members:
    :show-inheritance:
```

## Codecs

```{eval-rst}
.. autoclass:: tablecache.redis.Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.Nullable
    :exclude-members: __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.Array
    :exclude-members: __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.BoolCodec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.StringCodec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.IntAsStringCodec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.FloatAsStringCodec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt8Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt16Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt32Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt64Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt8Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt16Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt32Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt64Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.Float32Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.Float64Codec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UuidCodec
    :exclude-members: __init__, __new__
```

```{eval-rst}
.. autoclass:: tablecache.redis.UtcDatetimeCodec
    :exclude-members: __init__, __new__
```
