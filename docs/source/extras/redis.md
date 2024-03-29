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
```

## Codecs

```{eval-rst}
.. autoclass:: tablecache.redis.Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.Nullable
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.Array
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.BoolCodec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.StringCodec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.IntAsStringCodec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.FloatAsStringCodec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt8Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt16Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt32Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.SignedInt64Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt8Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt16Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt32Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UnsignedInt64Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.Float32Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.Float64Codec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UuidCodec
    :members:
```

```{eval-rst}
.. autoclass:: tablecache.redis.UtcDatetimeCodec
    :members:
```
