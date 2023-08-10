# Copyright 2023 Marc Lehmann

# This file is part of tablecache.
#
# tablecache is free software: you can redistribute it and/or modify it under
# the terms of the GNU Affero General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# tablecache is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
# details.
#
# You should have received a copy of the GNU Affero General Public License
# along with tablecache. If not, see <https://www.gnu.org/licenses/>.

import abc
import functools
import typing as t

import redis.asyncio as redis


class CodingError(Exception):
    """
    Raised when any error relating to en- or decoding occurs.
    """


class RedisStorage:
    """
    Redis connection proxy.

    Provides a connection to clients. Connects on async context manager enter,
    disconnects on exit.
    """
    def __init__(self, **connect_kwargs: t.Mapping[str, t.Any]) -> None:
        """
        :param connect_kwargs: Keyword arguments that will be passed to
            redis.asyncio.Redis().
        """
        self._conn_factory = functools.partial(redis.Redis, **connect_kwargs)

    async def __aenter__(self):
        self._conn = self._conn_factory()
        return self

    async def __aexit__(self, *_):
        await self._conn.close()
        del self._conn
        return False

    @property
    def conn(self) -> redis.Redis:
        try:
            return self._conn
        except AttributeError as e:
            raise AttributeError(
                'You have to connect the storage before using it.') from e


class StorageTable(abc.ABC):
    @abc.abstractmethod
    async def clear(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def put(self, record: t.Mapping[str, t.Any]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, record_key: t.Any) -> t.Mapping[str, t.Any]:
        raise NotImplementedError


class RedisTable(StorageTable):
    def __init__(
            self, redis_storage: RedisStorage, *, primary_key_name: str,
            encoders: t.Mapping[str, t.Callable[[t.Any], bytes]],
            decoders: t.Mapping[str, t.Callable[[t.Any], bytes]],
            primary_key_encoder: t.Callable[[t.Any], str] = str) -> None:
        """
        A table stored in Redis.

        Enables storage and retrieval of records in Redis. Records must be
        dict-like, with string keys.

        Records are stored in Redis as hashes. Each must have a primary key,
        which is used as the name of the hash. Hash keys and values are stored
        as bytes objects, encoded and decoded via the specified encoders and
        decoders. Only attributes for which en- and decoders exist are stored.

        However, the name of a hash must be a string, which is why there is a
        separate primary_key_encoder.

        The table owns the entire namespace of the connection it is given. In
        particular, calls to clear() delete everything in the current database.

        :param redis_storage: A RedisStorage that provides a connection.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in encoders and decoders.
        :param encoders: Dictionary of encoders for record attributes. Must map
            attribute names (string) to a function that takes the corresponding
            attribute and returns a bytes object representing it. Only
            attributes present here are stored. Must have the same keys as
            decoders.
        :param decoders: Dictionary of decoders for attributes stored in Redis.
            Must contain the same keys as encoders, and map to the
            corresponding inverse function.
        :primary_key_encoder: Function encoding the primary key as a string.
            Must be unique (i.e. 2 different primary keys must map to 2
            different encodings). For common primary key types (like int and
            str), the default str works fine, however more complex types may
            need repr or a custom function.
        """
        if set(encoders) != set(decoders):
            raise ValueError(
                'Encoders and decoders must specify the same attributes.')
        for attribute_name in encoders:
            if not isinstance(attribute_name, str):
                raise ValueError('Attribute names must be strings.')
        if primary_key_name not in encoders:
            raise ValueError(
                'Primary key attribute is missing from encoders and decoders.')
        self._storage = redis_storage
        self._primary_key_name = primary_key_name
        self._encoders = encoders
        self._decoders = decoders
        self._primary_key_encoder = primary_key_encoder

    async def clear(self) -> None:
        """
        Delete all data belonging to this table.

        Flushes the entire current database.
        """
        await self._storage.conn.flushdb()

    async def put(self, record: t.Mapping[str, t.Any]) -> None:
        """
        Store a record.

        Encodes the primary key to string and all attributes for which there
        exists an encoder to bytes, and stores them in Redis.

        Raises a ValueError if the primary key or any other attribute is
        missing from the record.

        Raises a CodingError if the primary key doesn't encode to a string, any
        attribute doesn't encode to bytes, or any error occurs during encoding.
        """
        try:
            record_key = record[self._primary_key_name]
        except KeyError:
            raise ValueError(f'Record {record} is missing a primary key.')
        record_key_str = self._record_key_str(record_key)
        encoded_record = self._encode_record(record)
        await self._storage.conn.hset(record_key_str, mapping=encoded_record)

    async def get(self, record_key: t.Any) -> t.Mapping[str, t.Any]:
        """
        Retrieve a previously stored record.

        Returns a dictionary containing the data stored in Redis associated
        with the given key. The key is first encoded using the configured
        primary key encoder. All attributes in the dictionary are decoded using
        the configured decoders, and only those for which a decoder exists are
        returned.

        Raises a ValueError if the data in Redis is missing any attribute for
        which there exists a decoder.

        Raises a CodingError if the given key fails to encode to a string, or
        an error occurs when decoding any attribute.
        """
        record_key_str = self._record_key_str(record_key)
        encoded_record = await self._storage.conn.hgetall(record_key_str)
        if not encoded_record:
            raise KeyError(
                f'No record with {self._primary_key_name}={record_key_str}.')
        return self._decode_record(encoded_record)

    def _record_key_str(self, record_key):
        try:
            record_key_str = self._primary_key_encoder(record_key)
        except Exception as e:
            raise CodingError(
                f'Unable to encode record key {record_key}.') from e
        if not isinstance(record_key_str, str):
            raise CodingError(
                f'Encoded record key {record_key_str} isn\'t a string.')
        return record_key_str

    def _encode_record(self, record):
        encoded_record = {}
        for attribute_name, encoder in self._encoders.items():
            try:
                attribute = record[attribute_name]
            except KeyError:
                raise ValueError(
                    f'Unable to encode {record}, which doesn\'t contain '
                    f'{attribute_name}.')
            try:
                encoded_attribute = encoder(attribute)
            except Exception as e:
                raise CodingError(
                    f'Error while encoding {attribute_name} {attribute} in '
                    f'{record}.') from e
            if not isinstance(encoded_attribute, bytes):
                raise CodingError(
                    f'Illegal type {type(encoded_attribute)} of '
                    f'{attribute_name}.')
            encoded_record[attribute_name.encode()] = encoded_attribute
        return encoded_record

    def _decode_record(self, encoded_record):
        decoded_record = {}
        for attribute_name, decoder in self._decoders.items():
            try:
                encoded_attribute = encoded_record[attribute_name.encode()]
            except KeyError:
                raise ValueError(
                    f'Unable to decode {encoded_record}, which doesn\'t '
                    f'contain {attribute_name}.')
            try:
                decoded_record[attribute_name] = decoder(encoded_attribute)
            except Exception as e:
                raise CodingError(
                    f'Error while decoding {attribute_name} '
                    f'{encoded_attribute} in {encoded_record}.') from e
        return decoded_record
