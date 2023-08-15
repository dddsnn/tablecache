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
import collections.abc as ca
import functools
import numbers
import operator as op
import typing as t

import redis.asyncio as redis

import tablecache.codec as codec


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
    def __init__(self, **connect_kwargs: ca.Mapping[str, t.Any]) -> None:
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
    async def put(self, record: ca.Mapping[str, t.Any]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get(self, record_key: t.Any) -> ca.Mapping[str, t.Any]:
        raise NotImplementedError


class RedisTable(StorageTable):
    def __init__(
        self, redis_storage: RedisStorage, *, table_name: str,
        primary_key_name: str, attribute_codecs: ca.Mapping[str, codec.Codec],
        score_function: t.Callable[[t.Mapping[str, t.Any]],
                                   numbers.Real] = None
    ) -> None:
        """
        A table stored in Redis.

        Enables storage and retrieval of records in Redis. Records must be
        dict-like, with string keys. Each record must have a primary key which
        uniquely identifies it within the table. Only attributes for which a
        codec is specified are stored.

        Records are serialized to byte strings and stored in Redis as elements
        of a sorted set. A spearate hash stores the score for each element used
        to find elements in the set with matching scores in order to retrieve
        them again.

        :param redis_storage: A RedisStorage that provides a connection.
        :param table_name: The name of the table, used as a prefix for keys in
            Redis. Must be unique within the Redis instance.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in attribute_codecs.
        :param attribute_codecs: Dictionary of codecs for record attributes.
            Must map attribute names (string) to tablecache.Codec instances
            that are able to en-/decode the corresponding values. Only
            attributes present here are stored.
        """
        if any(not isinstance(attribute_name, str)
               for attribute_name in attribute_codecs):
            raise ValueError('Attribute names must be strings.')
        self._storage = redis_storage
        self._table_name = table_name
        self._primary_key_name = primary_key_name
        try:
            self._primary_key_codec = attribute_codecs[primary_key_name]
        except KeyError:
            raise ValueError('Codec for primary key is missing.')
        self._row_codec = RowCodec(attribute_codecs)
        self._score_function = (
            score_function or op.itemgetter(primary_key_name))
        self._score_codec = codec.FloatAsStringCodec()

    async def clear(self) -> None:
        """Delete all data belonging to this table."""
        await self._storage.conn.delete(
            f'{self._table_name}:rows', f'{self._table_name}:key_scores')

    async def put(self, record: ca.Mapping[str, t.Any]) -> None:
        """
        Store a record.

        Stores a record of all attributes for which a codec was configured in
        Redis. Other attributes that may be present are silently ignored. If a
        record with the same primary key exists, it is overwritten.

        Raises a ValueError if any attribute is missing from the record.

        Raises a CodingError if any attribute encode to something other than
        bytes, or any error occurs during encoding.
        """
        try:
            record_key = record[self._primary_key_name]
        except KeyError as e:
            raise ValueError(
                f'Record {record} is missing a primary key.') from e
        encoded_record_key = await self._maybe_delete_old_record(record_key)
        score = self._score_function(record)
        await self._storage.conn.hset(
            f'{self._table_name}:key_scores', encoded_record_key,
            self._score_codec.encode(score))
        encoded_record = self._row_codec.encode(record)
        await self._storage.conn.zadd(
            f'{self._table_name}:rows',
            mapping=PairAsItems(memoryview(encoded_record), score))

    async def _maybe_delete_old_record(self, record_key):
        try:
            encoded_record_key, old_encoded_record, _ = await self._get(
                record_key)
            await self._storage.conn.zrem(
                f'{self._table_name}:rows', old_encoded_record)
        except KeyError:
            encoded_record_key = self._encode_primary_key(record_key)
        return encoded_record_key

    def _encode_primary_key(self, key):
        try:
            return self._primary_key_codec.encode(key)
        except Exception as e:
            raise CodingError(f'Unable to encode primary key {key}.') from e

    async def _get(self, record_key):
        encoded_record_key = self._encode_primary_key(record_key)
        encoded_score = await self._storage.conn.hget(
            f'{self._table_name}:key_scores', encoded_record_key)
        if encoded_score is None:
            raise KeyError(
                f'No record with {self._primary_key_name}={record_key}.')
        score = self._score_codec.decode(encoded_score)
        encoded_records = await self._storage.conn.zrange(
            f'{self._table_name}:rows', score, score, byscore=True)
        for encoded_record in encoded_records:
            decoded_record = self._row_codec.decode(encoded_record)
            if decoded_record[self._primary_key_name] == record_key:
                return encoded_record_key, encoded_record, decoded_record
        raise KeyError(
            f'No record with {self._primary_key_name}={record_key}.')

    async def get(self, record_key: t.Any) -> ca.Mapping[str, t.Any]:
        """
        Retrieve a previously stored record by primary key.

        Returns a dictionary containing the data stored in Redis associated
        with the given key.

        Raises a CodingError if the data in Redis is missing any attribute for
        which there exists a codec, or an error occurs when decoding the set of
        attributes.
        """
        _, _, decoded_record = await self._get(record_key)
        return decoded_record


class PairAsItems:
    """
    A pseudo-dict consinsting only of a single pair of values.

    A workaround to pass a non-hashable key to redis, since the library expects
    things implementing items().
    """
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def items(self):
        yield self.key, self.value


class AttributeIdMap:
    """
    Utility wrapper to map smaller keys to a dictionary.

    Takes a dictionary mapping attribute names to values, and assigns each
    key a fixed-length bytes equivalent (an ID). The length of each ID is
    stored in the id_length property.

    Supports item access by ID, returning a tuple of attribute name and value.
    Also supports iteration, yielding tuples of attribute name, attribute ID,
    and value.
    """
    def __init__(self, named_attributes: ca.Mapping[str, t.Any]) -> None:
        self.attribute_names = frozenset(named_attributes)
        self._data = {}
        self.id_length = (len(named_attributes).bit_length() + 7) // 8
        for i, (attribute_name, value) in enumerate(named_attributes.items()):
            attribute_id = i.to_bytes(length=self.id_length)
            self._data[attribute_id] = (attribute_name, value)

    def __iter__(self) -> ca.Iterator[tuple[str, bytes, t.Any]]:
        for attribute_id, (attribute_name, value) in self._data.items():
            yield attribute_name, attribute_id, value

    def __getitem__(self, attribute_id):
        return self._data[attribute_id]


class RowCodec:
    """
    Codec for an complete set of attributes.

    Encodes and decodes records into bytes. Uses an AttributeIdMap to generate
    small attribute IDs for each of a given set of named attributes.
    """
    def __init__(
            self, attribute_codecs: t.Mapping[str, codec.Codec],
            num_bytes_attribute_length: int = 2) -> None:
        """
        :param attribute_codecs: A dictionary mapping attribute names to
            codecs. Only record attributes contained here will be encoded.
        :param num_bytes_attribute_length: The number of bytes with which the
            length of each attribute is encoded. This value sets the limit for
            the maximum allowable encoded attribute size (to 1 less than the
            maximum unsigned integer representable with this number of bytes).
        """
        self._attribute_codecs = AttributeIdMap(attribute_codecs)
        self.num_bytes_attribute_length = num_bytes_attribute_length
        self.max_attribute_length = 2**(num_bytes_attribute_length * 8) - 1

    def encode(self, record: t.Mapping[str, t.Any]) -> bytearray:
        """
        Encode a record.

        Encodes the given record to a bytearray. Only attributes for which a
        codec was provided on construction are encoded.

        A CodingError is raised if the record is missing any attribute for
        which a codec was specified, an error occurs while encoding any
        individual attribute, or any encoded attribute is too long (determined
        by the number of bytes configured to store attribute length).
        """
        encoded_record = bytearray()
        for attribute_name, attribute_id, codec in self._attribute_codecs:
            encoded_attribute = self._encode_attribute(
                record, attribute_name, codec)
            encoded_record += attribute_id
            encoded_record += len(encoded_attribute).to_bytes(
                length=self.num_bytes_attribute_length)
            encoded_record += encoded_attribute
        return encoded_record

    def _encode_attribute(self, record, attribute_name, codec):
        try:
            attribute = record[attribute_name]
        except KeyError as e:
            raise ValueError(f'Attribute missing from {record}.')
        try:
            encoded_attribute = codec.encode(attribute)
        except Exception as e:
            raise CodingError(
                f'Error while encoding {attribute_name} {attribute} in '
                f'{record}.') from e
        if not isinstance(encoded_attribute, bytes):
            raise CodingError(
                f'Illegal type {type(encoded_attribute)} of encoding of '
                f'{attribute_name}.')
        if len(encoded_attribute) > self.max_attribute_length:
            raise CodingError(f'Encoding of {attribute_name} is too long.')
        return encoded_attribute

    def decode(self, encoded_record: bytes) -> t.Mapping[str, t.Any]:
        """
        Decode an encoded record.

        Decodes a byte string containing a previously encoded record.

        Raises a ValueError if encoded_record is not a bytes object, or any
        attribute for which a codec exists is missing from it.

        Raises a CodingError if the format of the encoded record is invalid or
        incomplete in any form.
        """
        if not isinstance(encoded_record, bytes):
            raise ValueError('Encoded record must be bytes.')
        decoded_record = {}
        reader = BytesReader(encoded_record)
        while reader.bytes_remaining:
            attribute_name, decoded_attribute = self._decode_next_attribute(
                reader)
            if attribute_name in decoded_record:
                raise CodingError(
                    f'{attribute_name} contained twice in {encoded_record}.')
            decoded_record[attribute_name] = decoded_attribute
        needed_attributes = self._attribute_codecs.attribute_names
        present_attributes = frozenset(decoded_record)
        if (missing := needed_attributes - present_attributes):
            raise CodingError(
                f'Attributes {missing} missing in {encoded_record}.')
        return decoded_record

    def _decode_next_attribute(self, reader):
        try:
            attribute_id = reader.read(self._attribute_codecs.id_length)
            value_length = int.from_bytes(
                reader.read(self.num_bytes_attribute_length))
            encoded_value = reader.read(value_length)
        except BytesReader.NotEnoughBytes:
            raise CodingError('Incomplete encoded attribute.')
        try:
            attribute_name, codec = self._attribute_codecs[attribute_id]
        except KeyError:
            raise CodingError(
                'Error decoding encoded attribute with unknown ID '
                f'{attribute_id}.')
        try:
            return attribute_name, codec.decode(encoded_value)
        except Exception as e:
            raise CodingError(
                f'Error while decoding {attribute_name} (ID {attribute_id}) '
                f'{encoded_value}.') from e


class BytesReader:
    class NotEnoughBytes(Exception):
        """Raised when there are not enough bytes to satisfy a read."""

    def __init__(self, bs: bytes) -> None:
        self._bs = bs
        self._pos = 0

    @property
    def bytes_remaining(self) -> int:
        return len(self._bs) - self._pos

    def read(self, n: int) -> bytes:
        """
        Read exactly n bytes, advancing the read position.

        Raises NotEnoughBytes if n > bytes_remaining.
        """
        if n > self.bytes_remaining:
            raise self.NotEnoughBytes
        new_pos = self._pos + n
        try:
            return self._bs[self._pos:new_pos]
        finally:
            self._pos = new_pos
