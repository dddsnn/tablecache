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
import struct
import typing as t

import redis.asyncio as redis

import tablecache.codec as codec
import tablecache.index as index
import tablecache.types as tp

type AttributeCodecs = ca.Mapping[str, codec.Codec]

class CodingError(Exception):
    """
    Raised when any error relating to en- or decoding occurs.
    """


class StorageTable[PrimaryKey](abc.ABC):
    @property
    @abc.abstractmethod
    def table_name(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    async def clear(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def put_record(self, record: tp.Record) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        raise NotImplementedError

    @abc.abstractmethod
    async def get_records(
            self, records_spec: index.StorageRecordsSpec) -> tp.Records:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_records(
            self, records_spec: index.StorageRecordsSpec) -> int:
        raise NotImplementedError


class RedisTable[PrimaryKey](StorageTable[PrimaryKey]):
    def __init__(
            self, conn: redis.Redis, *, table_name: str, primary_key_name: str,
            attribute_codecs: AttributeCodecs,
            score_functions: t.Mapping[str, tp.ScoreFunction]) -> None:
        """
        A table stored in Redis.

        Enables storage and retrieval of records in Redis. Records must be
        dict-like, with string keys. Each record must have a primary key which
        uniquely identifies it within the table. Only attributes for which a
        codec is specified are stored.

        Each record is also associated with a score, which can be used to get
        many elements with scores in a given subset.

        Records are serialized to byte strings and stored in Redis as elements
        of a sorted set. This is used for fast queries by score subset. For
        queries by primary key, a spearate hash stores the score for each
        element used to find elements in the set with matching scores. All
        matching elements have to be iterated here, which implies that getting
        by primary key is slow if there are many elements with the same score.

        :param conn: An async Redis connection. The connection will not be
            closed and needs to be cleaned up from the outside.
        :param table_name: The name of the table, used as a prefix for keys in
            Redis. Must be unique within the Redis instance.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in attribute_codecs.
        :param attribute_codecs: Dictionary of codecs for record attributes.
            Must map attribute names (string) to tablecache.Codec instances
            that are able to en-/decode the corresponding values. Only
            attributes present here are stored.
        :param score_function: A function that extracts a score from a record.
            A record's score must not change, even if the record is changed,
            i.e. it must be derived only from immutable fields. Getting the
            primary key or its hash may be a good option.
        """
        if any(not isinstance(attribute_name, str)
               for attribute_name in attribute_codecs):
            raise ValueError('Attribute names must be strings.')
        if primary_key_name not in attribute_codecs:
            raise ValueError('Codec for primary key is missing.')
        if 'primary_key' not in score_functions:
            raise ValueError
        self._conn = conn
        self._table_name = table_name
        self._primary_key_name = primary_key_name
        self._row_codec = RowCodec(attribute_codecs)
        self._score_functions = score_functions

    @property
    @t.override
    def table_name(self) -> str:
        return self._table_name

    @t.override
    async def clear(self) -> None:
        """Delete all data belonging to this table."""
        for index_name in self._score_functions:
            await self._conn.delete(f'{self.table_name}:{index_name}')

    @t.override
    async def put_record(self, record: tp.Record) -> None:
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
            primary_key = record[self._primary_key_name]
        except KeyError as e:
            raise ValueError(
                f'Record {record} is missing a primary key.') from e
        await self._maybe_delete_old_record(primary_key)
        encoded_record = self._row_codec.encode(record)
        primary_key_score = self._score_functions['primary_key'](**record)
        await self._conn.zadd(
            f'{self.table_name}:primary_key',
            mapping=PairAsItems(memoryview(encoded_record), primary_key_score))
        for index_name, score_function in self._score_functions.items():
            score = score_function(**record)
            if index_name == 'primary_key':
                continue
            encoded_scores = await self._conn.zrange(
                f'{self.table_name}:{index_name}', score, score, byscore=True)
            for encoded_score in encoded_scores:
                count, _, existing_primary_key_score = struct.unpack(
                    'Idd', encoded_score)
                if existing_primary_key_score == primary_key_score:
                    await self._conn.zrem(
                        f'{self.table_name}:{index_name}', encoded_score)
                    count += 1
                    break
            else:
                count = 1
            mapping = PairAsItems(struct.pack(
                'Idd', count, score, primary_key_score), score)
            await self._conn.zadd(
                f'{self.table_name}:{index_name}', mapping=mapping)

    async def _maybe_delete_old_record(self, primary_key):
        try:
            await self.delete_record(primary_key)
        except KeyError:
            pass

    async def _get(self, primary_key):
        primary_key_score = self._score_functions['primary_key'](
            **{self._primary_key_name: primary_key})
        encoded_records = await self._conn.zrange(
            f'{self.table_name}:primary_key', primary_key_score,
            primary_key_score, byscore=True)
        for encoded_record in encoded_records:
            decoded_record = self._row_codec.decode(encoded_record)
            if decoded_record[self._primary_key_name] == primary_key:
                return encoded_record, decoded_record
        # A score exists for that key, but no record with that score. This
        # happens when deleting ranges, where we don't have access to the keys
        # belonging to the scores we delete. Clean this up here.
        raise KeyError(
            f'No record with {self._primary_key_name}={primary_key}.')

    @t.override
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Retrieve a previously stored record by primary key.

        Returns a dictionary containing the data stored in Redis associated
        with the given key.

        Raises a CodingError if the data in Redis is missing any attribute for
        which there exists a codec, or an error occurs when decoding the set of
        attributes.
        """
        _, decoded_record = await self._get(primary_key)
        return decoded_record

    @t.override
    async def get_records(
            self, records_spec: index.StorageRecordsSpec) -> tp.Records:
        """
        Get records with scores within a subset.

        Asynchronously iterates over all records with a score within the given
        subset. The lower bound is inclusive, the upper bound exclusive, so a
        record with score s is yielded if there is an interval i in the
        subset's score_intervals with s in i.

        No particular order is guaranteed.
        """
        if records_spec.index_name == 'primary_key':
            primary_key_score_intervals = records_spec.score_intervals
        else:
            primary_key_scores = []
            for interval in records_spec.score_intervals:
                encoded_scores = await self._conn.zrange(
                    f'{self.table_name}:{records_spec.index_name}',
                    interval.ge, f'({interval.lt}', byscore=True)
                for encoded_score in encoded_scores:
                    primary_key_scores.append(
                        struct.unpack('Idd', encoded_score)[2])
            import math
            primary_key_score_intervals = [index.Interval(s, math.nextafter(
                s, float('inf'))) for s in primary_key_scores]
        # Prefixing the end of the range with '(' is the Redis way of saying we
        # want the interval to be open on that end.
        for interval in primary_key_score_intervals:
            encoded_records = await self._conn.zrange(
                f'{self.table_name}:primary_key', interval.ge,
                f'({interval.lt}', byscore=True)
            for encoded_record in encoded_records:
                decoded_record = self._row_codec.decode(encoded_record)
                if records_spec.recheck_predicate(decoded_record):
                    yield decoded_record

    @t.override
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        """Delete a record by primary key."""
        record = await self.get_record(primary_key)
        primary_key_score = self._score_functions['primary_key'](
            **{self._primary_key_name: primary_key})
        for index_name, score_function in self._score_functions.items():
            if index_name == 'primary_key':
                continue
            score = score_function(**record)
            encoded_scores = await self._conn.zrange(
                f'{self.table_name}:{index_name}', score, score, byscore=True)
            for encoded_score in encoded_scores:
                count, _, existing_primary_key_score = struct.unpack(
                    'Idd', encoded_score)
                if existing_primary_key_score == primary_key_score:
                    await self._conn.zrem(
                        f'{self.table_name}:{index_name}', encoded_score)
                    count -= 1
                    if count > 0:
                        mapping = PairAsItems(struct.pack(
                            'Idd', count, score, primary_key_score), score)
                        # PERF all zadd calls in one++++++++++++++
                        await self._conn.zadd(
                            f'{self.table_name}:{index_name}', mapping=mapping)
                    break
        await self._conn.zrem(f'{self.table_name}:primary_key', memoryview(self._row_codec.encode(record)))

    @t.override
    async def delete_records(
            self, records_spec: index.StorageRecordsSpec) -> int:
        """
        Delete records with scores in any of the given intervals.

        Returns the number of records deleted.
        """
        records = self.get_records(records_spec)
        num_deleted = 0
        async for record in records:
            await self.delete_record(record[self._primary_key_name])
            num_deleted += 1
        return num_deleted


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
            self, attribute_codecs: AttributeCodecs,
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

    def encode(self, record: tp.Record) -> bytearray:
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
        for attribute_name, attribute_id, codec_ in self._attribute_codecs:
            encoded_attribute = self._encode_attribute(
                record, attribute_name, codec_)
            encoded_record += attribute_id
            encoded_record += len(encoded_attribute).to_bytes(
                length=self.num_bytes_attribute_length)
            encoded_record += encoded_attribute
        return encoded_record

    def _encode_attribute(self, record, attribute_name, codec):
        try:
            attribute = record[attribute_name]
        except KeyError as e:
            raise ValueError(f'Attribute missing from {record}.') from e
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

    def decode(self, encoded_record: bytes) -> tp.Record:
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
