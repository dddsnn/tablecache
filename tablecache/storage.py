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
        """Delete all data belonging to this table."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_record(self, record: tp.Record) -> None:
        """
        Store a record.

        May raise an exception if the record is invalid in some way.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_record(self, primary_key: PrimaryKey) -> tp.Record:
        """
        Retrieve a previously stored record by primary key.

        Raises a KeyError if no record with that primary key exists.

        May raise other exceptions if there is a problem in retrieving the
        record.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_records(
            self, records_spec: index.StorageRecordsSpec) -> tp.Records:
        """
        Get multiple records.

        Asynchronously iterates over all records that match the recods spec.
        That's all records that have a score in the specified index that is
        contained in one of the specified intervals, and additionally match the
        recheck predicate.

        Records are guaranteed to be unique as long as the record_spec's
        intervals don't overlap (as per their contract).

        No particular order is guaranteed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        """Delete a record by primary key."""
        raise NotImplementedError

    @abc.abstractmethod
    async def delete_records(
            self, records_spec: index.StorageRecordsSpec) -> int:
        """
        Delete multiple records.

        Deletes exactly those records that would have been returned by
        get_records() when called with the same argument.

        Returns the number of records deleted.
        """
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

        Each record is also associated with one or more index scores, one for
        each of the given score functions. At the very least, there has to be
        one for the primary key (called primary_key), which returns a score for
        the primary key. Other than the primary key itself, the score needs not
        be unique. Score functions for other indexes may be defined, which
        allow queries for multiple records via intervals of scores.

        Each index is stored as a Redis sorted set with the key
        <table_name>:<index_name>. The primary_key takes on the special role of
        storing the records themselves. They are serialized to byte strings
        using the attribute codecs, and can be accessed via their primary key
        score. Other indexes only store, for their respective index score, the
        primary key score for the record. That means there is a layer of
        indirection in getting records via another index.

        Neither primary key nor other indexes' scores need be unique, so each
        index score may map to multiple primary key scores, each of which may
        belong to different primary keys. All of these primary key scores need
        to be checked (the wrong ones are filtered out via a recheck
        predicate). This implies firstly that it can be costly if lots of
        records have equal index scores. Secondly, the index needs to keep
        track of the number of primary key scores it maps to. This is stored as
        a 32-bit unsigned integer, so there is a hard limit of 2**32-1 primary
        key scores that each index score may map to.

        :param conn: An async Redis connection. The connection will not be
            closed and needs to be cleaned up from the outside.
        :param table_name: The name of the table, used as a prefix for keys in
            Redis. Must be unique within the Redis instance.
        :param primary_key_name: The name of the attribute to be used as
            primary key. Must also be present in attribute_codecs.
        :param attribute_codecs: A dictionary of codecs for record attributes.
            Must map attribute names (strings) to Codec instances that are able
            to en-/decode the corresponding values. Only attributes present
            here are stored.
        :param score_functions: A dictionary mapping index names (strings) to
            score functions. A score function takes records attributes as
            kwargs and returns a number that can be represented as a 64-bit
            float. Must contain at least one function for the primary_key.
        """
        if any(not isinstance(attribute_name, str)
               for attribute_name in attribute_codecs):
            raise ValueError('Attribute names must be strings.')
        if primary_key_name not in attribute_codecs:
            raise ValueError('Codec for primary key is missing.')
        if 'primary_key' not in score_functions:
            raise ValueError('Missing primary_key score function.')
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
        try:
            await self.delete_record(primary_key)
        except KeyError:
            pass
        encoded_record = self._row_codec.encode(record)
        primary_key_score = self._score_functions['primary_key'](**record)
        await self._conn.zadd(
            f'{self.table_name}:primary_key',
            mapping=PairAsItems(memoryview(encoded_record), primary_key_score))
        await self._modify_index_entries(record, primary_key_score, 1)

    async def _modify_index_entries(self, record, primary_key_score, inc):
        assert inc == -1 or inc == 1
        for index_name, score_function in self._score_functions.items():
            if index_name == 'primary_key':
                continue
            index_score = score_function(**record)
            encoded_primary_key_scores = await self._conn.zrange(
                f'{self.table_name}:{index_name}', index_score, index_score,
                byscore=True)
            await self._update_index_reference_count(
                index_name, index_score, encoded_primary_key_scores,
                primary_key_score, inc)

    async def _update_index_reference_count(
            self, index_name, index_score, encoded_primary_key_scores,
            primary_key_score, inc):
        for encoded_primary_key_score in encoded_primary_key_scores:
            current_count, _, existing_primary_key_score = struct.unpack(
                'Idd', encoded_primary_key_score)
            if existing_primary_key_score == primary_key_score:
                await self._conn.zrem(
                    f'{self.table_name}:{index_name}',
                    encoded_primary_key_score)
                break
        else:
            current_count = 0
        new_count = current_count + inc
        if new_count > 0:
            index_entry = struct.pack(
                'Idd', new_count, index_score, primary_key_score)
            mapping = PairAsItems(index_entry, index_score)
            await self._conn.zadd(
                f'{self.table_name}:{index_name}', mapping=mapping)

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
        _, decoded_record = await self._get_record(primary_key)
        return decoded_record

    async def _get_record(self, primary_key):
        primary_key_score = self._score_functions['primary_key'](
            **{self._primary_key_name: primary_key})
        records = self._get_records_by_primary_key_score(
            [index.Interval(primary_key_score, primary_key_score)],
            lambda r: r[self._primary_key_name] == primary_key)
        async for encoded_record, decoded_record in records:
            return encoded_record, decoded_record
        raise KeyError(
            f'No record with {self._primary_key_name}={primary_key}.')

    async def _get_records_by_primary_key_score(
            self, primary_key_score_intervals, recheck_predicate,
            upper_inclusive=True):
        for interval in primary_key_score_intervals:
            # Prefixing the end of the range with '(' is the Redis way of
            # saying we want the interval to be open on that end.
            upper = interval.lt if upper_inclusive else f'({interval.lt}'
            encoded_records = await self._conn.zrange(
                f'{self.table_name}:primary_key', interval.ge, upper,
                byscore=True)
            for encoded_record in encoded_records:
                decoded_record = self._row_codec.decode(encoded_record)
                if recheck_predicate(decoded_record):
                    yield encoded_record, decoded_record

    @t.override
    async def get_records(
            self, records_spec: index.StorageRecordsSpec) -> tp.Records:
        async for _, decoded_record in self._get_records(records_spec):
            yield decoded_record

    async def _get_records(self, records_spec):
        if records_spec.index_name == 'primary_key':
            upper_inclusive = False
            primary_key_score_intervals = records_spec.score_intervals
        else:
            upper_inclusive = True
            primary_key_scores = []
            for interval in records_spec.score_intervals:
                encoded_primary_key_scores = await self._conn.zrange(
                    f'{self.table_name}:{records_spec.index_name}',
                    interval.ge, f'({interval.lt}', byscore=True)
                for encoded_primary_key_score in encoded_primary_key_scores:
                    primary_key_scores.append(
                        struct.unpack('Idd', encoded_primary_key_score)[2])
            primary_key_score_intervals = [
                index.Interval(s, s) for s in primary_key_scores]
        records = self._get_records_by_primary_key_score(
            primary_key_score_intervals, records_spec.recheck_predicate,
            upper_inclusive)
        async for encoded_record, decoded_record in records:
            yield encoded_record, decoded_record

    @t.override
    async def delete_record(self, primary_key: PrimaryKey) -> None:
        encoded_record, decoded_record = await self._get_record(primary_key)
        await self._delete_record(encoded_record, decoded_record)

    async def _delete_record(self, encoded_record, decoded_record):
        primary_key_score = self._score_functions['primary_key'](
            **decoded_record)
        await self._modify_index_entries(decoded_record, primary_key_score, -1)
        await self._conn.zrem(f'{self.table_name}:primary_key', encoded_record)

    @t.override
    async def delete_records(
            self, records_spec: index.StorageRecordsSpec) -> int:
        num_deleted = 0
        async for encoded_record, decoded_record in (
                self._get_records(records_spec)):
            await self._delete_record(encoded_record, decoded_record)
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
