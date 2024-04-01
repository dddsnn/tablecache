# Copyright 2023, 2024 Marc Lehmann

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

import asyncio
import collections.abc as ca
import operator as op
import struct
import typing as t

import aiorwlock
import redis.asyncio as redis

import tablecache.redis.codec as codec
import tablecache.index as index
import tablecache.storage as storage
import tablecache.types as tp


class CompatibleWriterLock(aiorwlock._WriterLock):
    def locked(self) -> bool:
        return super().locked


# Unfortunately, the locks used in the aiorwlock library aren't completely
# usable as drop-in replacements for asyncio.Lock. Luckily, it's just the
# difference between locked being a property vs. a method. So if we hack a
# compatible version into the library, we can use the writer_lock of a RWLock
# as the lock of our asyncio.Condition in the RedisTable.
aiorwlock._WriterLock = CompatibleWriterLock

type AttributeCodecs = ca.Mapping[str, codec.Codec]


def _identity(x):
    return x


class RedisCodingError(Exception):
    """
    Raised when any error relating to en- or decoding occurs.
    """


class RedisTable[Record, PrimaryKey: tp.PrimaryKey](
        storage.StorageTable[Record]):
    """
    A table stored in Redis.

    Enables storage and retrieval of records in Redis. Each record must have a
    primary key which uniquely identifies it within the table. Only attributes
    for which a codec is specified are stored.

    Each record is also associated with one or more index scores, one for each
    of the given index names. Defining score functions allow queries for
    multiple records via intervals of scores. Scores must be 64-bit floats (or
    other numbers that can be represented as 64-bit floats).

    Records are stored in a Redis hash with key ``<table_name>:r``, using their
    primary key encoded via the given primary key codec. Another hash
    ``<table_name>:s`` contains scratch records that aren't merged yet. Each
    index is stored as a Redis sorted set with the key
    ``<table_name>:i:<index_name>``. These store, for their respective index
    score, the primary key for the record.

    Index scores need not be unique, so each index score may map to multiple
    primary keys. All of the corresponding records need to be checked (the
    wrong ones are filtered out via a recheck predicate). This implies that it
    can be costly if lots of records have equal index scores.

    While scratch space is in use (i.e. in between the first call to
    :py:meth:`scratch_put_record` or :py:meth:`scratch_discard_records` and the
    corresponding :py:meth:`scratch_merge`), regular write operations
    (:py:meth:`put_record` and :py:meth:`delete_records`) are locked. Merging
    scratch space starts a background task that cleans up data in Redis. During
    this operation, further scratch activity is locked.

    The implementation of the scratch space requires a generation count to be
    stored with each record, which are used to exclude records in scratch space
    that aren't merged yet. The generation is incremented with each merge.
    Since it is stored as a 32-bit unsigned integer, there is an upper limit of
    the number of merges that can be done (of 2**32-1). Deletions in scratch
    space store some data natively (i.e. in Python structures rather than in
    Redis), so scratch operations with lots of deletions may consume
    considerable amounts of memory.
    """

    def __init__(
            self, conn: redis.Redis, *, table_name: str,
            record_scorer: index.RecordScorer[Record, PrimaryKey],
            primary_key_codec: codec.Codec[PrimaryKey],
            attribute_codecs: AttributeCodecs,
            attribute_extractor: ca.Callable[[Record], t.Any] = op.getitem,
            record_factory: ca.Callable[[dict], Record] = _identity) -> None:
        """
        :param conn: An async Redis connection. The connection will not be
            closed and needs to be cleaned up from the outside.
        :param table_name: The name of the table, used as a prefix for keys in
            Redis. Must be unique within the Redis instance.
        :param record_scorer: A RecordScorer used to calculate a record's
            scores for all the indexes that need to be represented in storage.
            The score function must not raise exceptions, or the storage may be
            left in an undefined state.
        :param primary_key_codec: A :py:class:`Codec` suitable to encode return
            values of the ``record_scorer``'s
            :py:meth:`primary_key <.RecordScorer.primary_key>` method. Encoded
            primary keys are used as keys in the Redis hash.
        :param attribute_codecs: A dictionary of codecs for record attributes.
            Must map attribute names (strings) to :py:class:`Codec` instances
            that are able to en-/decode the corresponding values. Only
            attributes present here are stored.
        :param attribute_extractor: A function extracting an attribute from a
            record by name. The default works when records are dicts.
        :param record_factory: A function that takes a dictionary mapping
            attribute names to values and returns a Record. The default works
            when records are dicts.
        :raise ValueError: If ``attribute_codecs`` is invalid.
        """
        if any(not isinstance(attribute_name, str)
               for attribute_name in attribute_codecs):
            raise ValueError('Attribute names must be strings.')
        self._conn = conn
        self._table_name = table_name
        self._record_scorer = record_scorer
        self._primary_key_codec = primary_key_codec
        self._row_codec = RowCodec(
            attribute_codecs, attribute_extractor, record_factory)
        self._rwlock = aiorwlock.RWLock()
        self._scratch_space = ScratchSpace()
        self._scratch_condition = asyncio.Condition(self._rwlock.writer_lock)

    def __repr__(self) -> str:
        return f'Redis table {self.name}'

    @t.override
    @property
    def name(self) -> str:
        return self._table_name

    @t.override
    async def clear(self) -> None:
        for index_name in self._record_scorer.index_names:
            await self._conn.delete(f'{self._table_name}:i:{index_name}')
        await self._conn.delete(f'{self._table_name}:r')
        await self._conn.delete(f'{self._table_name}:s')

    @t.override
    async def put_record(self, record: Record) -> None:
        """
        Store a record.

        Stores a record of all attributes for which a codec was configured in
        Redis. Other attributes that may be present are silently ignored. If a
        record with the same primary key exists, it is overwritten.

        :param record: The record to add.
        :raise ValueError: If any attribute is missing from the record.
        :raise RedisCodingError: If any attribute encode to something other
            than bytes, or any error occurs during encoding.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_space.is_clear)
            await self._put_record_locked(record)

    async def _put_record_locked(self, record):
        primary_key = self._record_scorer.primary_key(record)
        try:
            await self._delete_record_locked(primary_key)
        except KeyError:
            pass
        encoded_record = self._row_codec.encode(
            record, self._scratch_space.current_generation)
        await self._put_encoded_record(primary_key, record, encoded_record)

    async def _put_encoded_record(
            self, primary_key, record, encoded_record, hash_name='r'):
        assert hash_name in ('r', 's')
        encoded_primary_key = self._primary_key_codec.encode(primary_key)
        await self._conn.hset(
            f'{self._table_name}:{hash_name}', encoded_primary_key,
            encoded_record)
        await self._modify_index_entries(record, encoded_primary_key, 1)

    async def _modify_index_entries(self, record, encoded_primary_key, inc):
        assert inc == -1 or inc == 1
        for index_name in self._record_scorer.index_names:
            index_score = self._record_scorer.score(index_name, record)
            index_entries = await self._conn.zrange(
                f'{self._table_name}:i:{index_name}', index_score, index_score,
                byscore=True)
            await self._update_index_reference_count(
                index_name, index_score, index_entries, encoded_primary_key,
                inc)

    async def _update_index_reference_count(
            self, index_name, index_score, index_entries, encoded_primary_key,
            inc):
        for index_entry in index_entries:
            current_count, _, existing_encoded_primary_key = (
                self._decode_index_entry(index_entry))
            if existing_encoded_primary_key == encoded_primary_key:
                await self._conn.zrem(
                    f'{self._table_name}:i:{index_name}', index_entry)
                break
        else:
            current_count = 0
        new_count = current_count + inc
        assert 0 <= new_count <= 2
        if new_count > 0:
            index_entry = self._encode_index_entry(
                new_count, index_score, encoded_primary_key)
            mapping = PairAsItems(index_entry, index_score)
            await self._conn.zadd(
                f'{self._table_name}:i:{index_name}', mapping=mapping)

    def _encode_index_entry(self, count, index_score, encoded_primary_key):
        index_entry = struct.pack('Bd', count, index_score)
        return index_entry + encoded_primary_key

    def _decode_index_entry(self, index_entry):
        count, index_score = struct.unpack_from('Bd', index_entry)
        encoded_primary_key = index_entry[struct.calcsize('Bd'):]
        return count, index_score, encoded_primary_key

    async def _get_record_locked(self, primary_key):
        records = self._get_records_by_primary_keys(
            [self._primary_key_codec.encode(primary_key)])
        async for encoded_record, decoded_record in records:
            return encoded_record, decoded_record
        raise KeyError(f'No record with primary key {primary_key}.')

    async def _get_records_by_primary_keys(
            self, encoded_primary_keys,
            recheck_predicate=storage.StorageRecordsSpec.always_use_record,
            filter_scratch_records=True):
        if not encoded_primary_keys:
            return
        encoded_records = await self._get_records_from_hash(
            encoded_primary_keys, 'r')
        if not filter_scratch_records or self._scratch_space.is_merging():
            encoded_records |= await self._get_records_from_hash(
                encoded_primary_keys, 's')
        async for encoded_record, decoded_record in self._filtered_records(
                encoded_records, recheck_predicate, filter_scratch_records):
            yield encoded_record, decoded_record

    async def _get_records_from_hash(self, encoded_primary_keys, hash_name):
        assert hash_name in ('r', 's')
        encoded_records = await self._conn.hmget(
            f'{self._table_name}:{hash_name}', encoded_primary_keys)
        return {
            self._record_scorer.primary_key(self._row_codec.decode(r)[0]): r
            for r in encoded_records if r is not None}

    async def _filtered_records(
            self, encoded_records, recheck_predicate, filter_scratch_records):
        for primary_key, encoded_record in encoded_records.items():
            if encoded_record is None:
                continue
            decoded_record, generation = self._row_codec.decode(encoded_record)
            should_include_record = recheck_predicate(decoded_record)
            if filter_scratch_records:
                should_include_record &= (
                    self._scratch_space.record_is_current(
                        primary_key, encoded_record, generation))
            if should_include_record:
                yield encoded_record, decoded_record

    @t.override
    async def get_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        async with self._rwlock.reader_lock:
            async for _, decoded_record in self._get_records_locked(
                    records_spec):
                yield decoded_record

    async def _get_records_locked(
            self, records_spec, filter_scratch_records=True):
        encoded_primary_keys = []
        for interval in records_spec.score_intervals:
            # Prefixing the end of the range with '(' is the Redis way of
            # saying we want the interval to be open on that end.
            index_entries = await self._conn.zrange(
                f'{self._table_name}:i:{records_spec.index_name}',
                interval.ge, f'({interval.lt}', byscore=True)
            for index_entry in index_entries:
                _, _, encoded_primary_key = self._decode_index_entry(
                    index_entry)
                encoded_primary_keys.append(encoded_primary_key)
        records = self._get_records_by_primary_keys(
            encoded_primary_keys, records_spec.recheck_predicate,
            filter_scratch_records)
        async for encoded_record, decoded_record in records:
            yield encoded_record, decoded_record

    async def _delete_record_locked(self, primary_key):
        _, decoded_record = await self._get_record_locked(primary_key)
        await self._delete_record(primary_key, decoded_record)

    async def _delete_record(self, primary_key, decoded_record):
        encoded_primary_key = self._primary_key_codec.encode(primary_key)
        await self._modify_index_entries(
            decoded_record, encoded_primary_key, -1)
        await self._conn.hdel(f'{self._table_name}:r', encoded_primary_key)

    @t.override
    async def delete_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_space.is_clear)
            async for record in self._delete_records_locked(records_spec):
                yield record

    async def _delete_records_locked(self, records_spec):
        async for _, decoded_record in (
                self._get_records_locked(records_spec)):
            primary_key = self._record_scorer.primary_key(decoded_record)
            await self._delete_record(primary_key, decoded_record)
            yield decoded_record

    @t.override
    async def scratch_put_record(self, record: Record) -> None:
        """
        Add a record to scratch space.

        Regular write operations are locked until scratch space is merged.

        :param record: The record to add to scratch space.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_space.is_not_merging)
            await self._scratch_put_record_locked(record)

    async def _scratch_put_record_locked(self, record):
        primary_key = self._record_scorer.primary_key(record)
        try:
            existing_encoded_record, existing_decoded_record = (
                await self._get_record_locked(primary_key))
            self._scratch_space.mark_existing_record_for_deletion(
                existing_encoded_record, existing_decoded_record)
        except KeyError:
            pass
        encoded_record = self._row_codec.encode(
            record, self._scratch_space.next_generation)
        self._scratch_space.mark_record_for_adding(primary_key, encoded_record)
        await self._put_encoded_record(
            primary_key, record, encoded_record, 's')

    @t.override
    async def scratch_discard_records(
            self, records_spec: storage.StorageRecordsSpec[Record]
    ) -> ca.AsyncIterable[Record]:
        """
        Mark a set of records to be deleted in scratch space.

        Asynchronously iterates over the records that are marked for discarding
        as they exist in storage. These records will continue to be available
        until scratch space is merged. Must be fully consumed to finish the
        operation.

        Regular write operations are locked until scratch space is merged.

        Marking records for deletion requires a bit of internal state, which
        may get large with large numbers of records.

        :param records_spec: A specification of the records to mark for
            discarding.
        :return: The records marked for discarding as an asynchronous iterator,
            in no particular order.
        """
        async with self._scratch_condition:
            await self._scratch_condition.wait_for(
                self._scratch_space.is_not_merging)
            async for record in self._scratch_discard_records_locked(
                    records_spec):
                yield record

    async def _scratch_discard_records_locked(self, records_spec):
        async for _, decoded_record in self._get_records_locked(
                records_spec, filter_scratch_records=False):
            primary_key = self._record_scorer.primary_key(decoded_record)
            self._scratch_space.mark_primary_key_for_deletion(primary_key)
            yield decoded_record

    @t.override
    def scratch_merge(self) -> None:
        self._scratch_space.merge()
        asyncio.create_task(self._scratch_merge())

    async def _scratch_merge(self):
        await self._clean_up_scratch_records()
        self._scratch_space.merge_done()
        async with self._scratch_condition:
            self._scratch_condition.notify_all()

    async def _clean_up_scratch_records(self):
        encoded_records_to_delete = (
            self._scratch_space.encoded_records_for_deletion)
        primary_keys_to_delete = self._scratch_space.primary_keys_for_deletion
        scratch_records = await self._conn.hgetall(f'{self._table_name}:s')
        if scratch_records:
            await self._conn.hset(
                f'{self._table_name}:r', mapping=scratch_records)

        def should_delete(primary_key, encoded_record):
            return (
                primary_key in primary_keys_to_delete or
                encoded_record in encoded_records_to_delete)
        primary_keys_for_potential_deletion = list(
            primary_keys_to_delete | {
                self._record_scorer.primary_key(record)
                for record in self._scratch_space.records_for_deletion})
        records = self._get_records_by_primary_keys(
            primary_keys_for_potential_deletion, filter_scratch_records=False)
        async for encoded_record, decoded_record in records:
            primary_key = self._record_scorer.primary_key(decoded_record)
            if should_delete(primary_key, encoded_record):
                await self._delete_record(primary_key, decoded_record)
        await self._conn.delete(f'{self._table_name}:s')


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


class RowCodec[Record]:
    """
    Codec for a complete set of attributes.

    Encodes and decodes records into bytes. Uses an AttributeIdMap to generate
    small attribute IDs for each of a given set of named attributes. Each
    record is encoded along with a generation counter, which is an unsigned
    integer that can be used to determine when a record becomes valid.
    """

    def __init__(
            self, attribute_codecs: AttributeCodecs,
            attribute_extractor: ca.Callable[[Record], t.Any],
            record_factory: ca.Callable[[dict], Record],
            num_bytes_attribute_length: int = 2,
            num_bytes_generation: int = 4) -> None:
        """
        :param attribute_codecs: A dictionary mapping attribute names to
            codecs. Only record attributes contained here will be encoded.
        :param attribute_extractor: A function extracting an attribute from a
            record by name.
        :param record_factory: A function that takes a dictionary mapping
            attribute names to values and returns a Record.
        :param num_bytes_attribute_length: The number of bytes with which the
            length of each attribute is encoded. This value sets the limit for
            the maximum allowable encoded attribute size (to 1 less than the
            maximum unsigned integer representable with this number of bytes).
        :param num_bytes_generation: The number of bytes used to encode a
            record's generation. This limits the maximum number of generations
        """
        self._attribute_codecs = AttributeIdMap(attribute_codecs)
        self._attribute_extractor = attribute_extractor
        self._record_factory = record_factory
        self.num_bytes_attribute_length = num_bytes_attribute_length
        self.max_attribute_length = 2**(num_bytes_attribute_length * 8) - 1
        self.num_bytes_generation = num_bytes_generation
        self.max_generation = 2**(num_bytes_generation * 8) - 1

    def encode(self, record: Record, generation: int) -> bytes:
        """
        Encode a record.

        Encodes the given record into bytes. Only attributes for which a codec
        was provided on construction are encoded.

        A RedisCodingError is raised if the record is missing any attribute for
        which a codec was specified, an error occurs while encoding any
        individual attribute, or any encoded attribute is too long (determined
        by the number of bytes configured to store attribute length).

        Raises a ValueError if the generation is greater than max_generation
        (as determined by num_bytes_generation).
        """
        encoded_record = bytearray()
        if generation > self.max_generation:
            raise ValueError('Generation is too big.')
        encoded_record += generation.to_bytes(length=self.num_bytes_generation)
        for attribute_name, attribute_id, codec_ in self._attribute_codecs:
            encoded_attribute = self._encode_attribute(
                record, attribute_name, codec_)
            encoded_record += attribute_id
            encoded_record += len(encoded_attribute).to_bytes(
                length=self.num_bytes_attribute_length)
            encoded_record += encoded_attribute
        return bytes(encoded_record)

    def _encode_attribute(self, record, attribute_name, codec):
        try:
            attribute = self._attribute_extractor(record, attribute_name)
        except KeyError as e:
            raise ValueError(f'Attribute missing from {record}.') from e
        try:
            encoded_attribute = codec.encode(attribute)
        except Exception as e:
            raise RedisCodingError(
                f'Error while encoding {attribute_name} {attribute} in '
                f'{record}.') from e
        if not isinstance(encoded_attribute, bytes):
            raise RedisCodingError(
                f'Illegal type {type(encoded_attribute)} of encoding of '
                f'{attribute_name}.')
        if len(encoded_attribute) > self.max_attribute_length:
            raise RedisCodingError(
                f'Encoding of {attribute_name} is too long.')
        return encoded_attribute

    def decode(self, encoded_record: bytes) -> tuple[Record, int]:
        """
        Decode an encoded record.

        Decodes a byte string containing a previously encoded record. Returns
        the record along with its generation.

        Raises a ValueError if encoded_record is not a bytes object, or any
        attribute for which a codec exists is missing from it.

        Raises a RedisCodingError if the format of the encoded record is
        invalid or incomplete in any form.
        """
        if not isinstance(encoded_record, bytes):
            raise ValueError('Encoded record must be bytes.')
        record_dict = {}
        reader = BytesReader(encoded_record)
        try:
            generation = int.from_bytes(reader.read(self.num_bytes_generation))
        except BytesReader.NotEnoughBytes:
            raise RedisCodingError('Incomplete record generation.')
        while reader.bytes_remaining:
            attribute_name, decoded_attribute = self._decode_next_attribute(
                reader)
            if attribute_name in record_dict:
                raise RedisCodingError(
                    f'{attribute_name} contained twice in {encoded_record}.')
            record_dict[attribute_name] = decoded_attribute
        needed_attributes = self._attribute_codecs.attribute_names
        present_attributes = frozenset(record_dict)
        if (missing := needed_attributes - present_attributes):
            raise RedisCodingError(
                f'Attributes {missing} missing in {encoded_record}.')
        return self._record_factory(record_dict), generation

    def _decode_next_attribute(self, reader):
        try:
            attribute_id = reader.read(self._attribute_codecs.id_length)
            value_length = int.from_bytes(
                reader.read(self.num_bytes_attribute_length))
            encoded_value = reader.read(value_length)
        except BytesReader.NotEnoughBytes:
            raise RedisCodingError('Incomplete encoded attribute.')
        try:
            attribute_name, codec = self._attribute_codecs[attribute_id]
        except KeyError:
            raise RedisCodingError(
                'Error decoding encoded attribute with unknown ID '
                f'{attribute_id}.')
        try:
            return attribute_name, codec.decode(encoded_value)
        except Exception as e:
            raise RedisCodingError(
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


class ScratchSpace:
    """
    Scratch space metadata.

    Keeps track of data needed to distinguish between records that are current
    and should be returned to the client, and ones that belong to scratch space
    and should only be returned after a merge.

    Scratch space can be in one of 3 states:

    - Clear: No records have been added to scratch space (marked for adding or
      deletion). All records can be considered current.
    - Active: Records have been added to scratch space. A generation counter on
      all records determines whether a record is current (ones with a
      generation count higher than the current one are not).
    - Merging: The transition from active to merging increments the generation
      count and thus makes records previously added to scratch current. Data
      about records marked for deletion in scratch space makes it possible to
      consider those not current anymore.

    Scratch space transitions from clear to active any call to any of
    mark_existing_record_for_deletion(), mark_primary_key_for_deletion(), or
    mark_record_for_adding(). record_is_current() reflects the state from
    before adding any records to scratch space. The transition from active to
    merging is done via merge(). At this point, record_is_current() reflects
    all additions and deletions in scratch space. merge_done() transitions back
    to the clear state.

    It is up to the caller to ensure that these methods are called in the right
    order. If they are not, an AssertionError is raised.
    """

    def __init__(self):
        self._current_generation = 0
        self.merge_done()

    @property
    def current_generation(self):
        return self._current_generation

    @property
    def next_generation(self):
        return self._current_generation + 1

    @property
    def records_for_deletion(self):
        """Records that are currently marked to be deleted."""
        yield from self._existing_for_deletion.values()

    @property
    def encoded_records_for_deletion(self):
        """
        Encoded records that are currently marked to be deleted.

        These are the actual byte strings as they exist in storage.
        """
        return frozenset(self._existing_for_deletion)

    @property
    def primary_keys_for_deletion(self):
        """
        Primary keys that are currently marked to be deleted.

        After the merge, no records with any of these primary keys should exist
        anymore.
        """
        return frozenset(self._primary_keys_for_deletion)

    def _transition_to(self, state):
        assert state in ['clear', 'active', 'merging']
        if state == 'clear':
            assert getattr(self, '_state', 'merging') == 'merging'
        elif state == 'active':
            assert self._state in ['clear', 'active']
        else:
            assert self._state in ['clear', 'active']
        self._state = state

    def is_clear(self):
        return self._state == 'clear'

    def is_merging(self):
        return self._state == 'merging'

    def is_not_merging(self):
        return not self.is_merging()

    def mark_existing_record_for_deletion(
            self, encoded_record, decoded_record):
        """
        Mark an existing record to be deleted.

        This marks the actual byte string in storage to be deleted. Other
        records with the same primary key may remain. This can be used when a
        record in scratch space overwrites an existing one.

        This can be undone by mark_record_for_adding(), but only if the exact
        same byte string is provided there (not just the primary key).
        """
        self._transition_to('active')
        self._existing_for_deletion[encoded_record] = decoded_record

    def mark_primary_key_for_deletion(self, primary_key):
        """
        Mark a primary key to be deleted.

        This marks any record with the given primary key for deletion. After a
        call to merge(), no such record will be considered current.

        This can be undone by mark_record_for_adding().
        """
        self._transition_to('active')
        self._primary_keys_for_deletion.add(primary_key)

    def mark_record_for_adding(self, primary_key, encoded_record):
        """
        Mark a record for adding.

        This needs to be called if an existing record that was previously
        overwritten in scratch space is added back with its original data.
        """
        self._transition_to('active')
        self._primary_keys_for_deletion.discard(primary_key)
        self._existing_for_deletion.pop(encoded_record, None)

    def record_is_current(self, primary_key, encoded_record, generation):
        """
        Determine whether a record found in storage is current.

        Before the call to merge(), records added to scratch space are not
        considered current via their generation count (so that they are not
        returned to the client yet), and ones deleted from scratch space still
        are (so that they can still be returned to the client).

        After the call to merge(), this essentially switches, so that added
        records appear, and deleted ones vanish.
        """
        if generation > self._current_generation:
            return False
        if self.is_merging():
            return (
                encoded_record not in self._existing_for_deletion and
                primary_key not in self._primary_keys_for_deletion)
        return True

    def merge(self):
        """
        Merge scratch space.

        This triggers the transition from active to merging. At any point
        before this call, record_is_current() reflects the state as if changes
        in scratch space didn't exist. At any point after, it reflects the
        state that they do.
        """
        self._transition_to('merging')
        self._current_generation += 1

    def merge_done(self):
        """
        Signal that the merge is done.

        This triggers the state transition from merging back to clear. The
        caller assures that storage has been modified to reflect the state of
        scratch space, i.e. there doesn't exist any record for which
        record_is_current() would return False.
        """
        self._transition_to('clear')
        self._existing_for_deletion = {}
        self._primary_keys_for_deletion = set()
