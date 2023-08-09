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

import functools

import redis.asyncio as redis


class CodingError(Exception):
    """
    Raised when any error relating to en- or decoding occurs.
    """


class RedisStorage:
    def __init__(self, **connect_kwargs):
        self._conn_factory = functools.partial(redis.Redis, **connect_kwargs)

    async def __aenter__(self):
        self._conn = self._conn_factory()
        return self

    async def __aexit__(self, *_):
        await self._conn.close()
        del self._conn
        return False

    @property
    def conn(self):
        try:
            return self._conn
        except AttributeError as e:
            raise AttributeError(
                'You have to connect the storage before using it.') from e


class RedisTable:
    def __init__(
            self, redis_storage, *, primary_key_name, encoders, decoders,
            primary_key_encoder=str):
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

    async def clear(self):
        await self._storage.conn.flushdb()

    async def put(self, record):
        try:
            record_key = record[self._primary_key_name]
        except KeyError:
            raise ValueError(f'Record {record} is missing a primary key.')
        record_key_str = self._record_key_str(record_key)
        encoded_record = self._encode_record(record)
        record_key = encoded_record[self._primary_key_name.encode()]
        await self._storage.conn.hset(record_key_str, mapping=encoded_record)

    async def get(self, record_key):
        record_key_str = self._record_key_str(record_key)
        encoded_record = await self._storage.conn.hgetall(record_key_str)
        if not encoded_record:
            raise KeyError(
                f'No record with {self._primary_key_name}={record_key_str}.')
        return self._decode_record(encoded_record)

    def _record_key_str(self, record_key):
        try:
            record_key_str = self._primary_key_encoder(record_key)
            assert isinstance(record_key_str, str)
        except Exception as e:
            raise CodingError(
                f'Unable to encode record key {record_key} as string.') from e
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
