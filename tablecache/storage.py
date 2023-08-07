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
            self, redis_storage, table_name, primary_key, *, encoders,
            decoders):
        self._storage = redis_storage
        self.table_name = table_name
        self._primary_key = primary_key.encode()
        self._encoders = encoders
        self._decoders = decoders

    async def put(self, record):
        encoded_record = self._encode_record(record)
        record_key = encoded_record[self._primary_key]
        await self._storage.conn.hset(
            f'{self.table_name}:{record_key}', mapping=encoded_record)

    async def get(self, key):
        encoded_record = await self._storage.conn.hgetall(
            f'{self.table_name}:{key}')
        if not encoded_record:
            raise KeyError(
                f'No record with {self._primary_key.decode()}={key}.')
        return self._decode_record(encoded_record)

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
                raise ValueError(
                    f'Error while encoding {attribute_name} {attribute} in '
                    f'{record}.') from e
            if not isinstance(encoded_attribute, bytes):
                raise ValueError(
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
                raise ValueError(
                    f'Error while decoding {attribute_name} '
                    f'{encoded_attribute} in {encoded_record}.') from e
        return decoded_record
