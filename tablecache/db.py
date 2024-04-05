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

"""
The :py:class:`DbAccess` is the abstract base for access to a database. It's a
very simple interface, able to get records based on a
:py:class:`DbRecordsSpec`.
"""

import abc
import collections.abc as ca
import typing as t


type RecordParser[DbRecord, Record] = ca.Callable[[DbRecord], Record]


def _identity_parser(record):
    return record


class DbRecordsSpec[DbRecord, Record]:
    """Base type for a specification of records in the DB."""

    def __init__(
            self, *,
            record_parser: t.Optional[RecordParser[DbRecord, Record]] = None):
        """
        :param record_parser: An optional function that is applied to each
            record before it is returned. The default is to return the record
            as-is.
        """
        self.record_parser = record_parser or _identity_parser


class QueryArgsDbRecordsSpec[DbRecord, Record](
        DbRecordsSpec[DbRecord, Record]):
    """A specification of DB records via a query and args."""

    def __init__(
            self, query: str, args: tuple, *,
            record_parser: t.Optional[RecordParser[DbRecord, Record]] = None):
        super().__init__(record_parser=record_parser)
        self.query = query
        self.args = args

    def __repr__(self) -> str:
        return f'a query with arguments {self.args}'


class DbAccess[Record, RecordsSpec](abc.ABC):
    """
    A DB access abstraction.

    Provides access to sets of records stored in the DB via a records spec that
    is up to the concrete implementation.
    """
    @abc.abstractmethod
    async def get_records(
            self, records_spec: RecordsSpec) -> ca.AsyncIterable[Record]:
        """
        Asynchronously iterate over a subset of records.

        Fetches records matching the given spec and yields them.

        :param records_spec: A specification of records.
        :return: The requested records, as an asynchronous iterator.
        """
        raise NotImplementedError

    async def get_record(self, records_spec: RecordsSpec) -> Record:
        """
        Fetch a single record.

        This is just a convenience shortcut around :py:meth:`get_records`.

        If more than one record matches the spec, one of them is returned, but
        there is no guarantee which.
        :raise KeyError: If no record matches.
        """
        try:
            return await anext(self.get_records(records_spec))
        except StopAsyncIteration as e:
            raise KeyError from e
