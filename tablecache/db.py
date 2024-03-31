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
import dataclasses as dc


@dc.dataclass(frozen=True)
class DbRecordsSpec:
    """Base type for a specification of records in the DB."""


@dc.dataclass(frozen=True)
class QueryArgsDbRecordsSpec(DbRecordsSpec):
    """A specification of DB records via a query and args."""
    query: str
    args: tuple

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
