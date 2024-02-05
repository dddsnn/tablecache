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

import abc

import tablecache.types as tp


class DbAccess[RecordsSpec](abc.ABC):
    """
    A DB access abstraction.

    Provides access to sets of records stored in the DB via a records spec that
    is up to the concrete implementation.
    """
    @abc.abstractmethod
    async def get_records(self, records_spec: RecordsSpec) -> tp.AsyncRecords:
        """
        Asynchronously iterate over a subset of records.

        Fetches records matching the given spec and yields them.
        """
        raise NotImplementedError

    async def get_record(self, records_spec: RecordsSpec) -> tp.Record:
        """
        Fetch a single record.

        This is just a convenience shortcut around get_records().

        If more than one record matches the spec, one of them is returned, but
        there is no guarantee which. Raises a KeyError if no record matches.
        """
        try:
            return await anext(self.get_records(records_spec))
        except StopAsyncIteration as e:
            raise KeyError from e
