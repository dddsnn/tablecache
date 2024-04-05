# Copyright 2024 Marc Lehmann

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
This submodule provides the :py:class:`.PostgresAccess`, an implementation
of :py:class:`.DbAccess` for Postgres. It uses the :external:py:mod:`asyncpg`
library, which gets pulled in as a dependency when the ``postgres`` extra is
selected.
"""

try:
    import asyncpg
except ImportError as e:
    raise Exception(
        'Please install tablecache[postgres] to use tablecache.postgres.'
    ) from e

from tablecache.postgres.db import DbRecord, PostgresAccess, RecordsSpec
