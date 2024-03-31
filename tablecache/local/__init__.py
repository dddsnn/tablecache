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
This submodule provides the :py:class:`.LocalStorageTable`, an implementation
of :py:class:`.StorageTable` that stores its records in local Python data
structures. It uses the
:external:py:mod:`sortedcontainers <sortedcontainers.sortedlist>`
library for its indexes, which is installed as a dependency when the ``local``
extra is selected.
"""

try:
    import aiorwlock
    import sortedcontainers
except ImportError as e:
    raise Exception(
        'Please install tablecache[local] to use tablecache.local.') from e

from tablecache.local.storage import LocalStorageTable
