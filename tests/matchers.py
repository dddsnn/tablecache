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


import hamcrest.core.base_matcher

import tablecache as tc


class IsIntervalContaining(hamcrest.core.base_matcher.BaseMatcher):
    def __init__(self, value):
        self.value = value

    def _matches(self, item):
        if not isinstance(item, tc.Interval):
            return False
        return item.ge <= self.value < item.lt

    def describe_to(self, description):
        description.append_text(f'interval containing {self.value}')


def is_interval_containing(value):
    return IsIntervalContaining(value)
