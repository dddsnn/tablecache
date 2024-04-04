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


_counters = {}
_gauges = {}


class NopMetric:
    def __init__(*args, **kwargs):
        pass

    def inc(self, _=1):
        pass

    def set(self, _):
        pass

    def labels(self, **_):
        return self


def get_prometheus_counter(name, *args, **kwargs):
    try:
        return _counters[name]
    except KeyError:
        return _counters.setdefault(name, pc.Counter(name, *args, **kwargs))


def get_prometheus_gauge(name, *args, **kwargs):
    try:
        return _gauges[name]
    except KeyError:
        return _gauges.setdefault(name, pc.Gauge(name, *args, **kwargs))


def get_nop_counter(name, *args, **kwargs):
    return _counters.setdefault(name, NopMetric())


def get_nop_gauge(name, *args, **kwargs):
    return _gauges.setdefault(name, NopMetric())


try:
    import prometheus_client as pc
    get_counter = get_prometheus_counter
    get_gauge = get_prometheus_gauge
except ImportError:
    get_counter = get_nop_counter
    get_gauge = get_nop_gauge
