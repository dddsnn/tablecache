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

from hamcrest import *
import pytest

import tablecache as tc
from matchers import is_interval_containing


class TestPrimaryKeyIndexes:
    @pytest.fixture
    def indexes(self):
        return tc.PrimaryKeyIndexes('pk', 'query_all', 'query_some')

    def test_index_names(self, indexes):
        assert indexes.index_names == frozenset(['primary_key'])

    @pytest.mark.parametrize('pk', [123, '123'])
    def test_score(self, indexes, pk):
        assert indexes.score('primary_key', {'pk': pk, 's': 's'}) == hash(pk)

    @pytest.mark.parametrize('pk', [123, '123'])
    def test_primary_key_score(self, indexes, pk):
        assert indexes.primary_key_score(pk) == hash(pk)

    @pytest.mark.parametrize('pks', [[], [1], [1, 2, 3]])
    def test_storage_records_spec_with_some(self, indexes, pks):
        assert_that(
            indexes.storage_records_spec('primary_key', *pks),
            has_properties(index_name='primary_key', score_intervals=all_of(
                *[has_item(is_interval_containing(pk)) for pk in pks])))

    def test_storage_records_spec_with_all(self, indexes):
        assert_that(
            indexes.storage_records_spec(
                'primary_key', 'doesnt', 'matter', all_primary_keys=True),
            has_properties(
                index_name='primary_key',
                score_intervals=[tc.Interval(float('-inf'), float('inf'))]))

    def test_storage_records_spec_recheck_predicate(self, indexes):
        spec = indexes.storage_records_spec('primary_key', 1, 2)
        assert len(set([hash(1), hash(2), hash(3)])) == 3
        assert spec.recheck_predicate({'pk': 1})
        assert spec.recheck_predicate({'pk': 2})
        assert not spec.recheck_predicate({'pk': 3})

    @pytest.mark.parametrize('pks', [[], [1], [1, 2, 3]])
    def test_db_records_spec_with_some(self, indexes, pks):
        assert_that(
            indexes.db_records_spec('primary_key', *pks),
            has_properties(query='query_some', args=(tuple(pks),)))

    def test_db_records_spec_with_all(self, indexes):
        assert_that(
            indexes.db_records_spec(
                'primary_key', 'doesnt', 'matter', all_primary_keys=True),
            has_properties(query='query_all', args=()))

    def test_prepare_all_to_all(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        assert_that(adj, has_properties(expire_spec=None, new_spec=None))

    @pytest.mark.parametrize('pks', [[], [1, 2]])
    def test_prepare_all_to_some(self, indexes, pks):
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', *pks)
        expected_expire_intervals = [tc.Interval(float('-inf'), float('inf'))]
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=expected_expire_intervals),
                new_spec=has_properties(
                    query='query_some', args=(tuple(pks),))))

    @pytest.mark.parametrize('pks', [[], [1, 2]])
    def test_prepare_some_to_all(self, indexes, pks):
        adj = indexes.prepare_adjustment('primary_key', *pks)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        assert_that(
            adj, has_properties(
                expire_spec=None,
                new_spec=has_properties(query='query_all', args=())))

    @pytest.mark.parametrize('pks1', [[], [1, 2]])
    @pytest.mark.parametrize('pks2', [[], [3, 4]])
    def test_prepare_some_to_some(self, indexes, pks1, pks2):
        adj = indexes.prepare_adjustment('primary_key', *pks1)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', *pks2)
        expected_expire_intervals = all_of(
            *[has_item(is_interval_containing(pk)) for pk in pks1])
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=expected_expire_intervals),
                new_spec=has_properties(
                    query='query_some', args=(tuple(pks2),))))

    def test_covers_all(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        assert indexes.covers('primary_key')
        assert indexes.covers('primary_key', 1)
        assert indexes.covers('primary_key', 1, '2')
        assert indexes.covers('primary_key', all_primary_keys=True)

    def test_covers_some(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', 1, 2)
        indexes.commit_adjustment(adj)
        assert indexes.covers('primary_key')
        assert indexes.covers('primary_key', 1)
        assert indexes.covers('primary_key', 1, 2)
        assert not indexes.covers('primary_key', 3)
        assert not indexes.covers('primary_key', all_primary_keys=True)

    def test_commit_all_to_all(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        assert indexes.covers('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        assert indexes.covers('primary_key', all_primary_keys=True)

    def test_commit_all_to_some(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', 1)
        assert indexes.covers('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        assert not indexes.covers('primary_key', all_primary_keys=True)
        assert indexes.covers('primary_key', 1)

    def test_commit_some_to_all(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', 1)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', all_primary_keys=True)
        assert not indexes.covers('primary_key', all_primary_keys=True)
        indexes.commit_adjustment(adj)
        assert indexes.covers('primary_key', all_primary_keys=True)

    def test_commit_some_to_some(self, indexes):
        adj = indexes.prepare_adjustment('primary_key', 1)
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment('primary_key', 2)
        assert indexes.covers('primary_key', 1)
        assert not indexes.covers('primary_key', 2)
        indexes.commit_adjustment(adj)
        assert not indexes.covers('primary_key', 1)
        assert indexes.covers('primary_key', 2)
