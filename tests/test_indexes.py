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

import operator as op

from hamcrest import *
import pytest

import tablecache as tc
from tests.helpers import is_interval_containing


class TestAllIndexes:
    @pytest.fixture
    def indexes(self):
        return tc.AllIndexes(op.itemgetter('pk1', 'pk2'), 'query_all')

    def test_index_names(self, indexes):
        assert indexes.index_names == frozenset(['all'])

    def test_storage_records_spec(self, indexes):
        assert_that(
            indexes.storage_records_spec(indexes.IndexSpec('all')),
            has_properties(
                index_name='all',
                score_intervals=[tc.Interval.everything()]))

    def test_storage_records_spec_recheck_predicate(self, indexes):
        spec = indexes.storage_records_spec(
            indexes.storage_records_spec(
                indexes.IndexSpec(
                    'all', recheck_predicate=lambda r: r['x'] < 3)))
        assert spec.recheck_predicate({'pk1': 3, 'pk2': '3', 'x': 1})
        assert not spec.recheck_predicate({'pk1': 1, 'pk2': '1', 'x': 3})

    def test_db_records_spec(self, indexes):
        assert_that(
            indexes.db_records_spec(indexes.IndexSpec('all')),
            has_properties(query='query_all', args=()))

    def test_prepare(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.storage_records_spec(indexes.IndexSpec('all')))
        assert_that(adj, has_properties(
            expire_spec=None,
            load_spec=has_properties(query='query_all', args=())))

    def test_covers(self, indexes):
        assert indexes.covers(
            indexes.storage_records_spec(indexes.IndexSpec('all')))


class TestPrimaryKeyIndexes:
    @pytest.fixture
    def indexes(self):
        return tc.PrimaryKeyIndexes(
            op.itemgetter('pk'), 'query_all', 'query_some')

    def test_index_names(self, indexes):
        assert indexes.index_names == frozenset(['primary_key'])

    @pytest.mark.parametrize('pk', [1, '2'])
    def test_score(self, indexes, pk):
        score = indexes.score('primary_key', {'pk': pk, 's': 's'})
        assert score == hash(pk)

    @pytest.mark.parametrize('pks', [[], [1], [1, '2']])
    def test_storage_records_spec_with_some(self, indexes, pks):
        assert_that(
            indexes.storage_records_spec(
                indexes.IndexSpec('primary_key', *pks)),
            has_properties(index_name='primary_key', score_intervals=all_of(
                *[has_item(is_interval_containing(hash(pk)))for pk in pks])))

    def test_storage_records_spec_with_all(self, indexes):
        assert_that(
            indexes.storage_records_spec(indexes.IndexSpec(
                'primary_key', all_primary_keys=True)),
            has_properties(
                index_name='primary_key',
                score_intervals=[tc.Interval.everything()]))

    def test_storage_records_spec_recheck_predicate(self, indexes):
        spec = indexes.storage_records_spec(
            indexes.IndexSpec('primary_key', 1, '2'))
        assert len(set([hash(1), hash('2'), hash(3)])) == 3
        assert spec.recheck_predicate({'pk': 1})
        assert spec.recheck_predicate({'pk': '2'})
        assert not spec.recheck_predicate({'pk': 3})

    @pytest.mark.parametrize('pks', [[], [1], [1, 2]])
    def test_db_records_spec_with_some(self, indexes, pks):
        assert_that(
            indexes.db_records_spec(indexes.IndexSpec('primary_key', *pks)),
            has_properties(
                query='query_some',
                args=contains_exactly(contains_inanyorder(*pks))))

    def test_db_records_spec_with_all(self, indexes):
        assert_that(
            indexes.db_records_spec(indexes.IndexSpec(
                'primary_key', all_primary_keys=True)),
            has_properties(query='query_all', args=()))

    def test_prepare_all_to_all(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        assert_that(adj, has_properties(expire_spec=None, load_spec=None))

    @pytest.mark.parametrize('pks', [[], [1, 2]])
    def test_prepare_all_to_some(self, indexes, pks):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', *pks))
        expected_expire_intervals = [tc.Interval.everything()]
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=expected_expire_intervals),
                load_spec=has_properties(
                    query='query_some',
                    args=contains_exactly(contains_inanyorder(*pks)))))

    @pytest.mark.parametrize('pks', [[], [1, 2]])
    def test_prepare_some_to_all(self, indexes, pks):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', *pks))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        assert_that(
            adj, has_properties(
                expire_spec=None,
                load_spec=has_properties(query='query_all', args=())))

    @pytest.mark.parametrize('pks1', [[], [1, '2']])
    @pytest.mark.parametrize('pks2', [[], [3, '4']])
    def test_prepare_some_to_some(self, indexes, pks1, pks2):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', *pks1))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', *pks2))
        expected_expire_intervals = all_of(
            *[has_item(is_interval_containing(hash(pk))) for pk in pks1])
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=expected_expire_intervals),
                load_spec=has_properties(
                    query='query_some',
                    args=contains_exactly(contains_inanyorder(*pks2)))))

    def test_prepare_some_to_some_retains_overlap(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', 0, '1', 2, '3'))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', 2, '3', 4, '5'))
        expected_expire_intervals = all_of(
            *[has_item(is_interval_containing(hash(pk))) for pk in [0, '1']])
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=expected_expire_intervals),
                load_spec=has_properties(
                    query='query_some',
                    args=contains_exactly(contains_inanyorder(4, '5')))))

    def test_covers_all(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec('primary_key'))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1, '2'))
        assert indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))

    def test_covers_some(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', 1, 2))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec('primary_key'))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1, 2))
        assert not indexes.covers(indexes.IndexSpec('primary_key', 3))
        assert not indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))

    def test_commit_all_to_all(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        assert indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))

    def test_commit_all_to_some(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(indexes.IndexSpec('primary_key', 1))
        assert indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        assert not indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1))

    def test_commit_some_to_all(self, indexes):
        adj = indexes.prepare_adjustment(indexes.IndexSpec('primary_key', 1))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', all_primary_keys=True))
        assert not indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec(
            'primary_key', all_primary_keys=True))

    def test_commit_some_to_some(self, indexes):
        adj = indexes.prepare_adjustment(indexes.IndexSpec('primary_key', 1))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(indexes.IndexSpec('primary_key', 2))
        assert indexes.covers(indexes.IndexSpec('primary_key', 1))
        assert not indexes.covers(indexes.IndexSpec('primary_key', 2))
        indexes.commit_adjustment(adj)
        assert not indexes.covers(indexes.IndexSpec('primary_key', 1))
        assert indexes.covers(indexes.IndexSpec('primary_key', 2))


class TestPrimaryKeyRangeIndexes:
    @pytest.fixture
    def indexes(self):
        return tc.PrimaryKeyRangeIndexes(op.itemgetter('pk'), 'query_range')

    def test_index_names(self, indexes):
        assert indexes.index_names == frozenset(['primary_key'])

    @pytest.mark.parametrize('pk', [-1, 0, 123])
    def test_score(self, indexes, pk):
        assert indexes.score('primary_key', {'pk': pk, 's': 's'}) == pk

    def test_storage_records_spec(self, indexes):
        assert_that(
            indexes.storage_records_spec(
                indexes.IndexSpec('primary_key', ge=-5, lt=3)),
            has_properties(
                index_name='primary_key',
                score_intervals=contains_exactly(tc.Interval(-5, 3))))

    def test_db_records_spec(self, indexes):
        assert_that(
            indexes.db_records_spec(
                indexes.IndexSpec('primary_key', ge=-5, lt=3)),
            has_properties(query='query_range', args=(-5, 3)))

    def test_prepare_no_overlap(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=5, lt=10))
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=contains_exactly(tc.Interval(-5, 3))),
                load_spec=has_properties(query='query_range', args=(5, 10))))

    def test_prepare_overlap(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=0, lt=10))
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=contains_exactly(tc.Interval(-5, 3))),
                load_spec=has_properties(query='query_range', args=(0, 10))))

    def test_prepare_equal(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=contains_exactly(tc.Interval(-5, 3))),
                load_spec=has_properties(query='query_range', args=(-5, 3))))

    def test_prepare_contained(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-4, lt=2))
        assert_that(
            adj, has_properties(
                expire_spec=has_properties(
                    score_intervals=contains_exactly(tc.Interval(-5, 3))),
                load_spec=has_properties(query='query_range', args=(-4, 2))))

    def test_covers(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=-5, lt=3))
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=-4, lt=2))
        assert not indexes.covers(
            indexes.IndexSpec('primary_key', ge=-4, lt=4))
        assert not indexes.covers(
            indexes.IndexSpec('primary_key', ge=-6, lt=2))
        assert not indexes.covers(indexes.IndexSpec('primary_key', ge=4, lt=5))

    def test_commit_no_overlap(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=5, lt=10))
        indexes.commit_adjustment(adj)
        assert not indexes.covers(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=5, lt=10))

    def test_commit_overlap(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=0, lt=10))
        indexes.commit_adjustment(adj)
        assert not indexes.covers(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=0, lt=10))

    def test_commit_equal(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=-5, lt=3))

    def test_commit_contained(self, indexes):
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        indexes.commit_adjustment(adj)
        adj = indexes.prepare_adjustment(
            indexes.IndexSpec('primary_key', ge=-4, lt=2))
        indexes.commit_adjustment(adj)
        assert not indexes.covers(
            indexes.IndexSpec('primary_key', ge=-5, lt=3))
        assert indexes.covers(indexes.IndexSpec('primary_key', ge=-4, lt=2))
