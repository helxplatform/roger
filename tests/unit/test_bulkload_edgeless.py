"""Bulk loading nodes with no edges must fail loudly.

A build whose edge csvs had not been committed yet used to load nodes only
and report success, which is how the graph ended up with 3.9M nodes and zero
relationships.
"""
import os
import pytest

from roger.core.bulkload import BulkLoad
from roger.config import config


def _bulk(tmp_path, nodes=(), edges=()):
    for kind, names in (('nodes', nodes), ('edges', edges)):
        d = tmp_path / 'knowledge_graph_build' / f'CreateBulkLoad{kind.title()}' / kind
        d.mkdir(parents=True, exist_ok=True)
        for n in names:
            (d / n).write_text('id\n1\n')
    return tmp_path


def test_nodes_without_edges_raises(tmp_path):
    _bulk(tmp_path, nodes=['biolink~Gene.csv-0-1'])
    with pytest.raises(ValueError, match='edgeless'):
        BulkLoad(config).insert(input_data_path=tmp_path)


def test_nodes_with_edges_passes_the_guard(tmp_path):
    _bulk(tmp_path, nodes=['biolink~Gene.csv-0-1'],
          edges=['biolink~treats.csv-0-1'])
    # Gets past the guard and fails later trying to reach redis, which is
    # enough: the guard is what this test is about.
    with pytest.raises(Exception) as exc:
        BulkLoad(config).insert(input_data_path=tmp_path)
    assert 'edgeless' not in str(exc.value)
