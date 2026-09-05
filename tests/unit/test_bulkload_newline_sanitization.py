"""A bare \\r (no paired \\n) in a node's description/name still reads as a
line break under universal newlines. Both our own tooling and the
falkordb_bulk_loader CSV parser split on it, turning one row into two and
corrupting the column count -- seen in dbGaP codebook text pasted in with
stray CRs (a Study "activitybk" field).
"""
from roger.core.bulkload import BulkLoad
from roger.config import config


def test_bare_cr_in_description_does_not_split_the_row(tmp_path, monkeypatch):
    from roger.core import bulkload as bulkload_mod

    node = {
        'id': 'HDP00066:activitybk',
        'category': ['biolink:Study'],
        'name': 'activitybk',
        'description': 'SECTION H. READ Scale\r Ask: do you read to your child?',
    }
    leaf_class = 'biolink:Study'
    schema = {
        leaf_class: {
            'category': 'list', 'name': 'str',
            'description': 'str', 'id': 'str',
        }
    }

    monkeypatch.setattr(bulkload_mod.storage, 'merged_objects',
                        lambda kind, path=None: 'nodes.jsonl')
    monkeypatch.setattr(bulkload_mod.storage, 'json_line_iter',
                        lambda path: iter([node]))
    monkeypatch.setattr(bulkload_mod.storage, 'read_schema',
                        lambda schema_type, path=None: schema)
    monkeypatch.setattr(bulkload_mod.storage, 'bulk_path',
                        lambda name, path=None: str(tmp_path / name))

    class FakeBiolink:
        def get_leaf_class(self, names):
            return leaf_class

    bulk = BulkLoad(FakeBiolink(), config=config)
    bulk.create_nodes_csv_file(input_data_path=None, output_data_path=None)

    out_file = tmp_path / 'nodes' / f"{leaf_class.replace(':', '~')}.csv-0-1"
    lines = out_file.read_bytes().split(b'\n')
    lines = [l for l in lines if l]
    assert len(lines) == 2, lines  # header + one data row, not split in two
    header, row = (l.decode().split('\x1e') for l in lines)
    assert len(header) == len(row) == 4
    assert b'\r' not in out_file.read_bytes()
