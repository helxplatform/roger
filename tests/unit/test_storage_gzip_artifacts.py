"""write_object gzips .txt/.jsonl artifacts now -- the repeated CURIEs and
biolink categories in crawl-stage KG-answer JSON compress 5-10x, which is
the difference between a large dataset's crawl fitting on the PVC and
hitting ENOSPC mid-run. read_object must still read artifacts committed
before this was added.
"""
import gzip

from roger.core import storage


def test_txt_artifact_round_trips_through_gzip(tmp_path):
    path = str(tmp_path / 'concepts.txt')
    text = '{"id": "UMLS:C1"}' * 100

    storage.write_object(text, path)

    assert open(path, 'rb').read(2) == b'\x1f\x8b'
    assert storage.read_object(path) == text


def test_txt_artifact_backward_compat_with_plain_text(tmp_path):
    path = tmp_path / 'concepts.txt'
    text = '{"id": "UMLS:C1"}'
    path.write_text(text, encoding='utf-8')

    assert storage.read_object(str(path)) == text
