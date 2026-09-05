"""crawl_tranql threads whole input files; failures must not be swallowed.

The dir loop is where the bulk of crawl concurrency comes from -- one file
per annotated input, tens of thousands of them for a dbGaP dataset. Two
things must hold: every file gets processed exactly once regardless of
worker count, and a file that raises fails the task rather than quietly
producing no output.
"""
import threading
import types

import pytest


def make_pipeline(workers, crawl_one):
    """A stand-in carrying only what crawl_tranql touches."""
    from roger.pipelines.base import DugPipeline

    pipeline = types.SimpleNamespace()
    pipeline.config = types.SimpleNamespace(
        indexing=types.SimpleNamespace(crawl_file_workers=workers))
    pipeline.log_stream = types.SimpleNamespace(getvalue=lambda: '')
    pipeline.crawl_one_file = crawl_one
    pipeline.crawl_tranql = types.MethodType(
        DugPipeline.crawl_tranql.__wrapped__
        if hasattr(DugPipeline.crawl_tranql, '__wrapped__')
        else DugPipeline.crawl_tranql, pipeline)
    return pipeline


FILES = [f"/in/file{i}/concepts.txt" for i in range(12)]


@pytest.mark.parametrize("workers", [1, 4, 8])
def test_every_file_processed_once(workers, monkeypatch, tmp_path):
    import roger.pipelines.base as base

    seen = []
    lock = threading.Lock()

    def crawl_one(file_, output_data_path=None):
        with lock:
            seen.append(file_)

    monkeypatch.setattr(base.storage, 'clear_dir', lambda *a, **k: None)
    pipeline = make_pipeline(workers, crawl_one)
    pipeline.crawl_tranql(concept_files=list(FILES),
                          output_data_path=str(tmp_path))

    assert sorted(seen) == sorted(FILES)
    assert len(seen) == len(FILES)


def test_one_bad_file_fails_the_task(monkeypatch, tmp_path):
    import roger.pipelines.base as base

    def crawl_one(file_, output_data_path=None):
        if file_.endswith("file7/concepts.txt"):
            raise ValueError("bad pickle")

    monkeypatch.setattr(base.storage, 'clear_dir', lambda *a, **k: None)
    pipeline = make_pipeline(4, crawl_one)
    with pytest.raises(ValueError, match="bad pickle"):
        pipeline.crawl_tranql(concept_files=list(FILES),
                              output_data_path=str(tmp_path))


def test_worker_count_is_capped_by_file_count(monkeypatch, tmp_path):
    """Two files must not spin up eight threads."""
    import roger.pipelines.base as base

    threads = set()
    lock = threading.Lock()

    def crawl_one(file_, output_data_path=None):
        with lock:
            threads.add(threading.current_thread().name)

    monkeypatch.setattr(base.storage, 'clear_dir', lambda *a, **k: None)
    pipeline = make_pipeline(8, crawl_one)
    pipeline.crawl_tranql(concept_files=FILES[:2],
                          output_data_path=str(tmp_path))
    assert len(threads) <= 2
