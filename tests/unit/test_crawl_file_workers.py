"""crawl_tranql threads whole input files; one bad file must not sink the rest.

The dir loop is where the bulk of crawl concurrency comes from -- one file
per annotated input, tens of thousands of them for a dbGaP dataset. Every
file gets processed exactly once regardless of worker count, and a file
that raises (e.g. a corrupted input) is logged to a manifest and skipped
rather than failing the whole task -- input is re-pulled fresh from lakefs
on every retry, so a task-ending exception here means the same file kills
every future retry too.
"""
import json
import os
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
    # nothing exists yet in these tests -- every file is pending, same as
    # the pre-skip-check behavior this test suite was written against
    pipeline.crawl_is_complete = lambda f, output_data_path: False
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


def test_one_bad_file_is_skipped_not_fatal(monkeypatch, tmp_path):
    """A corrupted input file must not sink the other pending files, and
    must not fail the task -- input is re-pulled fresh from lakefs on every
    retry, so a fatal exception here would fail identically forever."""
    import roger.pipelines.base as base

    seen = []
    lock = threading.Lock()

    def crawl_one(file_, output_data_path=None):
        if file_.endswith("file7/concepts.txt"):
            raise ValueError("bad pickle")
        with lock:
            seen.append(file_)

    monkeypatch.setattr(base.storage, 'clear_dir', lambda *a, **k: None)
    pipeline = make_pipeline(4, crawl_one)
    pipeline.crawl_tranql(concept_files=list(FILES),
                          output_data_path=str(tmp_path))

    bad_file = "/in/file7/concepts.txt"
    assert sorted(seen) == sorted(set(FILES) - {bad_file})

    manifest_path = os.path.join(str(tmp_path), base.CRAWL_FAILED_FILES_MANIFEST)
    with open(manifest_path) as f:
        failed = json.load(f)
    assert failed == [{"file": bad_file, "error": "bad pickle"}]


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


def test_already_crawled_files_are_skipped(monkeypatch, tmp_path):
    """A resumed try must not redo files an earlier try already crawled --
    TranQL's response cache makes that cheap, not free, and it stands
    between a large dataset and any real new progress."""
    import roger.pipelines.base as base

    seen = []

    def crawl_one(file_, output_data_path=None):
        seen.append(file_)

    monkeypatch.setattr(base.storage, 'clear_dir', lambda *a, **k: None)
    pipeline = make_pipeline(4, crawl_one)
    already_done = set(FILES[:5])
    pipeline.crawl_is_complete = lambda f, output_data_path: f in already_done

    pipeline.crawl_tranql(concept_files=list(FILES),
                          output_data_path=str(tmp_path))

    assert sorted(seen) == sorted(set(FILES) - already_done)


def test_crawl_is_complete_checks_the_real_pipeline(tmp_path):
    """Exercise the actual DugPipeline method, not just the stand-in used
    above -- this is what would have caught crawl_output_path drifting
    from where crawl_concepts actually writes."""
    from roger.pipelines.base import DugPipeline

    concept_file = "/in/phs000123.v1.data_dict/concepts.txt"
    output_data_path = str(tmp_path)

    assert DugPipeline.crawl_is_complete(concept_file, output_data_path) is False

    path = DugPipeline.crawl_output_path(concept_file, output_data_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write('{}')

    assert DugPipeline.crawl_is_complete(concept_file, output_data_path) is True
