"""Unit tests for incremental (delta) lakefs ingestion in roger.tasks.

These tests need airflow/avalon/lakefs_sdk importable (e.g. inside the
Roger image); they skip cleanly elsewhere.
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

tasks = pytest.importorskip("roger.tasks")


def make_ti(dag_id="annotate_and_index",
            task_id="tg.annotate_topmed_files",
            run_id="manual__1", try_number=1, upstream_ids=()):
    return SimpleNamespace(
        dag_id=dag_id, task_id=task_id, run_id=run_id,
        try_number=try_number,
        task=SimpleNamespace(upstream_task_ids=list(upstream_ids)))


def paged_diff(entries, page_size=2):
    """Fake refs_api.diff_refs returning paginated results."""
    pages = ([entries[i:i + page_size]
              for i in range(0, len(entries), page_size)] or [[]])

    def fetch(**kwargs):
        idx = int(kwargs.get('after') or 0)
        return SimpleNamespace(
            results=pages[idx],
            pagination=SimpleNamespace(has_more=idx + 1 < len(pages),
                                       next_offset=idx + 1))
    return fetch


def paged_objects(paths, page_size=2):
    """Fake objects_api.list_objects returning paginated object listings."""
    pages = ([paths[i:i + page_size]
              for i in range(0, len(paths), page_size)] or [[]])

    def fetch(**kwargs):
        idx = int(kwargs.get('after') or 0)
        return SimpleNamespace(
            results=[SimpleNamespace(path=p) for p in pages[idx]],
            pagination=SimpleNamespace(has_more=idx + 1 < len(pages),
                                       next_offset=idx + 1))
    return fetch


class FakeClient:
    def __init__(self, diff_entries=None, tip="tipA", objects=None):
        self.downloads = []
        self._client = SimpleNamespace(
            refs_api=SimpleNamespace(
                diff_refs=paged_diff(diff_entries or []),
                log_commits=lambda repository, ref, amount:
                    SimpleNamespace(results=[SimpleNamespace(id=tip)])),
            objects_api=SimpleNamespace(
                list_objects=paged_objects(objects or [])))

    def download_files(self, remote_files, local_path, repository,
                       branch_or_commit_id):
        self.downloads.append((tuple(remote_files), local_path, repository,
                               branch_or_commit_id))


@pytest.fixture
def lakefs_env(monkeypatch, tmp_path):
    monkeypatch.setenv("ROGER_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(tasks.config.lakefs_config, "enabled", True)
    monkeypatch.setattr(tasks.config.lakefs_config, "repo", "roger-out")
    monkeypatch.setattr(tasks.config.lakefs_config, "branch", "main")
    return tmp_path


def diff_entry(diff_type, path):
    return SimpleNamespace(type=diff_type, path=path)


def test_incremental_state_key():
    key = tasks.incremental_state_key(
        "annotate_and_index", "tg.annotate_topmed_files", "topmed", "v2.0")
    assert key == ("roger_incr::annotate_and_index::"
                   "tg.annotate_topmed_files::topmed@v2.0")
    other = tasks.incremental_state_key(
        "annotate_and_index", "tg.annotate_topmed_files", "topmed", "v3.0")
    assert key != other


def test_resolve_ref_tip():
    assert tasks.resolve_ref_tip(FakeClient(tip="abc123"), 'r', 'v1.0') \
        == "abc123"
    empty = FakeClient()
    empty._client.refs_api.log_commits = (
        lambda repository, ref, amount: SimpleNamespace(results=[]))
    # empty history: fall back to the ref itself
    assert tasks.resolve_ref_tip(empty, 'r', 'v1.0') == 'v1.0'


def test_get_changed_files_buckets_and_prefix():
    entries = [
        diff_entry('added', 'a/task/f1'),
        diff_entry('changed', 'a/task/f2'),
        diff_entry('removed', 'a/task/f3'),
        diff_entry('added', 'a/task_b/f4'),
        diff_entry('conflict', 'a/task/f5'),
    ]
    client = FakeClient(diff_entries=entries)

    # prefix without trailing slash must not match the 'a/task_b' sibling
    changes = tasks.get_changed_files(client, 'repo', 'c1', 'c2',
                                      prefixes=['a/task'])
    assert changes == {'added': ['a/task/f1'], 'changed': ['a/task/f2'],
                       'removed': ['a/task/f3']}

    # '*' or no prefixes means no filtering; conflict entries ignored
    for prefixes in (['*'], None):
        changes = tasks.get_changed_files(client, 'repo', 'c1', 'c2',
                                          prefixes=prefixes)
        assert changes['added'] == ['a/task/f1', 'a/task_b/f4']
        assert changes['removed'] == ['a/task/f3']


def test_setup_input_data_skips_when_no_change(monkeypatch, lakefs_env):
    client = FakeClient(tip="tipA")
    monkeypatch.setattr(tasks, "init_lakefs_client", lambda config: client)
    monkeypatch.setattr(tasks, "_get_last_consumed", lambda key: "tipA")
    get_files = MagicMock()
    monkeypatch.setattr(tasks, "get_files", get_files)

    context = {'ti': make_ti(), 'params': {'incremental': True}}
    exec_conf = {'repos': [{'repo': 'topmed', 'branch': 'v2.0', 'path': '*'}]}
    with pytest.raises(tasks.AirflowSkipException):
        tasks.setup_input_data(context, exec_conf)
    get_files.assert_not_called()
    assert client.downloads == []


def test_setup_input_data_first_run_full_download(monkeypatch, lakefs_env):
    client = FakeClient(tip="tipA")
    monkeypatch.setattr(tasks, "init_lakefs_client", lambda config: client)
    monkeypatch.setattr(tasks, "_get_last_consumed", lambda key: None)
    get_files = MagicMock()
    monkeypatch.setattr(tasks, "get_files", get_files)

    ti = make_ti()
    context = {'ti': ti, 'params': {'incremental': True}}
    exec_conf = {'repos': [{'repo': 'topmed', 'branch': 'v2.0', 'path': '*'}]}
    tasks.setup_input_data(context, exec_conf)

    get_files.assert_called_once()
    call = get_files.call_args.kwargs
    assert call['branch'] == "tipA"          # pinned to resolved tip
    assert call['changes_only'] is False
    assert call['repo'] == 'topmed'

    state = tasks.read_state_file(ti)
    key = tasks.incremental_state_key(ti.dag_id, ti.task_id, 'topmed', 'v2.0')
    assert state['entries'][key]['commit_id'] == "tipA"


def test_setup_input_data_incremental_diff_download(monkeypatch, lakefs_env):
    entries = [diff_entry('added', 'f1'), diff_entry('removed', 'f2')]
    client = FakeClient(diff_entries=entries, tip="tipNew")
    monkeypatch.setattr(tasks, "init_lakefs_client", lambda config: client)
    monkeypatch.setattr(tasks, "_get_last_consumed", lambda key: "tipOld")
    get_files = MagicMock()
    monkeypatch.setattr(tasks, "get_files", get_files)

    ti = make_ti()
    context = {'ti': ti, 'params': {'incremental': True}}
    exec_conf = {'repos': [{'repo': 'topmed', 'branch': 'v2.0', 'path': '*'}]}
    tasks.setup_input_data(context, exec_conf)

    get_files.assert_not_called()
    assert len(client.downloads) == 1
    remote_files, _local, repo, ref = client.downloads[0]
    assert remote_files == ('f1',)
    assert repo == 'topmed'
    assert ref == "tipNew"

    state = tasks.read_state_file(ti)
    assert state['removed'] == {'topmed': ['f2']}
    key = tasks.incremental_state_key(ti.dag_id, ti.task_id, 'topmed', 'v2.0')
    assert state['entries'][key]['commit_id'] == "tipNew"


def test_find_sibling_files():
    # data dict changed in a study dir; GapExchange sibling lives alongside it
    objects = ['s/phs1.v1/data_dict.xml',
               's/phs1.v1/GapExchange_phs1.v1.xml',
               's/phs1.v1/other.xml']
    client = FakeClient(objects=objects)
    siblings = tasks.find_sibling_files(
        client, 'repo', 'tip', ['s/phs1.v1/data_dict.xml'])
    assert siblings == ['s/phs1.v1/GapExchange_phs1.v1.xml']

    # already-downloaded marker is not duplicated
    assert tasks.find_sibling_files(
        client, 'repo', 'tip',
        ['s/phs1.v1/GapExchange_phs1.v1.xml']) == []


def test_setup_input_data_incremental_pulls_gap_exchange(monkeypatch,
                                                         lakefs_env):
    entries = [diff_entry('changed', 's/phs1.v1/data_dict.xml')]
    objects = ['s/phs1.v1/data_dict.xml',
               's/phs1.v1/GapExchange_phs1.v1.xml']
    client = FakeClient(diff_entries=entries, tip="tipNew", objects=objects)
    monkeypatch.setattr(tasks, "init_lakefs_client", lambda config: client)
    monkeypatch.setattr(tasks, "_get_last_consumed", lambda key: "tipOld")
    monkeypatch.setattr(tasks, "get_files", MagicMock())

    context = {'ti': make_ti(), 'params': {'incremental': True}}
    exec_conf = {'repos': [{'repo': 'topmed', 'branch': 'v2.0', 'path': '*'}]}
    tasks.setup_input_data(context, exec_conf)

    remote_files, _local, _repo, _ref = client.downloads[0]
    assert set(remote_files) == {'s/phs1.v1/data_dict.xml',
                                 's/phs1.v1/GapExchange_phs1.v1.xml'}


def test_setup_input_data_manual_override_precedence(monkeypatch, lakefs_env):
    client = FakeClient()
    monkeypatch.setattr(tasks, "init_lakefs_client", lambda config: client)
    last_consumed = MagicMock()
    monkeypatch.setattr(tasks, "_get_last_consumed", last_consumed)
    get_files = MagicMock()
    monkeypatch.setattr(tasks, "get_files", get_files)

    ti = make_ti()
    context = {'ti': ti, 'params': {
        'repository_id': 'ext-repo', 'branch_name': 'b',
        'commitid_from': 'c1', 'commitid_to': 'c2', 'incremental': True}}
    tasks.setup_input_data(context, {'repos': []})

    last_consumed.assert_not_called()
    get_files.assert_called_once()
    call = get_files.call_args.kwargs
    assert call['repo'] == 'ext-repo'
    assert call['branch'] == 'b'
    assert call['changes_only'] is True
    assert call['changes_from'] == 'c1'
    assert call['changes_to'] == 'c2'
    assert tasks.read_state_file(ti) == {}


def test_record_state_callback_sets_variables_and_cleans(monkeypatch,
                                                         lakefs_env):
    ti = make_ti()
    key = tasks.incremental_state_key(ti.dag_id, ti.task_id, 'topmed', 'v2.0')
    tasks.write_state_file(ti, {
        'entries': {key: {'repo': 'topmed', 'branch': 'v2.0',
                          'commit_id': 'tipA'}},
        'removed': {}})
    state_file = tasks.get_state_file_path(ti)
    assert os.path.isfile(state_file)

    recorded = []
    monkeypatch.setattr(
        tasks, "Variable",
        SimpleNamespace(set=lambda k, v: recorded.append((k, v))))

    tasks.record_state_callback({'ti': ti})

    assert recorded == [(key, 'tipA')]
    assert not os.path.exists(state_file)


def test_state_file_noop_when_lakefs_disabled(monkeypatch):
    monkeypatch.setattr(tasks.config.lakefs_config, "enabled", False)
    ti = make_ti()
    assert tasks.get_state_file_path(ti) is None
    tasks.write_state_file(ti, {'entries': {}})  # must not raise
    assert tasks.read_state_file(ti) == {}
