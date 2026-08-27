"""Output must be committed from post_execute, not on_success_callback.

Airflow records end_date and releases downstream tasks before success
callbacks run, so committing there let downstream read the branch before the
output landed. It also only logs exceptions from state-change callbacks, so a
failed merge left the task green.
"""
from unittest import mock

import pytest


def _make_task(**kw):
    from roger import tasks
    with mock.patch.object(tasks, 'PythonOperator', lambda **a: a):
        return tasks.create_python_task(
            mock.MagicMock(), "T", lambda **_: None, **kw)


@pytest.mark.parametrize("no_output_files", [False, True])
def test_commit_runs_from_post_execute(no_output_files, monkeypatch):
    from roger import tasks
    monkeypatch.setattr(tasks.config, 'lakefs_config',
                        mock.MagicMock(enabled=True))
    args = _make_task(no_output_files=no_output_files)
    assert 'post_execute' in args
    assert 'on_success_callback' not in args

    called = []
    target = 'record_state_callback' if no_output_files else 'avalon_commit_callback'
    monkeypatch.setattr(tasks, target, lambda ctx, **k: called.append(ctx))
    # rebuild so the partial closes over the patched function
    args = _make_task(no_output_files=no_output_files)
    args['post_execute']({'ti': 'x'}, None)
    assert called == [{'ti': 'x'}]


def test_post_execute_accepts_the_result_arg(monkeypatch):
    """Airflow calls the hook as (context, result); ours takes context."""
    from roger import tasks
    monkeypatch.setattr(tasks.config, 'lakefs_config',
                        mock.MagicMock(enabled=True))
    monkeypatch.setattr(tasks, 'avalon_commit_callback', lambda ctx, **k: None)
    hook = _make_task()['post_execute']
    hook({'ti': 'x'}, "some result")   # must not raise TypeError
