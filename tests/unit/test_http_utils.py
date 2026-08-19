import socket
import threading

import pytest
from requests_cache import CachedSession

from roger.config import AnnotationConfig
from roger.utils.http_utils import TimeoutRetryAdapter, harden_session


@pytest.fixture
def blackhole_server():
    """A server whose first connection goes silent, like the observed failure.

    It accepts the request and acknowledges it at the TCP layer, then never
    responds and never closes. Later connections are answered normally, which
    matches production: the backends stayed healthy while one pooled
    connection was silently broken.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', 0))
    srv.listen(8)
    state = {'attempts': 0, 'held': []}
    body = b'{"denotations":[]}'

    def serve():
        while True:
            try:
                conn, _ = srv.accept()
            except OSError:
                return
            conn.recv(65535)
            state['attempts'] += 1
            if state['attempts'] == 1:
                state['held'].append(conn)
                continue
            conn.sendall(
                b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n'
                b'Content-Length: %d\r\n\r\n%s' % (len(body), body))
            conn.close()

    threading.Thread(target=serve, daemon=True).start()
    yield f"http://127.0.0.1:{srv.getsockname()[1]}/annotate/", state
    for conn in state['held']:
        conn.close()
    srv.close()


def _session(tmp_path, read_timeout=0.3, retries=2):
    return harden_session(
        CachedSession(cache_name=str(tmp_path / 'cache')),
        connect_timeout=0.5,
        read_timeout=read_timeout,
        retries=retries,
        backoff_factor=0,
    )


def test_adapter_mounted_with_timeout(tmp_path):
    session = _session(tmp_path)
    for scheme in ('http://', 'https://'):
        adapter = session.get_adapter(scheme)
        assert isinstance(adapter, TimeoutRetryAdapter)
        assert adapter._timeout == (0.5, 0.3)


def test_keepalive_enabled(tmp_path):
    session = _session(tmp_path)
    options = session.get_adapter(
        'http://').poolmanager.connection_pool_kw['socket_options']
    assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options


def test_silent_peer_does_not_hang_forever(tmp_path, blackhole_server):
    """Without a timeout this call never returns; that caused multi-day stalls."""
    url, _ = blackhole_server
    session = _session(tmp_path, retries=0)
    with pytest.raises(Exception) as exc:
        session.post(url, json={'text': 'x'})  # no timeout passed, as in dug
    assert 'Timeout' in type(exc.value).__name__ or 'Connection' in type(
        exc.value).__name__


def test_retry_recovers_from_silent_peer(tmp_path, blackhole_server):
    """A broken pooled connection should be retried on a fresh one, not fail."""
    url, state = blackhole_server
    session = _session(tmp_path, retries=2)
    response = session.post(url, json={'text': 'x'})
    assert response.status_code == 200
    assert state['attempts'] >= 2


def test_timeouts_coerced_from_environment_strings():
    """Env vars arrive as strings; urllib3 rejects a string timeout outright."""
    conf = AnnotationConfig(
        http_connect_timeout='5',
        http_read_timeout='45.5',
        http_retries='7',
        http_retry_backoff='2',
    )
    assert conf.http_connect_timeout == 5.0
    assert conf.http_read_timeout == 45.5
    assert conf.http_retries == 7
    assert conf.http_retry_backoff == 2.0
    assert isinstance(conf.http_retries, int)
