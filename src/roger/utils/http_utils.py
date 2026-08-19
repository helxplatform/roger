"Hardening for the outbound HTTP session used by the annotation pipeline"

import socket

from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPConnection
from urllib3.util.retry import Retry

from roger.logger import get_logger

log = get_logger()

# The annotation services are read-only lookups, so replaying a POST is safe.
RETRY_METHODS = frozenset(["GET", "POST"])
RETRY_STATUSES = (429, 502, 503, 504)

# Keepalive probing: start after 60s idle, then every 15s, give up after 4.
KEEPALIVE_IDLE = 60
KEEPALIVE_INTERVAL = 15
KEEPALIVE_COUNT = 4


def keepalive_socket_options():
    """Socket options enabling TCP keepalive on top of urllib3's defaults.

    A socket with no keepalive has no timer of any kind, so a peer that
    disappears without sending a FIN or RST is invisible to the client and a
    blocking read waits forever. Keepalive gives the kernel its own way to
    notice, independent of any application level timeout.
    """
    options = list(HTTPConnection.default_socket_options)
    options.append((socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1))
    # These three are Linux only. Dev machines are often macOS, where the
    # names are absent or spelled differently, so probe rather than assume.
    for name, value in (("TCP_KEEPIDLE", KEEPALIVE_IDLE),
                        ("TCP_KEEPINTVL", KEEPALIVE_INTERVAL),
                        ("TCP_KEEPCNT", KEEPALIVE_COUNT)):
        option = getattr(socket, name, None)
        if option is not None:
            options.append((socket.IPPROTO_TCP, option, value))
    return options


class TimeoutRetryAdapter(HTTPAdapter):
    """Adapter applying a default timeout and TCP keepalive to every request.

    requests has no session level timeout, so any call site that omits one
    blocks indefinitely. Defaulting it here covers all of them, including the
    call sites inside dug, none of which pass a timeout.
    """

    def __init__(self, *args, timeout=None, **kwargs):
        self._timeout = timeout
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs.setdefault('socket_options', keepalive_socket_options())
        super().init_poolmanager(*args, **kwargs)

    def send(self, request, **kwargs):
        if kwargs.get('timeout') is None:
            kwargs['timeout'] = self._timeout
        return super().send(request, **kwargs)


def harden_session(session, connect_timeout, read_timeout, retries,
                   backoff_factor):
    """Mount a timeout/retry/keepalive adapter on `session` and return it.

    Annotation talks to several in-cluster services over long lived
    keep-alive connections. When one of those connections breaks silently,
    with the peer gone but no FIN or RST arriving, an unbounded read hangs
    until a human notices, which has taken days. A read timeout turns that
    into an error, and the retries absorb the transient case so that a single
    blip does not fail a crawl that takes hours.

    Retries matter here specifically because dug's Crawler.annotate_elements
    has no per-element error handling, so an exception that escapes discards
    the progress of the whole run.
    """
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=RETRY_STATUSES,
        allowed_methods=RETRY_METHODS,
        raise_on_status=False,
    )
    adapter = TimeoutRetryAdapter(
        max_retries=retry,
        timeout=(connect_timeout, read_timeout),
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    log.info(
        "HTTP session hardened: connect_timeout=%ss read_timeout=%ss "
        "retries=%s backoff_factor=%s",
        connect_timeout, read_timeout, retries, backoff_factor)
    return session
