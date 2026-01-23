from urllib.parse import quote, urlparse
import os
import threading
from contextlib import contextmanager
import httpx

VALID_PROXY_SCHEMES = {"http", "https", "socks5"}


def _build_proxy_url(scheme, host, port, username=None, password=None):
    if not scheme or not host or not port:
        return ""
    if username is not None and password is not None:
        user = quote(str(username), safe="")
        pwd = quote(str(password), safe="")
        return f"{scheme}://{user}:{pwd}@{host}:{port}"
    return f"{scheme}://{host}:{port}"


def _parse_host_port(text):
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None
    host, sep, port = raw.rpartition(":")
    if not sep:
        return None
    host = host.strip()
    port = port.strip()
    if not host or not port.isdigit():
        return None
    return host, port


def _parse_user_pass(text):
    if not text:
        return None
    raw = text.strip()
    if not raw:
        return None
    user, sep, pwd = raw.partition(":")
    if not sep:
        return None
    user = user.strip()
    pwd = pwd.strip()
    if not user or not pwd:
        return None
    return user, pwd


def normalize_proxy(proxy, default_scheme="http"):
    raw = (proxy or "").strip()
    if not raw:
        return ""
    if default_scheme not in VALID_PROXY_SCHEMES:
        default_scheme = "http"

    if "://" in raw:
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower()
        port = None
        if scheme in VALID_PROXY_SCHEMES:
            try:
                port = parsed.port
            except ValueError:
                port = None
        if scheme in VALID_PROXY_SCHEMES and parsed.hostname and port:
            return _build_proxy_url(
                scheme,
                parsed.hostname,
                port,
                parsed.username,
                parsed.password,
            )
        if scheme in VALID_PROXY_SCHEMES:
            remainder = raw.split("://", 1)[1]
            return normalize_proxy(remainder, default_scheme=scheme)
        return ""

    if "@" in raw:
        left, right = raw.split("@", 1)
        left_host = _parse_host_port(left)
        right_host = _parse_host_port(right)
        left_user = _parse_user_pass(left)
        right_user = _parse_user_pass(right)
        if left_host and right_user:
            host, port = left_host
            user, pwd = right_user
            return _build_proxy_url(default_scheme, host, port, user, pwd)
        if right_host and left_user:
            host, port = right_host
            user, pwd = left_user
            return _build_proxy_url(default_scheme, host, port, user, pwd)
        return ""

    parts = raw.split(":")
    if len(parts) == 2:
        host_port = _parse_host_port(raw)
        if host_port:
            host, port = host_port
            return _build_proxy_url(default_scheme, host, port)
        return ""

    if len(parts) == 4:
        if parts[1].isdigit():
            host, port, user, pwd = parts
            return _build_proxy_url(default_scheme, host, port, user, pwd)
        if parts[3].isdigit():
            user, pwd, host, port = parts
            return _build_proxy_url(default_scheme, host, port, user, pwd)
        return ""

    return ""


def is_proxy_valid(proxy):
    return bool(normalize_proxy(proxy))


def build_httpx_proxies(proxy, default_scheme="http"):
    normalized = normalize_proxy(proxy, default_scheme=default_scheme)
    if not normalized:
        return None
    return {
        "http://": normalized,
        "https://": normalized,
    }


_HTTPX_SUPPORTS_PROXIES = "proxies" in httpx.Client.__init__.__code__.co_varnames
_PROXY_ENV_LOCK = threading.Lock()


@contextmanager
def httpx_client(proxy=None, **kwargs):
    proxies = build_httpx_proxies(proxy)
    if proxies and _HTTPX_SUPPORTS_PROXIES:
        with httpx.Client(proxies=proxies, **kwargs) as client:
            yield client
        return
    if proxies and not _HTTPX_SUPPORTS_PROXIES:
        with _PROXY_ENV_LOCK:
            old_http = os.environ.get("HTTP_PROXY")
            old_https = os.environ.get("HTTPS_PROXY")
            os.environ["HTTP_PROXY"] = proxies["http://"]
            os.environ["HTTPS_PROXY"] = proxies["https://"]
            try:
                with httpx.Client(trust_env=True, **kwargs) as client:
                    yield client
            finally:
                if old_http is None:
                    os.environ.pop("HTTP_PROXY", None)
                else:
                    os.environ["HTTP_PROXY"] = old_http
                if old_https is None:
                    os.environ.pop("HTTPS_PROXY", None)
                else:
                    os.environ["HTTPS_PROXY"] = old_https
        return
    with httpx.Client(**kwargs) as client:
        yield client
