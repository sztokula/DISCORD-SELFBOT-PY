from urllib.parse import quote, urlparse
from contextlib import contextmanager, asynccontextmanager
import os


def _sanitize_openssl_env():
    # Avoid OpenSSL config/module overrides that can break TLS in curl_cffi on Windows.
    if os.getenv("MASSDM_KEEP_OPENSSL_ENV", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    for key in ("OPENSSL_CONF", "OPENSSL_MODULES"):
        if os.getenv(key):
            os.environ.pop(key, None)


_sanitize_openssl_env()

from curl_cffi import requests as curl_requests
from client_identity import IMPERSONATE_PROFILE, JA3_FINGERPRINT, AKAMAI_FINGERPRINT

try:
    import httpx as _httpx
except Exception:  # pragma: no cover - optional dependency for timeout conversion
    _httpx = None

VALID_PROXY_SCHEMES = {"http", "https", "socks5"}
TRAFFIC_WS = "ws"
TRAFFIC_REST = "rest"
TRAFFIC_EXTERNAL = "external"


def _safe_log(obj, message):
    try:
        logger = getattr(obj, "log", None)
        if callable(logger):
            logger(message)
    except Exception:
        pass


def load_external_proxy(db, default_scheme="http"):
    if not db:
        return None
    try:
        raw = db.get_setting("external_proxy", "")
    except Exception as exc:
        _safe_log(db, f"[Proxy] Failed to read external proxy: {type(exc).__name__}")
        raw = ""
    normalized = normalize_proxy(raw, default_scheme=default_scheme) if raw else ""
    return normalized or None


def resolve_proxy_for_traffic(
    traffic,
    *,
    discord_proxy=None,
    external_proxy=None,
    default_scheme="http",
):
    key = (traffic or "").strip().lower()
    if key in {"external", "ext", "captcha", "openai"}:
        return normalize_proxy(external_proxy, default_scheme=default_scheme) or ""
    if key in {"ws", "gateway", "rest", "discord"}:
        return normalize_proxy(discord_proxy, default_scheme=default_scheme) or ""
    return normalize_proxy(discord_proxy or external_proxy, default_scheme=default_scheme) or ""


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


def _apply_tls_defaults(kwargs):
    if kwargs.get("impersonate") and kwargs.get("impersonate") != IMPERSONATE_PROFILE:
        kwargs["impersonate"] = IMPERSONATE_PROFILE
    else:
        kwargs.setdefault("impersonate", IMPERSONATE_PROFILE)
    if JA3_FINGERPRINT is not None:
        kwargs["ja3"] = JA3_FINGERPRINT
    else:
        kwargs.pop("ja3", None)
    if AKAMAI_FINGERPRINT is not None:
        kwargs["akamai"] = AKAMAI_FINGERPRINT
    else:
        kwargs.pop("akamai", None)
    return kwargs

def _normalize_timeout(timeout):
    if timeout is None:
        return None
    if isinstance(timeout, (int, float)):
        return float(timeout)
    if isinstance(timeout, (tuple, list)) and len(timeout) == 2:
        return tuple(timeout)
    if _httpx and isinstance(timeout, _httpx.Timeout):
        connect = timeout.connect
        read = timeout.read
        if connect is None and read is None:
            return None
        if connect is None:
            connect = read
        if read is None:
            read = connect
        return (connect, read)
    return timeout


def _resolve_cookie_token(cookie_token, logger=None):
    if callable(cookie_token):
        try:
            return cookie_token()
        except Exception as exc:
            if callable(logger):
                logger(f"[Cookies] Failed to resolve cookie token: {type(exc).__name__}")
            return None
    return cookie_token


def _load_cookies_into_session(client, cookies, logger=None):
    if not cookies:
        return
    try:
        if isinstance(cookies, dict):
            client.cookies.update(cookies)
            return
        if isinstance(cookies, list):
            for item in cookies:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if not name:
                    continue
                value = item.get("value", "")
                kwargs = {}
                domain = item.get("domain")
                if domain:
                    kwargs["domain"] = domain
                path = item.get("path")
                if path:
                    kwargs["path"] = path
                expires = item.get("expires")
                if expires is not None:
                    kwargs["expires"] = expires
                secure = item.get("secure")
                if secure is not None:
                    kwargs["secure"] = secure
                client.cookies.set(name, value, **kwargs)
    except Exception as exc:
        if callable(logger):
            logger(f"[Cookies] Failed to load cookies into session: {type(exc).__name__}")
        return


def _dump_cookies_from_session(client, logger=None):
    if not client or not hasattr(client, "cookies"):
        return []
    jar = getattr(client.cookies, "jar", None) or client.cookies
    cookies = []
    try:
        for cookie in jar:
            cookies.append(
                {
                    "name": getattr(cookie, "name", None),
                    "value": getattr(cookie, "value", ""),
                    "domain": getattr(cookie, "domain", None),
                    "path": getattr(cookie, "path", None),
                    "secure": getattr(cookie, "secure", None),
                    "expires": getattr(cookie, "expires", None),
                }
            )
        return [item for item in cookies if item.get("name")]
    except Exception as exc:
        if callable(logger):
            logger(f"[Cookies] Failed to dump cookies from session: {type(exc).__name__}")
    try:
        if hasattr(client.cookies, "get_dict"):
            return client.cookies.get_dict()
    except Exception as exc:
        if callable(logger):
            logger(f"[Cookies] Failed to read cookies dict: {type(exc).__name__}")
        return []
    return []


@contextmanager
def httpx_client(proxy=None, cookie_db=None, cookie_token=None, **kwargs):
    proxies = build_httpx_proxies(proxy)
    if proxies:
        kwargs.setdefault("proxies", proxies)
    if "timeout" in kwargs:
        kwargs["timeout"] = _normalize_timeout(kwargs["timeout"])
    kwargs = _apply_tls_defaults(kwargs)
    with curl_requests.Session(**kwargs) as client:
        logger = getattr(cookie_db, "log", None) if cookie_db else None
        if cookie_db and cookie_token:
            token_value = _resolve_cookie_token(cookie_token, logger)
            if token_value:
                try:
                    cookies = cookie_db.get_token_cookies(token_value)
                except Exception as exc:
                    cookies = None
                    if callable(logger):
                        logger(f"[Cookies] Failed to read cookies: {type(exc).__name__}")
                _load_cookies_into_session(client, cookies, logger)
        try:
            yield client
        finally:
            if cookie_db and cookie_token:
                token_value = _resolve_cookie_token(cookie_token, logger)
                if token_value:
                    try:
                        cookie_db.set_token_cookies(
                            token_value,
                            _dump_cookies_from_session(client, logger),
                        )
                    except Exception as exc:
                        if callable(logger):
                            logger(f"[Cookies] Failed to save cookies: {type(exc).__name__}")


@asynccontextmanager
async def ws_connect(url, *, proxy=None, headers=None, timeout=None, **kwargs):
    kwargs = _apply_tls_defaults(kwargs)
    async with curl_requests.AsyncSession() as session:
        ws = await session.ws_connect(
            url,
            proxy=proxy,
            headers=headers,
            timeout=_normalize_timeout(timeout),
            **kwargs,
        )
        try:
            yield ws
        finally:
            try:
                close_result = ws.close()
                if hasattr(close_result, "__await__"):
                    await close_result
            except Exception:
                pass
