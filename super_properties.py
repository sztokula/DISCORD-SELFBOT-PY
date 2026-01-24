import base64
import json
import re
import threading
import time
from collections import OrderedDict
from datetime import datetime
import httpx
from client_identity import USER_AGENT, CHROME_VERSION
from proxy_utils import httpx_client

DEFAULT_BUILD_NUMBER = 300000
DEFAULT_LOCALE = "en-US"
DEFAULT_UA = USER_AGENT

_LOCALE_CACHE = {}
_LOCALE_CACHE_LOCK = threading.Lock()
_LOCALE_CACHE_TTL_SECONDS = 3600

_COUNTRY_TO_LOCALE = {
    "US": "en-US",
    "CA": "en-CA",
    "GB": "en-GB",
    "AU": "en-AU",
    "NZ": "en-NZ",
    "PL": "pl-PL",
    "DE": "de-DE",
    "FR": "fr-FR",
    "ES": "es-ES",
    "IT": "it-IT",
    "NL": "nl-NL",
    "SE": "sv-SE",
    "NO": "nb-NO",
    "FI": "fi-FI",
    "DK": "da-DK",
    "PT": "pt-PT",
    "BR": "pt-BR",
    "MX": "es-MX",
    "AR": "es-AR",
    "TR": "tr-TR",
    "RU": "ru-RU",
    "UA": "uk-UA",
    "CN": "zh-CN",
    "TW": "zh-TW",
    "JP": "ja-JP",
    "KR": "ko-KR",
    "IN": "en-IN",
    "ID": "id-ID",
    "TH": "th-TH",
    "VN": "vi-VN",
    "PH": "en-PH",
    "SA": "ar-SA",
    "AE": "ar-AE",
}


def _extract_chrome_version(user_agent):
    if not user_agent:
        return f"{CHROME_VERSION}.0.0.0"
    match = re.search(r"Chrome/([0-9.]+)", user_agent)
    return match.group(1) if match else f"{CHROME_VERSION}.0.0.0"


def _safe_int(value, fallback):
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _normalize_country_code(value):
    if not value:
        return None
    code = str(value).strip().upper()
    if len(code) != 2:
        return None
    return code


def _locale_for_country(country_code):
    if not country_code:
        return DEFAULT_LOCALE
    return _COUNTRY_TO_LOCALE.get(country_code, DEFAULT_LOCALE)


def _accept_language_for_locale(locale):
    if not locale:
        return "en-US,en;q=0.9"
    parts = locale.split("-", 1)
    lang = parts[0].lower()
    if lang == "en":
        return "en-US,en;q=0.9"
    return f"{locale},{lang};q=0.9,en-US;q=0.8,en;q=0.7"


def _get_cached_locale(proxy):
    if not proxy:
        return None
    now = time.monotonic()
    with _LOCALE_CACHE_LOCK:
        entry = _LOCALE_CACHE.get(proxy)
        if not entry:
            return None
        if (now - entry["ts"]) > _LOCALE_CACHE_TTL_SECONDS:
            _LOCALE_CACHE.pop(proxy, None)
            return None
        return entry.get("locale")


def _set_cached_locale(proxy, locale):
    if not proxy or not locale:
        return
    with _LOCALE_CACHE_LOCK:
        _LOCALE_CACHE[proxy] = {"locale": locale, "ts": time.monotonic()}


def _fetch_proxy_country(proxy, user_agent):
    if not proxy:
        return None
    endpoints = [
        ("https://ipinfo.io/json", ("country",)),
        ("https://ipapi.co/json", ("country_code",)),
        ("https://ifconfig.co/json", ("country_iso", "country_code")),
    ]
    headers = {"User-Agent": user_agent or DEFAULT_UA}
    timeout = httpx.Timeout(6.0)
    for url, keys in endpoints:
        try:
            with httpx_client(proxy, timeout=timeout, headers=headers) as client:
                resp = client.get(url)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if not isinstance(data, dict):
                continue
            for key in keys:
                code = _normalize_country_code(data.get(key))
                if code:
                    return code
        except Exception:
            continue
    return None


def resolve_locale_for_proxy(db, proxy=None, user_agent=None):
    if not proxy:
        return DEFAULT_LOCALE
    cached = _get_cached_locale(proxy)
    if cached:
        return cached
    code = _fetch_proxy_country(proxy, user_agent)
    locale = _locale_for_country(code)
    _set_cached_locale(proxy, locale)
    return locale or DEFAULT_LOCALE


def get_client_build_number(db):
    stored = db.get_setting("client_build_number", "")
    return _safe_int(stored, DEFAULT_BUILD_NUMBER)


def build_super_properties(db, user_agent=None):
    ua = user_agent or DEFAULT_UA
    build_number = get_client_build_number(db)
    return {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "system_locale": DEFAULT_LOCALE,
        "client_build_number": build_number,
        "browser_user_agent": ua,
        "browser_version": _extract_chrome_version(ua),
        "os_version": "10",
        "release_channel": "stable",
        "client_event_source": None,
        "client_build_number_updated_at": db.get_setting("client_build_number_updated_at", ""),
    }


def build_x_super_properties_value(db, user_agent=None):
    payload = build_super_properties(db, user_agent=user_agent)
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return base64.b64encode(raw.encode("utf-8")).decode("utf-8")


def _build_sec_ch_ua():
    version = str(CHROME_VERSION)
    return f"\"Chromium\";v=\"{version}\", \"Google Chrome\";v=\"{version}\", \"Not?A_Brand\";v=\"99\""


def ensure_discord_headers(headers, db, user_agent=None, add_super_properties=True, proxy=None):
    if headers is None:
        return headers
    ua = user_agent or headers.get("User-Agent") or DEFAULT_UA
    headers.setdefault("User-Agent", ua)
    headers.setdefault("sec-ch-ua", _build_sec_ch_ua())
    headers.setdefault("sec-ch-ua-mobile", "?0")
    headers.setdefault("sec-ch-ua-platform", "\"Windows\"")
    locale = resolve_locale_for_proxy(db, proxy=proxy, user_agent=ua)
    headers.setdefault("X-Discord-Locale", locale)
    headers.setdefault("Accept-Language", _accept_language_for_locale(locale))
    if add_super_properties:
        headers["X-Super-Properties"] = build_x_super_properties_value(db, user_agent=ua)
    _reorder_discord_headers(headers)
    return headers


def set_super_properties_header(headers, db, user_agent=None, proxy=None):
    return ensure_discord_headers(
        headers,
        db,
        user_agent=user_agent,
        add_super_properties=True,
        proxy=proxy,
    )


_DISCORD_HEADER_ORDER = [
    "host",
    "connection",
    "pragma",
    "cache-control",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "x-super-properties",
    "x-discord-locale",
    "authorization",
    "user-agent",
]


def _reorder_discord_headers(headers):
    if not headers:
        return headers
    items = list(headers.items())
    lower_to_key = {}
    for key, _value in items:
        lowered = str(key).lower()
        if lowered not in lower_to_key:
            lower_to_key[lowered] = key
    ordered = OrderedDict()
    for desired in _DISCORD_HEADER_ORDER:
        original_key = lower_to_key.get(desired)
        if original_key is None:
            continue
        if original_key in headers:
            ordered[original_key] = headers[original_key]
    for key, value in items:
        lowered = str(key).lower()
        if lowered in _DISCORD_HEADER_ORDER:
            continue
        if key not in ordered:
            ordered[key] = value
    headers.clear()
    headers.update(ordered)
    return headers
