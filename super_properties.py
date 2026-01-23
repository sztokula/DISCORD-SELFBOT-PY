import base64
import json
import re
from collections import OrderedDict
from datetime import datetime
from client_identity import USER_AGENT, CHROME_VERSION

DEFAULT_BUILD_NUMBER = 300000
DEFAULT_LOCALE = "pl-PL"
DEFAULT_UA = USER_AGENT


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


def ensure_discord_headers(headers, db, user_agent=None, add_super_properties=True):
    if headers is None:
        return headers
    ua = user_agent or headers.get("User-Agent") or DEFAULT_UA
    headers.setdefault("User-Agent", ua)
    headers.setdefault("sec-ch-ua", _build_sec_ch_ua())
    headers.setdefault("sec-ch-ua-mobile", "?0")
    headers.setdefault("sec-ch-ua-platform", "\"Windows\"")
    headers.setdefault("X-Discord-Locale", DEFAULT_LOCALE)
    if add_super_properties:
        headers["X-Super-Properties"] = build_x_super_properties_value(db, user_agent=ua)
    _reorder_discord_headers(headers)
    return headers


def set_super_properties_header(headers, db, user_agent=None):
    return ensure_discord_headers(headers, db, user_agent=user_agent, add_super_properties=True)


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
