import base64
import json
import re
from datetime import datetime

DEFAULT_BUILD_NUMBER = 300000
DEFAULT_LOCALE = "pl-PL"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _extract_chrome_version(user_agent):
    if not user_agent:
        return "120.0.0.0"
    match = re.search(r"Chrome/([0-9.]+)", user_agent)
    return match.group(1) if match else "120.0.0.0"


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


def set_super_properties_header(headers, db, user_agent=None):
    ua = user_agent or headers.get("User-Agent") or DEFAULT_UA
    headers["X-Super-Properties"] = build_x_super_properties_value(db, user_agent=ua)
    return headers
