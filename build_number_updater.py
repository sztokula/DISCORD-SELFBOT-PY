import re
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

from proxy_utils import httpx_client, normalize_proxy
from client_identity import USER_AGENT
from super_properties import ensure_discord_headers

LOGIN_URL = "https://discord.com/login"
BUILD_NUMBER_PATTERNS = [
    re.compile(r'build_number"\s*:\s*"(\d+)"'),
    re.compile(r"build_number\s*:\s*(\d+)"),
    re.compile(r'build_number\s*:\s*"(\d+)"'),
    re.compile(r'client_build_number"\s*:\s*"(\d+)"'),
    re.compile(r"client_build_number\s*:\s*(\d+)"),
    re.compile(r'client_build_number\s*:\s*"(\d+)"'),
]


class CriticalBuildError(RuntimeError):
    pass


class BuildNumberUpdater:
    def __init__(
        self,
        db_manager,
        log_callback=None,
        interval_seconds=86400,
        critical_callback=None,
    ):
        self.db = db_manager
        self.log = log_callback
        self.interval_seconds = interval_seconds
        self.critical_callback = critical_callback
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except CriticalBuildError as exc:
                self._log(f"[CRITICAL] Build number update failed: {exc}")
                if self.critical_callback:
                    try:
                        self.critical_callback(str(exc))
                    except Exception:
                        pass
                self._stop.set()
                break
            except Exception as exc:
                self._log(f"[Build] Update failed: {exc}")
            self._sleep_interval()

    def run_once(self, force=False):
        if not force and not self._should_check():
            return False

        proxy = self._select_proxy()
        if self._is_proxy_required() and not proxy:
            checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self._raise_critical("Proxy required but no proxy available for build number refresh.", checked_at)

        headers = {"User-Agent": USER_AGENT}
        ensure_discord_headers(headers, self.db, add_super_properties=False)
        js_urls = []
        build_number = None
        source_url = None
        checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            with httpx_client(proxy, headers=headers, timeout=15.0) as client:
                response = client.get(LOGIN_URL)
                if response.status_code != 200:
                    self._raise_critical(
                        f"Login fetch failed: {response.status_code}",
                        checked_at,
                    )
                html = response.text
                js_urls = self._extract_js_urls(html, LOGIN_URL)
                if not js_urls:
                    self._raise_critical("No JS assets found on login page.", checked_at)

                for js_url in js_urls:
                    try:
                        js_resp = client.get(js_url)
                    except Exception:
                        continue
                    if js_resp.status_code != 200:
                        continue
                    build_number = self._extract_build_number(js_resp.text)
                    if build_number:
                        source_url = js_url
                        break
        except CriticalBuildError:
            raise
        except Exception as exc:
            self._raise_critical(f"Build number refresh error: {exc}", checked_at)

        self._set_checked_at(checked_at)
        if not build_number:
            self._raise_critical("build_number not found in JS assets.", checked_at)

        self.db.set_setting("client_build_number", str(build_number))
        self.db.set_setting("client_build_number_updated_at", checked_at)
        if source_url:
            self.db.set_setting("client_build_number_js_url", source_url)
        self._log(f"[Build] Updated build_number to {build_number}.")
        return True

    def _should_check(self):
        raw = self.db.get_setting("client_build_number_checked_at", "")
        if not raw:
            return True
        try:
            last = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return True
        return datetime.now() - last >= timedelta(seconds=self.interval_seconds)

    def _is_proxy_required(self):
        try:
            value = self.db.get_setting("require_proxy", None)
        except Exception:
            return False
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _select_proxy(self):
        if not self._is_proxy_required():
            return None
        try:
            scrape_proxy = (self.db.get_setting("scrape_proxy", "") or "").strip()
        except Exception:
            scrape_proxy = ""
        if scrape_proxy:
            normalized = normalize_proxy(scrape_proxy)
            if normalized:
                return normalized
        try:
            pool = self.db.get_proxy_pool()
        except Exception:
            pool = []
        for proxy in pool or []:
            normalized = normalize_proxy(proxy)
            if normalized:
                return normalized
        try:
            proxies = self.db.get_account_proxies()
        except Exception:
            proxies = []
        for _acc_id, proxy in proxies or []:
            normalized = normalize_proxy(proxy)
            if normalized:
                return normalized
        return None

    def _set_checked_at(self, timestamp):
        self.db.set_setting("client_build_number_checked_at", timestamp)

    def _raise_critical(self, message, checked_at=None):
        if checked_at:
            self._set_checked_at(checked_at)
        else:
            self._set_checked_at(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        try:
            self.db.set_setting("build_number_critical_error", message)
            self.db.set_setting(
                "build_number_critical_at",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        except Exception:
            pass
        raise CriticalBuildError(message)

    def _extract_js_urls(self, html, base_url):
        matches = re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', html, re.IGNORECASE)
        urls = []
        for src in matches:
            full = urljoin(base_url, src)
            urls.append(full)
        urls = self._prioritize_js_urls(urls)
        return urls

    def _prioritize_js_urls(self, urls):
        seen = set()
        unique = []
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            unique.append(url)
        scored = []
        for url in unique:
            score = 0
            lowered = url.lower()
            if "app" in lowered or "main" in lowered:
                score += 2
            if "web" in lowered:
                score += 1
            scored.append((score, url))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [url for _, url in scored]

    def _extract_build_number(self, text):
        for pattern in BUILD_NUMBER_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(1)
        return None

    def _sleep_interval(self):
        remaining = self.interval_seconds
        while remaining > 0 and not self._stop.is_set():
            chunk = min(60, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def _log(self, message):
        if self.log:
            self.log(message)


if __name__ == "__main__":
    from database import DatabaseManager

    db = DatabaseManager()
    updater = BuildNumberUpdater(db)
    updater.run_forever()
